package golsm

import (
	"github.com/nikita-tomilov/golsm/dto"
	"github.com/nikita-tomilov/golsm/memt"
	"github.com/nikita-tomilov/golsm/sst"
	"github.com/nikita-tomilov/golsm/utils"
	"sort"
	"sync"
	"time"
)

type StorageReader struct {
	SSTManager *sst.Manager
	MemTable   *memt.Manager
	MemtPrefetch time.Duration
	mutex      *sync.Mutex
}

func (sr *StorageReader) Init() {
	sr.mutex = &sync.Mutex{}
	if (len(sr.SSTManager.GetTags()) > 0) && (sr.MemtPrefetch.Milliseconds() > 0) {
		//i was initialized over existing storage; should prefetch some data to memt
		sr.prefetch()
	}
}

func (sr *StorageReader) prefetch() {
	availFrom, availTo := sr.SSTManager.Availability()
	if (availTo == 0) || (availFrom == 0) {
		return
	}

	from := maxNotZero(availFrom, uint64(int64(availTo) - sr.MemtPrefetch.Milliseconds()))
	to := availTo
	tags := sr.SSTManager.GetTags()
	data := sr.Retrieve(tags, from, to)

	sr.MemTable.MergeWithPrefetched(data)
}

func (sr *StorageReader) Retrieve(tags []string, from uint64, to uint64) map[string][]dto.Measurement {
	ans := make(map[string][]dto.Measurement)

	for _, tag := range tags {
		ans[tag] = sr.retrieveDataForTag(tag, from, to)
	}

	return ans
}

func (sr *StorageReader) Availability() (uint64, uint64) {
	fromForMem, toForMem := sr.MemTable.Availability()
	fromForSst, toForSst := sr.SSTManager.Availability()

	return minNotZero(fromForMem, fromForSst), maxNotZero(toForMem, toForSst)
}

func (sr *StorageReader) GetTags() []string {
	fromSst := sr.SSTManager.GetTags()
	fromMemt := sr.MemTable.GetTags()
	return utils.MergeWithoutDuplicates(fromSst, fromMemt)
}

func minNotZero(a uint64, b uint64) uint64 {
	if a == 0 {
		return b
	}
	if (b == 0) || (a < b) {
		return a
	}
	return b
}

func maxNotZero(a uint64, b uint64) uint64 {
	if a == 0 {
		return b
	}
	if (b == 0) || (a > b) {
		return a
	}
	return b
}

func (sr *StorageReader) retrieveDataForTag(tag string, from uint64, to uint64) []dto.Measurement {
	memtForTag := sr.MemTable.MemTableForTag(tag)
	sstForTag := sr.SSTManager.SstForTag(tag)

	timestampToValue := make(map[uint64][]byte)
	var dataFromMemt []memt.Entry

	if memtForTag == nil {
		return nil
	}

	availMemtFrom, availMemtTo := memtForTag.Availability()

	if (availMemtFrom != 0) && (availMemtTo != 0) {
		dataFromMemt = memtForTag.Retrieve(from, to)
	}

	if (availMemtFrom > from) || (availMemtTo < to) || (availMemtFrom == 0) || (availMemtTo == 0) {
		if sstForTag != nil {
			dataFromSst := sstForTag.GetEntriesWithIndex(from, to)

			for _, dfs := range dataFromSst {
				timestampToValue[dfs.Timestamp] = dfs.Value
			}
		}
	}

	for _, dfm := range dataFromMemt {
		timestampToValue[dfm.Timestamp] = dfm.Value
	}

	ans := make([]dto.Measurement, len(timestampToValue))
	i := 0
	for k, v := range timestampToValue {
		ans[i] = dto.Measurement{Timestamp: k, Value: v}
		i++
	}

	sort.Slice(ans, func(i, j int) bool {
		return ans[i].Timestamp < ans[j].Timestamp
	})

	return ans
}
