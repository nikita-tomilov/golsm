package golsm

import (
	log "github.com/jeanphorn/log4go"
	"github.com/nikita-tomilov/golsm/dto"
	"github.com/nikita-tomilov/golsm/memt"
	"github.com/nikita-tomilov/golsm/sst"
	"sort"
	"sync"
)

type StorageReader struct {
	SSTManager *sst.Manager
	MemTable   *memt.Manager
	mutex      *sync.Mutex
}

func (sr *StorageReader) Init() {
	sr.mutex = &sync.Mutex{}
}

func (sr *StorageReader) Retrieve(tags []string, from uint64, to uint64) map[string][]dto.Measurement {
	ans := make(map[string][]dto.Measurement)

	var wg sync.WaitGroup
	for _, tag := range tags {
		log.Debug("Launching Retrieve batch for tag %s", tag)
		wg.Add(1)
		go retrieveDataForTag(&wg, sr, tag, from, to, &ans)
	}

	wg.Wait()
	return ans
}

func (sr *StorageReader) Availability() (uint64, uint64) {
	fromForMem, toForMem := sr.MemTable.Availability()
	fromForSst, toForSst := sr.SSTManager.Availability()

	return minNotZero(fromForMem, fromForSst), maxNotZero(toForMem, toForSst)
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

func retrieveDataForTag(wg *sync.WaitGroup, sr *StorageReader, tag string, from uint64, to uint64, res *map[string][]dto.Measurement) {
	defer wg.Done()

	memtForTag := sr.MemTable.MemTableForTag(tag)
	sstForTag := sr.SSTManager.ManagerForTag(tag)

	timestampToValue := make(map[uint64][]byte)
	var dataFromMemt []memt.Entry

	if memtForTag == nil {
		return
	}

	availMemtFrom, availMemtTo := memtForTag.Availability()

	if (availMemtFrom != 0) && (availMemtTo != 0) {
		dataFromMemt = memtForTag.Retrieve(from, to)
	}

	if (availMemtFrom >= from) || (availMemtTo <= to) {
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

	sr.mutex.Lock()
	(*res)[tag] = ans
	sr.mutex.Unlock()
}
