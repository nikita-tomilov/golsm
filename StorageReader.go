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

//TODO: OPTIMIZE REGARDING AVAILABILITY (E.G. IF THE REQUESTED RANGE IS FULLY WITHIN MEMT, NO NEED TO USE SST)
func retrieveDataForTag(wg *sync.WaitGroup, sr *StorageReader, tag string, from uint64, to uint64, res *map[string][]dto.Measurement) {
	defer wg.Done()

	sstForTag := sr.SSTManager.ManagerForTag(tag)
	dataFromSst := sstForTag.GetEntriesWithIndex(from, to)

	memtForTag := sr.MemTable.MemTableForTag(tag)
	dataFromMemt := memtForTag.Retrieve(from, to)

	timestampToValue := make(map[uint64][]byte)

	for _, dfs := range dataFromSst {
		timestampToValue[dfs.Timestamp] = dfs.Value
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

	(*res)[tag] = ans
}
