package sst

import (
	"fmt"
	log "github.com/jeanphorn/log4go"
	"math/rand"
	"os"
	"testing"
	"time"
)

func BenchmarkGetEntriesWithIndex(b *testing.B) {
	stSsd := getNewInitializedStorage("/home/hotaro/sst-file-ssd.db")
	stHdd := getNewInitializedStorage("/media/hotaro/Bigdata/sst-file-hdd.db")
	min, max := stSsd.Availability()

	functions := []struct {
		name string
		fun  func(fromTs uint64, toTs uint64) []Entry
	}{
		{"with index on ssd", stSsd.GetEntriesWithIndex},
		{"without index on ssd", stSsd.GetEntriesWithoutIndex},
		{"with index on hdd", stHdd.GetEntriesWithIndex},
		{"without index on hdd", stHdd.GetEntriesWithoutIndex},
	}
	for _, function := range functions {
		b.Run(function.name, func(b *testing.B) {
			i := 0
			for i < b.N {
				from := randomTs(min+20, min+(max-min)/2)
				to := randomTs(from, max)
				if to-from <= 10 {
					from -= 10
				}
				slice := function.fun(from, to)
				if len(slice) == 0 {
					log.Warn(fmt.Sprintf("Slice empty for from; to %d; %d", from, to))
				}
				i++
			}
		})
	}
}

func getNewInitializedStorage(path string) *SSTforTag {
	os.Remove(path)
	st := SSTforTag{FileName: path}
	st.InitStorage()

	actualEntries := getBigBatchOfEntriesOfSize(100000, 1000, 0, 4096)

	start := time.Now()
	st.MergeWithCommitlog(actualEntries)
	elapsed := time.Since(start)
	log.Warn(fmt.Sprintf("Took %s to write %d entries to file %s", elapsed.String(), len(actualEntries), path))

	min, max := st.Availability()

	//then
	if min != uint64(10000) {
		panic("min ts incorrect")
	}
	if max != uint64(1009990) {
		panic("max ts incorrect")
	}

	return &st
}

func randomTs(from uint64, to uint64) uint64 {
	return uint64(rand.Float64()*float64(to-from) + float64(from))
}
