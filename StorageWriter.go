package golsm

import (
	"github.com/nikita-tomilov/golsm/commitlog"
	"github.com/nikita-tomilov/golsm/dto"
	"github.com/nikita-tomilov/golsm/memt"
	"github.com/nikita-tomilov/golsm/writer"
	"sync"
)

type StorageWriter struct {
	DiskWriter *writer.DiskWriter
	MemTable   *memt.Manager
	mutex      *sync.Mutex
}

func (sw *StorageWriter) Init() {
	sw.mutex = &sync.Mutex{}
}

func (sw *StorageWriter) Store(data map[string][]dto.Measurement, expiresAt uint64) {
	for tag, values := range data {
		entries := make([]commitlog.Entry, 0)
		for _, value := range values {
			e := commitlog.Entry{Key:[]byte(tag), Timestamp:value.Timestamp, ExpiresAt:expiresAt, Value:value.Value}
			entries = append(entries, e)
			sw.DiskWriter.Store(e)
		}
		sw.MemTable.MergeWithCommitlogForTag(tag, entries)
	}
}