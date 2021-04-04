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
		entries := make([]commitlog.Entry, len(values))
		for i, value := range values {
			e := commitlog.Entry{Key: []byte(tag), Timestamp: value.Timestamp, ExpiresAt: expiresAt, Value: value.Value}
			entries[i] = e
		}
		sw.DiskWriter.StoreMultiple(entries)
		sw.MemTable.MergeWithCommitlogForTag(tag, entries)
	}
}

func (sw *StorageWriter) StoreBatch(data []dto.TaggedMeasurement, expiresAt uint64) {
	entriesPerTag := make(map[string][]commitlog.Entry)

	for _, entry := range data {
		entries, exists := entriesPerTag[entry.Tag]
		if !exists {
			entriesPerTag[entry.Tag] = make([]commitlog.Entry, 0, len(data))
			entries = entriesPerTag[entry.Tag]
		}
		entriesPerTag[entry.Tag] = append(entries, commitlog.Entry{Key: []byte(entry.Tag), Timestamp: entry.Timestamp, ExpiresAt: expiresAt, Value: entry.Value})
	}

	for tag, entries := range entriesPerTag {
		sw.DiskWriter.StoreMultiple(entries)
		sw.MemTable.MergeWithCommitlogForTag(tag, entries)
	}
}
