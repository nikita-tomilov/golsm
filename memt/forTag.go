package memt

import (
	"github.com/google/btree"
	"github.com/nikita-tomilov/golsm/commitlog"
	"sync"
)

type MemTforTag struct {
	Tag                     string
	MaxEntriesCount         int
	mutex                   *sync.Mutex
	data                    *btree.BTree
}

func (mt *MemTforTag) InitStorage() {
	mt.data = btree.New(10)
	mt.mutex = &sync.Mutex{}
}


func (mt *MemTforTag) MergeWithCommitlog(entries []commitlog.Entry) {
	mt.mutex.Lock()
	for _, entry := range entries {
		mt.save(entry.Timestamp, entry.ExpiresAt, entry.Value)
	}
	mt.mutex.Unlock()
}


func (mt *MemTforTag) save(timestamp uint64, expiresAt uint64, value []byte) {
	entry := Entry{Timestamp: timestamp, ExpiresAt: expiresAt, Value: value}
	if mt.data.Len() >= mt.MaxEntriesCount {
		min := mt.data.Min()
		if min.Less(&entry) {
			mt.data.DeleteMin()
		}
	}
	mt.data.ReplaceOrInsert(&entry)
}

func (mt *MemTforTag) RetrieveAll() []Entry {
	return mt.Retrieve(0, ^uint64(0) - 1)
}

func (mt *MemTforTag) Retrieve(fromTs uint64, toTs uint64) []Entry {
	ans := make([]Entry, 0)
	mt.data.AscendRange(buildIndexKey(fromTs), buildIndexKey(toTs + 1), func (i btree.Item) bool {
		oe := i.(*Entry)
		ans = append(ans, *oe)
		return true
	})
	return ans
}

func buildIndexKey(ts uint64) btree.Item {
	return &Entry{Timestamp:ts}
}
