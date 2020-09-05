package memt

import (
	"github.com/google/btree"
	"github.com/nikita-tomilov/golsm/commitlog"
	"github.com/nikita-tomilov/golsm/utils"
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

func (mt *MemTforTag) Availability() (uint64, uint64) {
	min := mt.data.Min()
	max := mt.data.Max()

	mine := min.(*Entry)
	maxe := max.(*Entry)

	return mine.Timestamp, maxe.Timestamp
}

func (mt *MemTforTag) Retrieve(fromTs uint64, toTs uint64) []Entry {
	mt.mutex.Lock()
	ans := make([]Entry, 0)
	mt.data.AscendRange(buildIndexKey(fromTs), buildIndexKey(toTs + 1), func (i btree.Item) bool {
		oe := i.(*Entry)
		ans = append(ans, *oe)
		return true
	})
	mt.mutex.Unlock()
	return ans
}

func (mt *MemTforTag) PerformExpiration() {
	mt.mutex.Lock()
	toBeDeleted := make([]*Entry, 0)
	now := utils.GetNowMillis()
	mt.data.Ascend(func(i btree.Item) bool {
		oe := i.(*Entry)
		if (oe.ExpiresAt != 0) && (oe.ExpiresAt < now) {
			toBeDeleted = append(toBeDeleted, oe)
		}
		return true
	})
	for _, i := range toBeDeleted {
		mt.data.Delete(i)
	}
	mt.mutex.Unlock()
}

func buildIndexKey(ts uint64) btree.Item {
	return &Entry{Timestamp:ts}
}
