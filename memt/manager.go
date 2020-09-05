package memt

import (
	"bytes"
	log "github.com/jeanphorn/log4go"
	"github.com/nikita-tomilov/golsm/commitlog"
	"sort"
	"sync"
	"time"
)

type Manager struct {
	memtForTag             map[string]*MemTforTag
	mutex                  *sync.Mutex
	shouldBeRunning        bool
	MaxEntriesPerTag       int
	PerformExpirationEvery time.Duration
}

func (sm *Manager) InitStorage() {
	sm.memtForTag = make(map[string]*MemTforTag)
	if sm.MaxEntriesPerTag == 0 {
		sm.MaxEntriesPerTag = 10
	}
	if sm.PerformExpirationEvery == 0 {
		sm.PerformExpirationEvery = 10 * time.Second
	}
	sm.shouldBeRunning = true
	go func() {
		for sm.shouldBeRunning {
			time.Sleep(sm.PerformExpirationEvery)
			for _, memtft := range sm.memtForTag {
				memtft.PerformExpiration()
			}
		}
	}()
}

func (sm *Manager) CloseStorage() {
	sm.shouldBeRunning = false
}

func (sm *Manager) MergeWithCommitlog(commitlogEntries []commitlog.Entry) {
	sort.Slice(commitlogEntries, func(i, j int) bool {
		return bytes.Equal(commitlogEntries[i].Key, commitlogEntries[j].Key)
	})

	groupedByTag := make(map[string][]commitlog.Entry)
	for _, entry := range commitlogEntries {
		tag := string(entry.Key)
		entriesForTag, tagExistsInEntriesGroup := groupedByTag[tag]
		if tagExistsInEntriesGroup {
			groupedByTag[tag] = append(entriesForTag, entry)
		} else {
			newGroup := make([]commitlog.Entry, 1)
			newGroup[0] = entry
			groupedByTag[tag] = newGroup
		}
		sm.createMemtForTagIfNeeded(tag)
	}

	var wg sync.WaitGroup
	for tag, entries := range groupedByTag {
		log.Debug("Launching MEMT batch for tag %s", tag)
		wg.Add(1)
		go applyEntriesForTag(&wg, sm, tag, entries)
	}

	wg.Wait()
}

func (sm *Manager) MergeWithCommitlogForTag(tag string, entries []commitlog.Entry) {
	sm.createMemtForTagIfNeeded(tag)
	st := sm.memtForTag[tag]
	st.MergeWithCommitlog(entries)
}

func (sm *Manager) Availability() (uint64, uint64) {
	fromts := ^uint64(0)
	tots := uint64(0)

	for _, memtft := range sm.memtForTag {
		f, t := memtft.Availability()
		if fromts > f {
			fromts = f
		}
		if tots < t {
			tots = t
		}
	}

	if tots == uint64(0) {
		return 0, 0
	}
	return fromts, tots
}

func (sm *Manager) createMemtForTagIfNeeded(tag string) {
	_, sstForTagExists := sm.memtForTag[tag]
	if !sstForTagExists {
		sm.createMemtForTag(tag)
	}
}

func (sm *Manager) createMemtForTag(tag string) {
	memtft := MemTforTag{Tag: tag, MaxEntriesCount: sm.MaxEntriesPerTag}
	memtft.InitStorage()
	sm.memtForTag[tag] = &memtft
}

func (sm *Manager) MemTableForTag(tag string) *MemTforTag {
	return sm.memtForTag[tag]
}

func applyEntriesForTag(wg *sync.WaitGroup, sm *Manager, tag string, entries []commitlog.Entry) {
	defer wg.Done()
	sm.MergeWithCommitlogForTag(tag, entries)
}
