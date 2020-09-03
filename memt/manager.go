package memt

import (
	"bytes"
	log "github.com/jeanphorn/log4go"
	"github.com/nikita-tomilov/golsm/commitlog"
	"sort"
	"sync"
)

type Manager struct {
	memtForTag       map[string]*MemTforTag
	mutex            *sync.Mutex
	MaxEntriesPerTag int
}

func (sm *Manager) InitStorage() {
	sm.memtForTag = make(map[string]*MemTforTag)
	if sm.MaxEntriesPerTag == 0 {
		sm.MaxEntriesPerTag = 10
	}
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
