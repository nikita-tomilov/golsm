package memt

import (
	"bytes"
	log "github.com/jeanphorn/log4go"
	"github.com/nikita-tomilov/golsm/commitlog"
	"sort"
	"sync"
)

type Manager struct {
	sstForTag        map[string]*MemTforTag
	mutex            *sync.Mutex
	MaxEntriesPerTag int
}

func (sm *Manager) InitStorage() {
	sm.sstForTag = make(map[string]*MemTforTag)
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
		_, sstForTagExists := sm.sstForTag[tag]
		if !sstForTagExists {
			sst := MemTforTag{Tag: tag, MaxEntriesCount: sm.MaxEntriesPerTag}
			sst.InitStorage()
			sm.sstForTag[tag] = &sst
		}
	}

	var wg sync.WaitGroup
	for tag, entries := range groupedByTag {
		log.Debug("Launching MEMT batch for tag %s", tag)
		wg.Add(1)
		go applyEntriesForTag(&wg, sm, tag, entries)
	}

	wg.Wait()
}

func (sm *Manager) MemTableForTag(tag string) *MemTforTag {
	return sm.sstForTag[tag]
}

func applyEntriesForTag(wg *sync.WaitGroup, sm *Manager, tag string, entries []commitlog.Entry) {
	defer wg.Done()

	st := sm.sstForTag[tag]
	st.MergeWithCommitlog(entries)
}
