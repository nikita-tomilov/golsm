package sst

import (
	"bytes"
	"github.com/btcsuite/btcutil/base58"
	log "github.com/jeanphorn/log4go"
	"github.com/nikita-tomilov/golsm/commitlog"
	"sort"
	"sync"
)

type Manager struct {
	RootDir string
	sstForTag map[string]*SSTforTag
	mutex *sync.Mutex
}

func (sm *Manager) InitStorage() {
	sm.sstForTag = make(map[string]*SSTforTag)
}

func (sm *Manager) MergeWithCommitlog(commitlogEntries []commitlog.Entry) {
	sort.Slice(commitlogEntries, func (i, j int) bool {
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
			sst := SSTforTag{Tag:tag, FileName:sm.RootDir + "/" + base58.Encode(entry.Key)}
			sst.InitStorage()
			sm.sstForTag[tag] = &sst
		}
	}

	var wg sync.WaitGroup
	for tag, entries := range groupedByTag {
		log.Debug("Launching batch for tag %s", tag)
		wg.Add(1)
		go applyEntriesForTag(&wg, sm, tag, entries)
	}

	wg.Wait()
}

func (sm *Manager) ManagerForTag(tag string) *SSTforTag {
	return sm.sstForTag[tag]
}

func applyEntriesForTag(wg *sync.WaitGroup, sm *Manager, tag string, entries []commitlog.Entry) {
	defer wg.Done()

	st := sm.sstForTag[tag]
	st.MergeWithCommitlog(entries)
}