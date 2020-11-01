package memt

import (
	"github.com/nikita-tomilov/golsm/commitlog"
	"github.com/nikita-tomilov/golsm/dto"
	"github.com/nikita-tomilov/golsm/utils"
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

func (sm *Manager) MergeWithPrefetched(data map[string][]dto.Measurement) {
	expiresAt := utils.GetNowMillis() + uint64(sm.PerformExpirationEvery.Milliseconds() * 10)
	for tag, values := range data {
		memtForTag := sm.MemTableForTag(tag)
		memtForTag.MergeWithPrefetched(values, expiresAt)
	}
}

func (sm *Manager) MergeWithCommitlog(commitlogEntries []commitlog.Entry) {
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
	}
	for tag, values := range groupedByTag {
		memtForTag := sm.MemTableForTag(tag)
		memtForTag.MergeWithCommitlog(values)
	}
}

func (sm *Manager) MergeWithCommitlogForTag(tag string, entries []commitlog.Entry) {
	st := sm.MemTableForTag(tag)
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

func (sm *Manager) GetTags() []string {
	keys := make([]string, len(sm.memtForTag))
	i := 0
	for k := range sm.memtForTag {
		keys[i] = k
		i++
	}
	return keys
}

func (sm *Manager) createMemtForTag(tag string) *MemTforTag {
	memtft := MemTforTag{Tag: tag, MaxEntriesCount: sm.MaxEntriesPerTag}
	memtft.InitStorage()
	sm.memtForTag[tag] = &memtft
	return &memtft
}

func (sm *Manager) MemTableForTag(tag string) *MemTforTag {
	memtForTag, memtForTagExists := sm.memtForTag[tag]
	if !memtForTagExists {
		memtForTag = sm.createMemtForTag(tag)
	}
	return memtForTag
}
