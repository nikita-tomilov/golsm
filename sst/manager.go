package sst

import (
	"github.com/btcsuite/btcutil/base58"
	"github.com/nikita-tomilov/golsm/commitlog"
	"io/ioutil"
	"sync"
)

type Manager struct {
	RootDir   string
	sstForTag map[string]*SSTforTag
	mutex     *sync.Mutex
}

func (sm *Manager) InitStorage() {
	sm.sstForTag = make(map[string]*SSTforTag)
	files, _ := ioutil.ReadDir(sm.RootDir)
	for _, f := range files {
		tag := string(base58.Decode(f.Name()))
		sm.SstForTag(tag)
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
		sstForTag := sm.SstForTag(tag)
		sstForTag.MergeWithCommitlog(values)
	}
}

func (sm *Manager) Availability() (uint64, uint64) {
	fromts := ^uint64(0)
	tots := uint64(0)

	for _, sstft := range sm.sstForTag {
		f, t := sstft.Availability()
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

func (sm *Manager) SstForTag(tag string) *SSTforTag {
	sstForTag, sstForTagExists := sm.sstForTag[tag]
	if !sstForTagExists {
		sstForTag = sm.createSstForTag(tag)
	}
	return sstForTag
}

func (sm *Manager) createSstForTag(tag string) *SSTforTag {
	sst := SSTforTag{Tag: tag, FileName: sm.RootDir + "/" + base58.Encode([]byte(tag))}
	sst.InitStorage()
	sm.sstForTag[tag] = &sst
	return &sst
}

func (sm *Manager) GetTags() []string {
	keys := make([]string, len(sm.sstForTag))
	i := 0
	for k := range sm.sstForTag {
		keys[i] = k
		i++
	}
	return keys
}