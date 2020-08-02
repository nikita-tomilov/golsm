package commitlog

import (
	"os"
	"sync/atomic"
)

type CommitlogManager struct {
	Path            string
	commitlogA      OverFile
	commitlogB      OverFile
	usingA          bool
	activeCommitlog atomic.Value
}

func (m *CommitlogManager) Init() {
	os.MkdirAll(m.Path, os.ModePerm)

	m.commitlogA = OverFile{commitlogFileName: m.Path + "/COMMITLOGA"}
	m.commitlogB = OverFile{commitlogFileName: m.Path + "/COMMITLOGB"}

	m.commitlogA.Init()
	m.commitlogB.Init()

	m.activeCommitlog.Store(m.commitlogA)
	m.usingA = true
}

func (m *CommitlogManager) Store(entry CommitlogEntry) {
	active := m.activeCommitlog.Load().(OverFile)
	active.Store(entry)
}

func (m *CommitlogManager) RetrieveAll() []CommitlogEntry {
	active := m.activeCommitlog.Load().(OverFile)
	return active.RetrieveAll()
}

func (m *CommitlogManager) SwapCommitlogs() {
	if m.usingA {
		m.activeCommitlog.Store(m.commitlogB)
	} else {
		m.activeCommitlog.Store(m.commitlogA)
	}
	m.usingA = !m.usingA
}