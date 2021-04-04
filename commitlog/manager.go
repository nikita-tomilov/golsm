package commitlog

import (
	"os"
	"sync/atomic"
)

type Manager struct {
	Path              string
	commitlogA        *OverFile
	commitlogB        *OverFile
	usingA            bool
	activeCommitlog   atomic.Value
	inactiveCommitlog atomic.Value
}

func (m *Manager) Init() {
	os.MkdirAll(m.Path, os.ModePerm)

	m.commitlogA = &OverFile{commitlogFileName: m.Path + "/COMMITLOGA"}
	m.commitlogB = &OverFile{commitlogFileName: m.Path + "/COMMITLOGB"}

	m.commitlogA.Init()
	m.commitlogB.Init()

	m.activeCommitlog.Store(m.commitlogA)
	m.usingA = true
}

func (m *Manager) getActiveCommitlog() *OverFile {
	active := m.activeCommitlog.Load().(*OverFile)
	return active
}

func (m *Manager) getInactiveCommitlog() *OverFile {
	inactive := m.inactiveCommitlog.Load().(*OverFile)
	return inactive
}

func (m *Manager) Store(entry Entry) {
	active := m.getActiveCommitlog()
	active.Store(entry)
}

func (m *Manager) StoreMultiple(entries []Entry) {
	active := m.getActiveCommitlog()
	for _, entry := range entries {
		active.Store(entry)
	}
}

func (m *Manager) RetrieveAll() []Entry {
	active := m.getActiveCommitlog()
	return active.RetrieveAll()
}

func (m *Manager) SwapCommitlogs() {
	if m.usingA {
		m.inactiveCommitlog.Store(m.commitlogA)
		m.activeCommitlog.Store(m.commitlogB)
	} else {
		m.inactiveCommitlog.Store(m.commitlogB)
		m.activeCommitlog.Store(m.commitlogA)
	}
	m.usingA = !m.usingA
}

func (m *Manager) RetrieveAllFromPrevious() []Entry {
	inactive := m.getInactiveCommitlog()
	return inactive.RetrieveAll()
}

func (m *Manager) ClearPrevious() {
	inactive := m.getInactiveCommitlog()
	inactive.Clear()
}
