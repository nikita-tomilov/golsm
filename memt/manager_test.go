package memt

import (
	log "github.com/jeanphorn/log4go"
	"github.com/nikita-tomilov/golsm/commitlog"
	"github.com/nikita-tomilov/golsm/utils"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMemTManager_SanityCheck(t *testing.T) {
	//given
	m := Manager{}
	m.InitStorage()

	//when
	actualEntries := getDummyCommitlogEntriesForMultipleTags()
	m.MergeWithCommitlog(actualEntries)

	//then
	assert.Equal(t, 2, len(m.memtForTag), "mt count mismatch")

	st1 := m.memtForTag["tagZero"]
	st2 := m.memtForTag["tagOne"]

	st1e := st1.RetrieveAll()
	st2e := st2.RetrieveAll()

	assert.Equal(t, 3, len(st1e), "dto count in mt mismatch for tagZero")
	assert.Equal(t, 2, len(st2e), "dto count in mt mismatch for tagOne")

	//when
	actualEntries2 := getDummyCommitlogEntriesForMultipleTags2()
	m.MergeWithCommitlog(actualEntries2)

	//then
	assert.Equal(t, 2, len(m.memtForTag), "mt count mismatch")

	st1 = m.memtForTag["tagZero"]
	st2 = m.memtForTag["tagOne"]

	st1e = st1.RetrieveAll()
	st2e = st2.RetrieveAll()

	assert.Equal(t, 4, len(st1e), "dto count in mt mismatch for tagZero after appending")
	assert.Equal(t, 3, len(st2e), "dto count in mt mismatch for tagOne after appending")

	log.Close()
}

func TestMemTManager_MaxEntriesPerTagWorks(t *testing.T) {
	//given
	m := Manager{MaxEntriesPerTag:2}
	m.InitStorage()

	//when
	actualEntries := getDummyCommitlogEntriesForMultipleTags()
	m.MergeWithCommitlog(actualEntries)

	//then
	assert.Equal(t, 2, len(m.memtForTag), "mt count mismatch")

	st1 := m.memtForTag["tagZero"]
	st2 := m.memtForTag["tagOne"]

	st1e := st1.RetrieveAll()
	st2e := st2.RetrieveAll()

	assert.Equal(t, 2, len(st1e), "dto count in mt mismatch for tagZero")
	assert.Equal(t, 2, len(st2e), "dto count in mt mismatch for tagOne")

	assert.Equal(t, uint64(1341), st1e[0].Timestamp, "incorrect timestamp for tagZero")
	assert.Equal(t, uint64(1345), st1e[1].Timestamp, "incorrect timestamp for tagZero")
	assert.Equal(t, uint64(1339), st2e[0].Timestamp, "incorrect timestamp for tagOne")
	assert.Equal(t, uint64(1343), st2e[1].Timestamp, "incorrect timestamp for tagOne")

	//when
	actualEntries2 := getDummyCommitlogEntriesForMultipleTags2()
	m.MergeWithCommitlog(actualEntries2)

	//then
	assert.Equal(t, 2, len(m.memtForTag), "mt count mismatch")

	st1 = m.memtForTag["tagZero"]
	st2 = m.memtForTag["tagOne"]

	st1e = st1.RetrieveAll()
	st2e = st2.RetrieveAll()

	assert.Equal(t, 2, len(st1e), "dto count in mt mismatch for tagZero after appending")
	assert.Equal(t, 2, len(st2e), "dto count in mt mismatch for tagOne after appending")

	assert.Equal(t, uint64(1345), st1e[0].Timestamp, "incorrect timestamp for tagZero")
	assert.Equal(t, uint64(1347), st1e[1].Timestamp, "incorrect timestamp for tagZero")
	assert.Equal(t, uint64(1343), st2e[0].Timestamp, "incorrect timestamp for tagOne")
	assert.Equal(t, uint64(1345), st2e[1].Timestamp, "incorrect timestamp for tagOne")

	log.Close()
}

func TestMemTManager_ExpirationWorks(t *testing.T) {
	//given
	m := Manager{PerformExpirationEvery:1 * time.Second}
	m.InitStorage()

	//when
	actualEntries := getDummyCommitlogEntriesForExpirationTest()
	m.MergeWithCommitlog(actualEntries)

	//then
	assert.Equal(t, 1, len(m.memtForTag), "mt count mismatch")

	st1 := m.memtForTag["tagZero"]
	st1e := st1.RetrieveAll()

	assert.Equal(t, 2, len(st1e), "dto count in mt mismatch for tagZero before expiration")

	time.Sleep(m.PerformExpirationEvery * 3)

	st1 = m.memtForTag["tagZero"]
	st1e = st1.RetrieveAll()

	assert.Equal(t, 1, len(st1e), "dto count in mt mismatch for tagZero after expiration")
	assert.Equal(t, uint64(1345), st1e[0].Timestamp, "incorrect timestamp for tagZero after expiration")

	log.Close()
}

func getDummyCommitlogEntriesForMultipleTags() []commitlog.Entry {
	expiresAt := utils.GetNowMillis() + 100000
	ans := make([]commitlog.Entry, 5)
	ans[0] = commitlog.Entry{Key: []byte("tagZero"), Timestamp: 1337, ExpiresAt: expiresAt, Value: make([]byte, 4)}
	ans[1] = commitlog.Entry{Key: []byte("tagOne"), Timestamp: 1339, ExpiresAt: expiresAt, Value: make([]byte, 2)}
	ans[2] = commitlog.Entry{Key: []byte("tagZero"), Timestamp: 1341, ExpiresAt: expiresAt, Value: make([]byte, 16)}
	ans[3] = commitlog.Entry{Key: []byte("tagOne"), Timestamp: 1343, ExpiresAt: expiresAt, Value: make([]byte, 1)}
	ans[4] = commitlog.Entry{Key: []byte("tagZero"), Timestamp: 1345, ExpiresAt: expiresAt, Value: make([]byte, 1)}
	return ans
}

func getDummyCommitlogEntriesForMultipleTags2() []commitlog.Entry {
	expiresAt := utils.GetNowMillis() + 100000
	ans := make([]commitlog.Entry, 2)
	ans[0] = commitlog.Entry{Key: []byte("tagZero"), Timestamp: 1347, ExpiresAt: expiresAt, Value: make([]byte, 4)}
	ans[1] = commitlog.Entry{Key: []byte("tagOne"), Timestamp: 1345, ExpiresAt: expiresAt, Value: make([]byte, 2)}
	return ans
}

func getDummyCommitlogEntriesForExpirationTest() []commitlog.Entry {
	expiresAt := utils.GetNowMillis() + 1000
	ans := make([]commitlog.Entry, 2)
	ans[0] = commitlog.Entry{Key: []byte("tagZero"), Timestamp: 1347, ExpiresAt: expiresAt, Value: make([]byte, 4)}
	ans[1] = commitlog.Entry{Key: []byte("tagZero"), Timestamp: 1345, ExpiresAt: expiresAt + 5000, Value: make([]byte, 2)}
	return ans
}