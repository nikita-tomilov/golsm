package main

import (
	"fmt"
	"github.com/nikita-tomilov/golsm/commitlog"
	"github.com/nikita-tomilov/golsm/sst"
	"github.com/nikita-tomilov/golsm/utils"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestDiskWriter_DataIsNotLost(t *testing.T) {
	//given
	clm := commitlog.Manager{Path: fmt.Sprintf("/tmp/golsm_test/diskwriter/commitlog-%d-%d", utils.GetNowMillis(), utils.GetTestIdx())}
	sstm := sst.Manager{RootDir: fmt.Sprintf("/tmp/golsm_test/diskwriter/sstm-%d-%d", utils.GetNowMillis(), utils.GetTestIdx())}
	diskWriter := DiskWriter{SstManager:&sstm, ClManager:&clm, EntriesPerCommitlog: 10, PeriodBetweenFlushes: 5 * time.Second}
	diskWriter.Init()

	dummyData := make([]commitlog.Entry, 25)
	for i := 0; i < 25; i++ {
		dummyData[i] = commitlog.Entry{Key: []byte("whatever"), Timestamp: 1337 + uint64(i), ExpiresAt: 0, Value: make([]byte, 4)}
	}

	//when
	for i := 0; i < 16; i++ {
		diskWriter.Store(dummyData[i])
	}
	time.Sleep(1 * time.Second)
	for i := 16; i < 20; i++ {
		diskWriter.Store(dummyData[i])
	}
	time.Sleep(1 * time.Second)
	for i := 20; i < 25; i++ {
		diskWriter.Store(dummyData[i])
	}
	time.Sleep(10 * time.Second)
	writtenData := sstm.ManagerForTag("whatever").GetAllEntries()

	//then
	assert.Equal(t, len(dummyData), len(writtenData), "some data was lost")
	for i := 0; i < 25; i++ {
		assert.Equal(t, dummyData[i].Timestamp, writtenData[i].Timestamp, "entry timestamp incorrect")
		assert.Equal(t, dummyData[i].Value, writtenData[i].Value, "entry value incorrect")
	}
}
