package golsm

import (
	"fmt"
	"github.com/nikita-tomilov/golsm/commitlog"
	"github.com/nikita-tomilov/golsm/dto"
	"github.com/nikita-tomilov/golsm/memt"
	"github.com/nikita-tomilov/golsm/sst"
	"github.com/nikita-tomilov/golsm/utils"
	"github.com/nikita-tomilov/golsm/writer"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestLSM_StorageWriterWorks(t *testing.T) {
	//given
	clm := commitlog.Manager{Path: fmt.Sprintf("/tmp/golsm_test/diskwriter/commitlog-%d-%d", utils.GetNowMillis(), utils.GetTestIdx())}
	sstm := sst.Manager{RootDir: fmt.Sprintf("/tmp/golsm_test/diskwriter/sstm-%d-%d", utils.GetNowMillis(), utils.GetTestIdx())}
	dw := writer.DiskWriter{SstManager: &sstm, ClManager: &clm, EntriesPerCommitlog: 10, PeriodBetweenFlushes: 5 * time.Second}
	dw.Init()

	memtm := memt.Manager{MaxEntriesPerTag: 9999}
	memtm.InitStorage()

	storageWriter := StorageWriter{MemTable: &memtm, DiskWriter: &dw}
	storageWriter.Init()
	const tagName = "whatever"
	const expiration = 0

	dummyData := buildDummyData(25)

	//when
	storageWriter.Store(slice(dummyData, tagName, 0, 16), expiration)
	time.Sleep(1 * time.Second)
	storageWriter.Store(slice(dummyData, tagName, 16, 20), expiration)
	time.Sleep(1 * time.Second)
	storageWriter.Store(slice(dummyData, tagName, 20, 25), expiration)
	time.Sleep(10 * time.Second)

	storedDataOnDisk := sstm.ManagerForTag(tagName).GetAllEntries()
	storedDataInMemT := memtm.MemTableForTag(tagName).RetrieveAll()

	//then
	assert.Equal(t, len(dummyData), len(storedDataOnDisk), "some dto was lost")
	assert.Equal(t, len(dummyData), len(storedDataInMemT), "some dto was lost")
	for i := 0; i < 25; i++ {
		assert.Equal(t, dummyData[i].Timestamp, storedDataOnDisk[i].Timestamp, "entry timestamp incorrect")
		assert.Equal(t, dummyData[i].Value, storedDataOnDisk[i].Value, "entry value incorrect")

		assert.Equal(t, dummyData[i].Timestamp, storedDataInMemT[i].Timestamp, "entry timestamp incorrect")
		assert.Equal(t, dummyData[i].Value, storedDataInMemT[i].Value, "entry value incorrect")
	}
}

func TestLSM_StorageReaderWorks(t *testing.T) {
	storageReader, storageWriter := InitStorage(
		fmt.Sprintf("/tmp/golsm_test/diskwriter/commitlog-%d-%d", utils.GetNowMillis(), utils.GetTestIdx()),
		10,
		5*time.Second,
		10*time.Second,
		fmt.Sprintf("/tmp/golsm_test/diskwriter/sstm-%d-%d", utils.GetNowMillis(), utils.GetTestIdx()),
		9999)

	const tagName = "whatever"
	const expiration = 0

	dummyData := buildDummyData(25)

	//when
	storageWriter.Store(slice(dummyData, tagName, 0, 25), expiration)
	retrievedData := storageReader.Retrieve(toList(tagName), 1336, 1500)
	availFrom, availTo := storageReader.Availability()

	//then
	assert.Equal(t, 1, len(retrievedData), "weird stuff returned from StorageReader")
	assert.Equal(t, dummyData[0].Timestamp, availFrom, "availFrom incorrect")
	assert.Equal(t, dummyData[24].Timestamp, availTo, "availTo incorrect")
	retrievedDataForTag := retrievedData[tagName]
	assert.Equal(t, len(dummyData), len(retrievedDataForTag), "some dto was lost")
	for i := 0; i < 25; i++ {
		assert.Equal(t, dummyData[i].Timestamp, retrievedDataForTag[i].Timestamp, "measurement timestamp incorrect")
		assert.Equal(t, dummyData[i].Value, retrievedDataForTag[i].Value, "measurement value incorrect")
	}
}

func buildDummyData(count int) []dto.Measurement {
	ans := make([]dto.Measurement, count)
	for i := 0; i < count; i++ {
		ans[i] = dto.Measurement{Timestamp: 1337 + uint64(i), Value: make([]byte, 4)}
	}
	return ans
}

func slice(data []dto.Measurement, tag string, from int, to int) map[string][]dto.Measurement {
	ans := make(map[string][]dto.Measurement)
	ans[tag] = make([]dto.Measurement, 0)
	for i := from; i < to; i++ {
		ans[tag] = append(ans[tag], data[i])
	}
	return ans
}

func toList(tag string) []string {
	ans := make([]string, 1)
	ans[0] = tag
	return ans
}
