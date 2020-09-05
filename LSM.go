package golsm

import (
	"github.com/nikita-tomilov/golsm/commitlog"
	"github.com/nikita-tomilov/golsm/memt"
	"github.com/nikita-tomilov/golsm/sst"
	"github.com/nikita-tomilov/golsm/writer"
	"time"
)

func InitStorage(commitlogPath string, entriesPerCommitlog int, periodBetweenFlushes time.Duration, memtExpirationPeriod time.Duration, sstPath string, memtMaxEntriesPerTag int) (*StorageReader, *StorageWriter) {
	clm := commitlog.Manager{Path: commitlogPath}
	sstm := sst.Manager{RootDir: sstPath}
	dw := writer.DiskWriter{SstManager: &sstm, ClManager: &clm, EntriesPerCommitlog: entriesPerCommitlog, PeriodBetweenFlushes: periodBetweenFlushes}
	dw.Init()

	memtm := memt.Manager{MaxEntriesPerTag: memtMaxEntriesPerTag, PerformExpirationEvery: memtExpirationPeriod}
	memtm.InitStorage()

	storageWriter := StorageWriter{MemTable: &memtm, DiskWriter: &dw}
	storageWriter.Init()

	storageReader := StorageReader{MemTable: &memtm, SSTManager: &sstm}
	storageReader.Init()

	return &storageReader, &storageWriter
}
