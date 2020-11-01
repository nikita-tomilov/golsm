package golsm

import (
	"github.com/nikita-tomilov/golsm/commitlog"
	"github.com/nikita-tomilov/golsm/memt"
	"github.com/nikita-tomilov/golsm/sst"
	"github.com/nikita-tomilov/golsm/writer"
	"time"
)

func InitStorage(commitlogPath string, entriesPerCommitlog int, periodBetweenFlushes time.Duration, memtPerformExpirationEvery time.Duration, memtPrefetchSeconds time.Duration, sstPath string, memtMaxEntriesPerTag int) (*StorageReader, *StorageWriter) {
	clm := commitlog.Manager{Path: commitlogPath}
	sstm := sst.Manager{RootDir: sstPath}
	dw := writer.DiskWriter{SstManager: &sstm, ClManager: &clm, EntriesPerCommitlog: entriesPerCommitlog, PeriodBetweenFlushes: periodBetweenFlushes}
	dw.Init()

	memtm := memt.Manager{MaxEntriesPerTag: memtMaxEntriesPerTag, PerformExpirationEvery: memtPerformExpirationEvery}
	memtm.InitStorage()

	storageWriter := StorageWriter{MemTable: &memtm, DiskWriter: &dw}
	storageWriter.Init()

	storageReader := StorageReader{MemTable: &memtm, SSTManager: &sstm, MemtPrefetch:memtPrefetchSeconds}
	storageReader.Init()

	return &storageReader, &storageWriter
}
