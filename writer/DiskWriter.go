package writer

import (
	"fmt"
	log "github.com/jeanphorn/log4go"
	"github.com/nikita-tomilov/golsm/commitlog"
	"github.com/nikita-tomilov/golsm/sst"
	"github.com/nikita-tomilov/golsm/utils"
	"sync"
	"time"
)

type DiskWriter struct {
	SstManager           *sst.Manager
	ClManager            *commitlog.Manager
	EntriesPerCommitlog  int
	PeriodBetweenFlushes time.Duration
	currentEntries       int
	mutex                *sync.Mutex
}

func (dbw *DiskWriter) Init() {
	dbw.SstManager.InitStorage()
	dbw.ClManager.Init()
	dbw.currentEntries = 0
	dbw.mutex = &sync.Mutex{}

	go utils.DoEvery(dbw.PeriodBetweenFlushes, func() {
		dbw.trySwitchCommitlog()
	})
}

func (dbw *DiskWriter) Store(e commitlog.Entry) {
	dbw.mutex.Lock()
	dbw.ClManager.Store(e)
	dbw.currentEntries++
	dbw.mutex.Unlock()
	if dbw.currentEntries >= dbw.EntriesPerCommitlog {
		dbw.trySwitchCommitlog()
		dbw.currentEntries = 0
	}
}

func (dbw *DiskWriter) StoreMultiple(e []commitlog.Entry) {
	dbw.mutex.Lock()
	dbw.ClManager.StoreMultiple(e)
	dbw.currentEntries += len(e)

	dbw.mutex.Unlock()
	if dbw.currentEntries >= dbw.EntriesPerCommitlog {
		dbw.trySwitchCommitlog()
		dbw.currentEntries = 0
	}
}

func (dbw *DiskWriter) trySwitchCommitlog() {
	dbw.mutex.Lock()
	currentEntries := dbw.ClManager.RetrieveAll()
	if len(currentEntries) > 0 {
		log.Debug("Switching commitlogs")
		dbw.ClManager.SwapCommitlogs()
		dbw.ClManager.ClearPrevious()
		dbw.SstManager.MergeWithCommitlog(currentEntries)

		log.Debug(fmt.Sprintf("%d entries sent to SST", len(currentEntries)))
	}
	dbw.mutex.Unlock()
}
