package sst

import (
	"bufio"
	"encoding/binary"
	log "github.com/jeanphorn/log4go"
	"github.com/nikita-tomilov/golsm/commitlog"
	"github.com/nikita-tomilov/golsm/utils"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type SSTforTag struct {
	Tag string
	FileName string
	currentMinimumTimestamp uint64
	currentMaximumTimestamp uint64
	file *os.File
	mutex *sync.Mutex
}

func (st *SSTforTag) InitStorage() {
	dir, _ := filepath.Split(st.FileName)
	os.MkdirAll(dir, os.ModePerm)
	if utils.FileExists(st.FileName) {
		st.initOverExistingFile()
	} else {
		st.initOverNewFile()
	}
}

func (st *SSTforTag) initOverNewFile() {
	file, err := os.OpenFile(st.FileName, os.O_CREATE|os.O_WRONLY, 0644)
	utils.Check(err)
	st.file = file
	st.mutex = &sync.Mutex{}
	st.currentMinimumTimestamp = 0
	st.currentMaximumTimestamp = 0
}

func (st *SSTforTag) initOverExistingFile() {
	file, err := os.OpenFile(st.FileName, os.O_APPEND|os.O_WRONLY, 0644)
	utils.Check(err)
	st.file = file
	st.mutex = &sync.Mutex{}
	st.currentMinimumTimestamp = ^uint64(0)
	st.currentMaximumTimestamp = 0
	st.iterateOverFileAndApplyForAllEntries(func (e Entry) {
		tryOverrideRange(e, &(st.currentMinimumTimestamp), &(st.currentMaximumTimestamp))
	})
}

func (st *SSTforTag) reopenFile() {
	file, err := os.OpenFile(st.FileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	utils.Check(err)
	st.file = file
}

func (st *SSTforTag) GetAllEntries() []Entry {
	ans := make([]Entry, 0)
	st.iterateOverFileAndApplyForAllEntries(func(e Entry) {
		ans = append(ans, e)
	})
	return ans
}

func (st *SSTforTag) iterateOverFileAndApplyForAllEntries(receiver func(Entry)) {
	st.mutex.Lock()
	file, err := os.OpenFile(st.FileName, os.O_RDONLY, 0644)
	utils.Check(err)

	reader := bufio.NewReader(file)

	sizeBuf := make([]uint8, 2)
	for {
		n, err := reader.Read(sizeBuf)
		if n != 2 {
			break
		}
		utils.Check(err)
		entrySize := int(binary.LittleEndian.Uint16(sizeBuf))
		entryBytes := make([]uint8, entrySize)
		n2, err := reader.Read(entryBytes)
		utils.Check(err)
		if n2 != entrySize {
			panic("read seems to be failed")
		}
		entry := FromByteArray(entryBytes)
		receiver(entry)
	}

	err = file.Close()
	utils.Check(err)
	st.mutex.Unlock()
}

func (st *SSTforTag) GetFileRange() (uint64, uint64) {
	return st.currentMinimumTimestamp, st.currentMaximumTimestamp
}

func (st *SSTforTag) MergeWithCommitlog(commitlogEntries []commitlog.Entry) {
	sort.Slice(commitlogEntries, func (i, j int) bool {
		return commitlogEntries[i].Timestamp < commitlogEntries[j].Timestamp
	})
	minimalTimestamp := commitlogEntries[0].Timestamp
	if st.currentMinimumTimestamp != 0 {
		if minimalTimestamp >= st.currentMaximumTimestamp {
			st.appendDataToEndOfTable(commitlogEntries)
			st.currentMaximumTimestamp = commitlogEntries[len(commitlogEntries) - 1].Timestamp
		} else {
			st.addDataResortingTable(commitlogEntries)
		}
	} else {
		st.appendDataToEndOfTable(commitlogEntries)
		st.currentMaximumTimestamp = commitlogEntries[len(commitlogEntries) - 1].Timestamp
		st.currentMinimumTimestamp = commitlogEntries[0].Timestamp
	}
}

func (st *SSTforTag) appendDataToEndOfTable(commitlogEntries []commitlog.Entry) {
	log.Debug("Appending to end of table")
	st.mutex.Lock()
	_, err := st.file.Seek(0, utils.WhenceRelativeToEndOfFile)
	utils.Check(err)
	writer := bufio.NewWriter(st.file)
	for _, entry := range commitlogEntries {
		sstEntry := Entry{Timestamp:entry.Timestamp, ExpiresAt:entry.ExpiresAt, Value:entry.Value}
		writeEntryToFile(sstEntry, writer)
	}
	err = writer.Flush()
	utils.Check(err)
	st.mutex.Unlock()
}

func (st *SSTforTag) addDataResortingTable(commitlogEntries []commitlog.Entry) {
	log.Debug("Adding and resorting the table")
	//TODO: what should I do if there is equal TS in both commitlog and already existing file?
	copyFileName := st.FileName + ".copy"
	copyFile, err := os.OpenFile(copyFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	utils.Check(err)
	writer := bufio.NewWriter(copyFile)
	idx := 0

	minTs := commitlogEntries[0].Timestamp
	maxTs := commitlogEntries[0].Timestamp

	//over sstable
	st.iterateOverFileAndApplyForAllEntries(func (sstEntry Entry) {
		if idx < len(commitlogEntries) {
			commitlogEntry := commitlogEntries[idx]
			if commitlogEntry.Timestamp < sstEntry.Timestamp {
				newSstEntry := Entry{Timestamp: commitlogEntry.Timestamp, ExpiresAt: commitlogEntry.ExpiresAt, Value: commitlogEntry.Value}
				writeEntryToFile(newSstEntry, writer)
				tryOverrideRange(newSstEntry, &minTs, &maxTs)
				idx++
			}
		}
		writeEntryToFile(sstEntry, writer)
		tryOverrideRange(sstEntry, &minTs, &maxTs)
	})

	//over still unprocessed new commitlog entries, if there are any
	if idx < len(commitlogEntries) {
		newEntry := commitlogEntries[idx]
		sstEntry := Entry{Timestamp:newEntry.Timestamp, ExpiresAt:newEntry.ExpiresAt, Value:newEntry.Value}
		writeEntryToFile(sstEntry, writer)
		tryOverrideRange(sstEntry, &minTs, &maxTs)
		idx += 1
	}

	writer.Flush()
	copyFile.Sync()
	copyFile.Close()

	st.mutex.Lock()
	st.file.Close()
	err = os.Rename(copyFileName, st.FileName)
	utils.Check(err)
	st.reopenFile()
	st.currentMinimumTimestamp = minTs
	st.currentMaximumTimestamp = maxTs
	st.mutex.Unlock()
}

func writeEntryToFile(e Entry, w *bufio.Writer) {
	bytes := e.ToByteArrayWithLength()
	n, err := w.Write(bytes)
	utils.Check(err)
	if n != len(bytes) {
		panic("write seems to be failed")
	}
	log.Debug("Wrote entry " + e.ToString())
}

func tryOverrideRange(e Entry, minTs *uint64, maxTs *uint64) {
	if e.Timestamp < *minTs {
		*minTs = e.Timestamp
	}
	if e.Timestamp > *maxTs {
		*maxTs = e.Timestamp
	}
}