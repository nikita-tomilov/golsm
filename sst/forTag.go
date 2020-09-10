package sst

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/google/btree"
	log "github.com/jeanphorn/log4go"
	"github.com/nikita-tomilov/golsm/commitlog"
	"github.com/nikita-tomilov/golsm/utils"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type SSTforTag struct {
	Tag                     string
	FileName                string
	PerformCompactionEvery  time.Duration
	file                    *os.File
	mutex                   *sync.Mutex
	index                   *btree.BTree
	nextCompactionTimestamp uint64
}

func (st *SSTforTag) InitStorage() {
	dir, _ := filepath.Split(st.FileName)
	os.MkdirAll(dir, os.ModePerm)
	st.index = btree.New(10)
	if st.PerformCompactionEvery == 0 {
		st.PerformCompactionEvery = time.Minute * 10
	}
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
}

func (st *SSTforTag) initOverExistingFile() {
	file, err := os.OpenFile(st.FileName, os.O_APPEND|os.O_WRONLY, 0644)
	utils.Check(err)
	st.file = file
	st.mutex = &sync.Mutex{}
	st.rebuildIndex()
}

func (st *SSTforTag) reopenFile() {
	file, err := os.OpenFile(st.FileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	utils.Check(err)
	st.file = file
}

func (st *SSTforTag) rebuildIndex() {
	st.iterateOverFileAndApplyForAllEntries(func(e Entry, o int64) {
		st.index.ReplaceOrInsert(buildIndexEntry(e.Timestamp, o, e.ExpiresAt))
	})
}

func (st *SSTforTag) GetAllEntries() []Entry {
	ans := make([]Entry, 0)
	st.iterateOverFileAndApplyForAllEntries(func(e Entry, o int64) {
		ans = append(ans, e)
	})
	sort.Slice(ans, func(i, j int) bool {
		return ans[i].Timestamp < ans[j].Timestamp
	})
	return ans
}

func (st *SSTforTag) iterateOverFileAndApplyForAllEntries(receiver func(Entry, int64)) {
	st.iterateOverFileAndApplyForEntries(0, int((^uint(0))>>1), receiver)
}

func (st *SSTforTag) iterateOverFileAndApplyForEntries(fileOffsetBytes int64, entriesCount int, receiver func(Entry, int64)) {
	st.mutex.Lock()
	file, err := os.OpenFile(st.FileName, os.O_RDONLY, 0644)
	utils.Check(err)

	if fileOffsetBytes > 0 {
		_, ok := file.Seek(fileOffsetBytes, 0)
		utils.Check(ok)
	}
	reader := bufio.NewReader(file)

	readerFileOffset := int64(fileOffsetBytes)
	prevFileOffset := int64(fileOffsetBytes)
	entriesParsed := 0
	prevEntry := Entry{Timestamp: 0}
	sizeBuf := make([]uint8, 2)
	for {
		n, err := io.ReadFull(reader, sizeBuf)
		if n != 2 {
			break
		}
		readerFileOffset += int64(n)
		utils.Check(err)
		entrySize := int(binary.LittleEndian.Uint16(sizeBuf))
		entryBytes := make([]uint8, entrySize)
		n2, err := io.ReadFull(reader, entryBytes)
		utils.Check(err)
		if n2 != entrySize {
			panic(fmt.Sprintf("read seems to be failed; expected to read %d, managed to read %d, parsed %d entries", entrySize, n2, entriesParsed))
		}
		readerFileOffset += int64(n2)
		entry := FromByteArray(entryBytes)
		if entry.Timestamp < prevEntry.Timestamp {
			panic(fmt.Sprintf("SST was not sorted! prevEntry TS %d, now TS %d", prevEntry.Timestamp, entry.Timestamp))
		}
		prevEntry = entry
		receiver(entry, prevFileOffset)
		prevFileOffset = readerFileOffset
		entriesParsed += 1
		if entriesParsed >= entriesCount {
			break
		}
	}

	err = file.Close()
	utils.Check(err)
	st.mutex.Unlock()
}

func (st *SSTforTag) getCurrentMinTimestamp() uint64 {
	min := st.index.Min()
	if min == nil {
		return 0
	}
	mine := min.(IndexEntry)
	if (mine.expiresAt != 0) && (mine.expiresAt < utils.GetNowMillis()) {
		st.performExpirationWithinIndex()
		return st.getCurrentMinTimestamp()
	} else {
		return mine.ts
	}
}

func (st *SSTforTag) getCurrentMaxTimestamp() uint64 {
	max := st.index.Max()
	if max == nil {
		return 0
	}
	maxe := max.(IndexEntry)
	if (maxe.expiresAt != 0) && (maxe.expiresAt < utils.GetNowMillis()) {
		st.performExpirationWithinIndex()
		return st.getCurrentMaxTimestamp()
	} else {
		return maxe.ts
	}
}

func (st *SSTforTag) performExpirationWithinIndex() {
	toBeDeleted := make([]IndexEntry, 0)
	now := utils.GetNowMillis()
	st.index.Ascend(func(i btree.Item) bool {
		oe := i.(IndexEntry)
		if (oe.expiresAt != 0) && (oe.expiresAt < now) {
			toBeDeleted = append(toBeDeleted, oe)
		}
		return true
	})
	for _, i := range toBeDeleted {
		st.index.Delete(i)
	}
}

func (st *SSTforTag) MergeWithCommitlog(commitlogEntries []commitlog.Entry) {
	sorted := commitlogEntries
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp < sorted[j].Timestamp
	})
	minimalTimestamp := sorted[0].Timestamp
	if st.getCurrentMinTimestamp() != 0 {
		if (minimalTimestamp >= st.getCurrentMaxTimestamp()) && (st.nextCompactionTimestamp > utils.GetNowMillis()) {
			st.appendDataToEndOfTable(sorted)
		} else {
			st.addDataResortingTable(sorted)
		}
	} else {
		st.appendDataToEndOfTable(sorted)
	}
}

func (st *SSTforTag) appendDataToEndOfTable(commitlogEntries []commitlog.Entry) {
	log.Debug("Appending to end of table")
	st.mutex.Lock()
	offset, err := st.file.Seek(0, utils.WhenceRelativeToEndOfFile)
	utils.Check(err)
	writer := bufio.NewWriter(st.file)
	for _, entry := range commitlogEntries {
		sstEntry := Entry{Timestamp: entry.Timestamp, ExpiresAt: entry.ExpiresAt, Value: entry.Value}
		st.index.ReplaceOrInsert(buildIndexEntry(sstEntry.Timestamp, offset, sstEntry.ExpiresAt))
		offset += writeEntryToFile(sstEntry, writer)
	}
	err = writer.Flush()
	st.file.Sync()
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

	//over sstable
	st.iterateOverFileAndApplyForAllEntries(func(sstEntry Entry, o int64) {
		banExistingEntry := false
		for idx < len(commitlogEntries) {
			commitlogEntry := commitlogEntries[idx]
			if commitlogEntry.Timestamp <= sstEntry.Timestamp {
				newSstEntry := Entry{Timestamp: commitlogEntry.Timestamp, ExpiresAt: commitlogEntry.ExpiresAt, Value: commitlogEntry.Value}
				writeEntryToFile(newSstEntry, writer)
				//log.Debug("write new %d", newSstEntry.Timestamp)
				if commitlogEntry.Timestamp == sstEntry.Timestamp {
					banExistingEntry = true
				}
				idx++
			} else {
				break
			}
		}
		if !banExistingEntry {
			writeEntryToFile(sstEntry, writer)
			//log.Debug("write exis %d", sstEntry.Timestamp)
		} else {
			log.Warn("Not writing old entry for tag %s ts %d as there is newer entry", st.Tag, sstEntry.Timestamp)
		}
	})

	st.mutex.Lock()

	//over still unprocessed new commitlog entries, if there are any
	for idx < len(commitlogEntries) {
		newEntry := commitlogEntries[idx]
		sstEntry := Entry{Timestamp: newEntry.Timestamp, ExpiresAt: newEntry.ExpiresAt, Value: newEntry.Value}
		writeEntryToFile(sstEntry, writer)
		idx += 1
	}

	writer.Flush()
	copyFile.Sync()
	copyFile.Close()

	st.file.Close()
	err = os.Rename(copyFileName, st.FileName)
	utils.Check(err)
	st.mutex.Unlock()
	st.reopenFile()
	st.rebuildIndex()
	st.nextCompactionTimestamp = utils.GetNowMillis() + uint64(st.PerformCompactionEvery.Milliseconds())
}

func (st *SSTforTag) GetEntriesWithoutIndex(fromTs uint64, toTs uint64) []Entry {
	if st.index.Len() == 0 {
		return []Entry{}
	}
	ans := make([]Entry, 0)
	now := utils.GetNowMillis()
	st.iterateOverFileAndApplyForAllEntries(func(e Entry, o int64) {
		if (e.Timestamp > 0) && (e.Timestamp >= fromTs) && (e.Timestamp <= toTs) && ((e.ExpiresAt == 0) || (e.ExpiresAt >= now)) {
			ans = append(ans, e)
		}
	})
	return ans
}

func (st *SSTforTag) GetEntriesWithIndex(fromTs uint64, toTs uint64) []Entry {
	count := 0
	firstOffset := int64(-1)
	now := utils.GetNowMillis()
	if st.index.Len() == 0 {
		return []Entry{}
	}
	st.mutex.Lock()
	st.index.AscendRange(buildIndexEntry(fromTs, 0, 0), buildIndexEntry(toTs+1, 0, 0), func(i btree.Item) bool {
		oe := i.(IndexEntry)
		if (oe.expiresAt != 0) && (oe.expiresAt < now) {
			return true
		}
		//log.Debug(fmt.Sprintf("ascendRange on tag %s entry ts %d offset %d", st.Tag, oe.ts, oe.fileOffset))
		if firstOffset == -1 {
			firstOffset = oe.fileOffset
		}
		count += 1
		return true
	})
	st.mutex.Unlock()
	ans := make([]Entry, 0)
	countInFile := 0
	st.iterateOverFileAndApplyForEntries(firstOffset, count, func(e Entry, i int64) {
		if (e.Timestamp > 0) && (e.Timestamp >= fromTs) && (e.Timestamp <= toTs) && ((e.ExpiresAt == 0) || (e.ExpiresAt >= now)) {
			ans = append(ans, e)
		}
		countInFile++
	})
	if len(ans) != count {
		panic(fmt.Sprintf("MISMATCH IN LENGTH ON TAG %s: INDEX SAID %d, IN REALITY WAS %d", st.Tag, count, len(ans)))
	}
	return ans
}

func (st *SSTforTag) Availability() (uint64, uint64) {
	return st.getCurrentMinTimestamp(), st.getCurrentMaxTimestamp()
}

func writeEntryToFile(e Entry, w *bufio.Writer) int64 {
	if (e.ExpiresAt != 0) && (e.ExpiresAt < utils.GetNowMillis()) {
		log.Debug("Attempt to WriteEntryToFile that was expired")
		return 0
	}
	bytes := e.ToByteArrayWithLength()
	n, err := w.Write(bytes)
	utils.Check(err)
	if n != len(bytes) {
		panic("write seems to be failed")
	}
	return int64(n)
	//log.Debug(fmt.Sprintf("Wrote disk entry for ts %d of bytes count %d", e.Timestamp, len(bytes)))
}

type IndexEntry struct {
	ts         uint64
	fileOffset int64
	expiresAt  uint64
}

func (e IndexEntry) Less(than btree.Item) bool {
	oe := than.(IndexEntry)
	return e.ts < oe.ts
}

func buildIndexEntry(ts uint64, offset int64, expiresAt uint64) btree.Item {
	return IndexEntry{ts, offset, expiresAt}
}
