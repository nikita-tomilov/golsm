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
)

type SSTforTag struct {
	Tag                     string
	FileName                string
	currentMinimumTimestamp uint64
	currentMaximumTimestamp uint64
	file                    *os.File
	mutex                   *sync.Mutex
	index                   *btree.BTree
}

func (st *SSTforTag) InitStorage() {
	dir, _ := filepath.Split(st.FileName)
	os.MkdirAll(dir, os.ModePerm)
	st.index = btree.New(10)
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
	st.rebuildIndex()
}

func (st *SSTforTag) reopenFile() {
	file, err := os.OpenFile(st.FileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	utils.Check(err)
	st.file = file
}

func (st *SSTforTag) rebuildIndex() {
	st.currentMinimumTimestamp = ^uint64(0)
	st.currentMaximumTimestamp = 0
	st.iterateOverFileAndApplyForAllEntries(func(e Entry, o int64) {
		tryOverrideRange(e, &(st.currentMinimumTimestamp), &(st.currentMaximumTimestamp))
		st.index.ReplaceOrInsert(buildIndexKey(e.Timestamp, o))
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
	st.iterateOverFileAndApplyForEntries(0, int((^uint(0)) >> 1) , receiver)
}

func (st *SSTforTag) iterateOverFileAndApplyForEntries(fileOffsetBytes int64, entriesCount int, receiver func(Entry, int64)) {
	st.mutex.Lock()
	file, err := os.OpenFile(st.FileName, os.O_RDONLY, 0644)
	utils.Check(err)

	if fileOffsetBytes != 0 {
		_, ok := file.Seek(fileOffsetBytes, 0)
		utils.Check(ok)
	}
	reader := bufio.NewReader(file)

	readerFileOffset := int64(0)
	entriesParsed := 0
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
		receiver(entry, readerFileOffset)
		entriesParsed += 1
		if entriesParsed >= entriesCount {
			break
		}
	}

	err = file.Close()
	utils.Check(err)
	st.mutex.Unlock()
}

func (st *SSTforTag) GetFileRange() (uint64, uint64) {
	return st.currentMinimumTimestamp, st.currentMaximumTimestamp
}

func (st *SSTforTag) MergeWithCommitlog(commitlogEntries []commitlog.Entry) {
	//TODO: maybe I should filter by tag directly here to avoid additional O(N)
	minimalTimestamp := commitlogEntries[0].Timestamp
	if st.currentMinimumTimestamp != 0 {
		if minimalTimestamp >= st.currentMaximumTimestamp {
			st.appendDataToEndOfTable(commitlogEntries)
			st.currentMaximumTimestamp = commitlogEntries[len(commitlogEntries)-1].Timestamp
		} else {
			st.addDataResortingTable(commitlogEntries)
		}
	} else {
		st.appendDataToEndOfTable(commitlogEntries)
		st.currentMaximumTimestamp = commitlogEntries[len(commitlogEntries)-1].Timestamp
		st.currentMinimumTimestamp = commitlogEntries[0].Timestamp
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
		st.index.ReplaceOrInsert(buildIndexKey(sstEntry.Timestamp, offset))
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

	minTs := commitlogEntries[0].Timestamp
	maxTs := commitlogEntries[0].Timestamp

	//over sstable
	st.iterateOverFileAndApplyForAllEntries(func(sstEntry Entry, o int64) {
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
		sstEntry := Entry{Timestamp: newEntry.Timestamp, ExpiresAt: newEntry.ExpiresAt, Value: newEntry.Value}
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
	st.mutex.Unlock()
	st.reopenFile()
	st.rebuildIndex()
}

func (st *SSTforTag) GetEntriesWithoutIndex(fromTs uint64, toTs uint64) []Entry {
	ans := make([]Entry, 0)
	st.iterateOverFileAndApplyForAllEntries(func(e Entry, o int64) {
		if (e.Timestamp >= fromTs) && (e.Timestamp <= toTs) {
			ans = append(ans, e)
		}
	})
	return ans
}

func (st *SSTforTag) GetEntriesWithIndex(fromTs uint64, toTs uint64) []Entry {
	count := 0
	firstOffset := int64(0)
	st.index.AscendRange(buildIndexKey(fromTs, 0), buildIndexKey(toTs + 1, 0), func (i btree.Item) bool {
		oe := i.(EntryIndexKey)
		if firstOffset == 0 {
			firstOffset = oe.fileOffset
		}
		//log.Debug(fmt.Sprintf("launched for entry with ts %d", oe.ts))
		count += 1
		return true
	})
	ans := make([]Entry, count)
	it := 0
	st.iterateOverFileAndApplyForEntries(firstOffset, count, func(entry Entry, i int64) {
		ans[it] = entry
		it++
	})
	return ans
}

func writeEntryToFile(e Entry, w *bufio.Writer) int64 {
	bytes := e.ToByteArrayWithLength()
	n, err := w.Write(bytes)
	utils.Check(err)
	if n != len(bytes) {
		panic("write seems to be failed")
	}
	return int64(n)
	//log.Debug(fmt.Sprintf("Wrote disk entry for ts %d of bytes count %d", e.Timestamp, len(bytes)))
}

func tryOverrideRange(e Entry, minTs *uint64, maxTs *uint64) {
	if e.Timestamp < *minTs {
		*minTs = e.Timestamp
	}
	if e.Timestamp > *maxTs {
		*maxTs = e.Timestamp
	}
}

type EntryIndexKey struct {
	ts         uint64
	fileOffset int64
}

func (e EntryIndexKey) Less(than btree.Item) bool {
	oe := than.(EntryIndexKey)
	return e.ts < oe.ts
}

func buildIndexKey(ts uint64, offset int64) btree.Item {
	return EntryIndexKey{ts, offset}
}