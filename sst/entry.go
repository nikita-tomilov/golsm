package sst

import (
	"encoding/binary"
	"encoding/json"
)

type Entry struct {
	Timestamp uint64
	ExpiresAt uint64
	Value     []byte
}

func (e *Entry) ToString() string {
	b, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func FromByteArray(arr []uint8) Entry {
	timestamp := binary.LittleEndian.Uint64(arr)
	expiresAt := binary.LittleEndian.Uint64(arr[8:])
	value := arr[16:]
	return Entry{
		Timestamp: timestamp,
		ExpiresAt: expiresAt,
		Value:     value,
	}
}

func (e *Entry) ToByteArray() []uint8 {
	entryLen := len(e.Value) + 8 + 8
	arr := make([]byte, entryLen)
	binary.LittleEndian.PutUint64(arr, e.Timestamp)
	binary.LittleEndian.PutUint64(arr[8:], e.ExpiresAt)
	copy(arr[16:], e.Value)
	return arr
}

func (e *Entry) ToByteArrayWithLength() []uint8 {
	dest := make([]byte, 2)
	arr := e.ToByteArray()
	binary.LittleEndian.PutUint16(dest, uint16(len(arr)))
	return append(dest[:], arr[:]...)
}
