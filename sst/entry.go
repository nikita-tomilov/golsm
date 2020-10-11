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
	value := make([]byte, len(arr) - 16)
	for i := 16; i < len(arr); i++ {
		value[i - 16] = arr[i]
	}
	return Entry{
		Timestamp: timestamp,
		ExpiresAt: expiresAt,
		Value:     value,
	}
}

func (e *Entry) ToByteArrayWithLength() []uint8 {
	entryLen := len(e.Value) + 8 + 8
	arr := make([]byte, entryLen + 2)
	binary.LittleEndian.PutUint64(arr[2:], e.Timestamp)
	binary.LittleEndian.PutUint64(arr[10:], e.ExpiresAt)
	for i := 18; i < len(arr); i++ {
		arr[i] = e.Value[i - 18]
	}
	binary.LittleEndian.PutUint16(arr, uint16(entryLen))
	return arr
}