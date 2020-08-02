package commitlog

import (
	"encoding/binary"
	"encoding/json"
)

type Entry struct {
	Key       uint16
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
	//valueLen := len(arr) - 2 - 8 - 8
	key := binary.LittleEndian.Uint16(arr)
	timestamp := binary.LittleEndian.Uint64(arr[2:])
	expiresAt := binary.LittleEndian.Uint64(arr[10:])
	value := arr[18:]
	return Entry{
		Key:       key,
		Timestamp: timestamp,
		ExpiresAt: expiresAt,
		Value:     value,
	}
}

func (e *Entry) ToByteArray() []uint8 {
	len := len(e.Value) + 2 + 8 + 8
	arr := make([]byte, len)
	binary.LittleEndian.PutUint16(arr, e.Key)
	binary.LittleEndian.PutUint64(arr[2:], e.Timestamp)
	binary.LittleEndian.PutUint64(arr[10:], e.ExpiresAt)
	copy(arr[18:], e.Value)
	return arr
}

func (e *Entry) ToByteArrayWithLength() []uint8 {
	dest := make([]byte, 2)
	arr := e.ToByteArray()
	binary.LittleEndian.PutUint16(dest, uint16(len(arr)))
	return append(dest[:], arr[:]...)
}
