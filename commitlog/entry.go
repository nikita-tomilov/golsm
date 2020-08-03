package commitlog

import (
	"encoding/binary"
	"encoding/json"
)

type Entry struct {
	Key       []byte
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
	keyLen := binary.LittleEndian.Uint16(arr)
	key := arr[2:(keyLen + 2)]
	timestamp := binary.LittleEndian.Uint64(arr[(keyLen + 2):])
	expiresAt := binary.LittleEndian.Uint64(arr[(keyLen + 10):])
	value := arr[(keyLen + 18):]
	return Entry{
		Key:       key,
		Timestamp: timestamp,
		ExpiresAt: expiresAt,
		Value:     value,
	}
}

func (e *Entry) ToByteArray() []uint8 {
	keyLen := len(e.Key)
	payloadLen := keyLen + len(e.Value) + 8 + 8 + 2
	arr := make([]byte, payloadLen)
	binary.LittleEndian.PutUint16(arr, uint16(keyLen))
	copy(arr[2:], e.Key)
	binary.LittleEndian.PutUint64(arr[(keyLen + 2):], e.Timestamp)
	binary.LittleEndian.PutUint64(arr[(keyLen + 10):], e.ExpiresAt)
	copy(arr[(keyLen + 18):], e.Value)
	return arr
}

func (e *Entry) ToByteArrayWithLength() []uint8 {
	dest := make([]byte, 2)
	arr := e.ToByteArray()
	binary.LittleEndian.PutUint16(dest, uint16(len(arr)))
	return append(dest[:], arr[:]...)
}
