package memt

import (
	"encoding/json"
	"github.com/google/btree"
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


func (e *Entry) Less(than btree.Item) bool {
	oe := than.(*Entry)
	return e.Timestamp < oe.Timestamp
}