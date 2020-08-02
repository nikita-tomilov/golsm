package utils

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	log "github.com/jeanphorn/log4go"
	"os"
	"time"
)

const WhenceRelativeToEndOfFile = 2

func ToString(e interface{}) string {
	b, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func ToNetworkByteArray(e interface{}) []byte {
	var network bytes.Buffer        // Stand-in for a network connection
	enc := gob.NewEncoder(&network) // Will write to network.
	//var dec = gob.NewDecoder(&network) // Will read from network.
	err := enc.Encode(e)
	if err != nil {
		log.Error("encode error:", err)
	}
	b := network.Bytes()
	return append([]byte{byte(len(b))}, b...)
}

func FromNetworkByteArray(arr []byte, x interface{}) (interface{}, error) {
	var network *bytes.Buffer

	l := arr[0]
	network = bytes.NewBuffer(arr[1 : l+1])

	dec := gob.NewDecoder(network) // Will read from network.
	err := dec.Decode(x)
	if err != nil {
		log.Error("decode error:", err)
		return nil, err
	}
	return x, nil
}

func Check(e error) {
	if e != nil {
		log.Error(e)
		panic(e)
	}
}

func FileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func GetNowMillis() uint64 {
	return uint64(time.Now().UnixNano() / 1000000)
}
