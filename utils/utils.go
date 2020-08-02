package utils

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"
	"os"
)

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
		log.Fatal("encode error:", err)
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
		log.Fatal("decode error:", err)
		return nil, err
	}
	return x, nil
}

func Check(e error) {
	if e != nil {
		log.Fatal(e)
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
