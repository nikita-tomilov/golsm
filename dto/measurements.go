package dto

type Measurement struct {
	Timestamp uint64
	Value     []byte
}

type TaggedMeasurement struct {
	Tag       string
	Timestamp uint64
	Value     []byte
}
