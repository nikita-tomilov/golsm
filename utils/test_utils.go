package utils

var testIdx int = 1

func GetTestIdx() int {
	testIdx += 1
	return testIdx
}