package util

func CalKeyIndex(key []byte, memIndexNum *int) int {
	sum := 0
	for _, ch := range key {
		sum += int(ch)
	}
	return sum & (*memIndexNum - 1)
}
