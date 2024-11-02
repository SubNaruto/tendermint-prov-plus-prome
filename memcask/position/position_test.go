package position

import (
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestPosition(t *testing.T) {
	pos := &Position{
		Filename: "hel2112321312lo.txt",
		Offset:   math.MaxInt64,
	}
	enc := pos.Encode()

	p := DecodePosition(enc)

	assert.Equal(t, p.Filename, "hel2112321312lo.txt")
	assert.Equal(t, p.Offset, math.MaxInt64)
}
