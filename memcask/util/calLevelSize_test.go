package util

import (
	"fmt"
	"runtime"
	"testing"
)

func TestCalLevelSize(t *testing.T) {
	fmt.Println(runtime.NumCPU())
	for i := 0; i < 5; i++ {
		fmt.Println(CalLevelSize(i))
	}
}
