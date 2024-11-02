package errors

import "fmt"

var (
	ErrCrcNotValid          = fmt.Errorf("error erc verification fail")
	ErrReadEmptyDiskFile    = fmt.Errorf("error read empty disk file")
	ErrInvalidSuffixFormat  = fmt.Errorf("error suffix format is invalid")
	ErrFileSizeExceed       = fmt.Errorf("error exceed filesize")
	ErrInvalidPartKeyFormat = fmt.Errorf("error blockPart key format is invalid")
	ErrDiskFileNotFound     = fmt.Errorf("error diskfile is not found")
	ErrInvalidProvFileName  = fmt.Errorf("error prov file name is invalid")
	ErrProvFileNotFound     = fmt.Errorf("error prov file is not found")
)
