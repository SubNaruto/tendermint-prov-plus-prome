package util

import (
	"github.com/tendermint/tendermint/memcask/errors"
	"strconv"
	"strings"
)

/*	GetFileNumByFilename
 *  @Description: 根据文件名获取文件编号 ( 00001.data -> 1 )
 *  @param filename: 文件名
 *  @return int64: 文件编号
 *  @return error: 错误信息
 */
func GetFileNumByFilename(filename string) (int64, error) {
	parts := strings.Split(filename, ".")
	if len(parts) != 2 {
		return 0, errors.ErrInvalidSuffixFormat
	}
	fileNumber, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, err
	}
	return fileNumber, nil
}
