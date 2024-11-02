package util

import (
	"github.com/tendermint/tendermint/memcask/errors"
	"strconv"
	"strings"
)

/*	GetPartIndexFromKey
 *  @Description: 根据分片key获取分片索引 ( P:Height:Index -> P:20:12 -> 12 )
 *  @param key: 分片的key
 *  @return int: 分片的索引
 *  @return error: 错误信息
 */
func GetPartIndexFromKey(key string) (int, error) {
	parts := strings.Split(key, ":")
	if len(parts) != 3 {
		return 0, errors.ErrInvalidPartKeyFormat
	}
	n, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return 0, err
	}
	return int(n), nil
}
