package util

import (
	"bytes"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/tendermint/tendermint/crypto/tmhash"
	tmproto "github.com/tendermint/tendermint/proto/tendermint/types"
	"github.com/tendermint/tendermint/types"
)

func GenProvKey(entityId string) string {
	return entityId + "-prov-key"
}

func GenProvResKey(entityId string) string {
	return entityId + "-prov-res-key"
}

func VerifyProvDataList(provDataList *tmproto.ProvDataList) error {
	var ok bool
	var version uint32

	// 遍历所有溯源数据
	for _, provData := range provDataList.ProvDataList {
		// 判断版本号验证完整性
		if !ok {
			version = provData.Tx.TxBody.Version
			ok = true
		} else {
			if version+1 != provData.Tx.TxBody.Version {
				return fmt.Errorf("version is not continuous")
			} else {
				version = provData.Tx.TxBody.Version
			}
		}

		pb, err := proto.Marshal(provData.Tx.TxBody)
		if err != nil {
			return err
		}

		// 通过VO验证正确性
		err = verifyTxByVO(provData.VO,
			&tmproto.Transaction{
				Key:   []byte(provData.Tx.Key),
				Value: pb,
			},
			provData.MBTreeRoot)
		if err != nil {
			return err
		}
	}

	return nil
}

// verifyTxByVO 根据VO验证tx是否正确无误
func verifyTxByVO(vo *tmproto.VO, tx *tmproto.Transaction, mbTreeRootHash []byte) error {
	var hash []byte // 最终计算出的哈希值

	for i := len(vo.VONodes) - 1; i >= 0; i-- {
		var concatenatedHash []byte // 用于拼接哈希值

		if i == len(vo.VONodes)-1 { // 在叶子层
			for j := 0; j < len(vo.VONodes[i].Hashes); j++ {
				if int64(j) == vo.VONodes[i].Index { //需要计算哈希值
					var kvTx []byte
					kvTx = append(kvTx, tx.Key...)
					kvTx = append(kvTx, []byte(types.Sep)...)
					kvTx = append(kvTx, tx.Value...)
					concatenatedHash = append(concatenatedHash, tmhash.Sum(kvTx)...)
				} else { //不需要计算哈希值
					concatenatedHash = append(concatenatedHash, vo.VONodes[i].Hashes[j]...)
				}
			}
		} else { //不在叶子层
			for j := 0; j < len(vo.VONodes[i].Hashes); j++ {
				if vo.VONodes[i].Index == int64(j) { // 需要计算哈希值
					concatenatedHash = append(concatenatedHash, hash...)
				} else {
					concatenatedHash = append(concatenatedHash, vo.VONodes[i].Hashes[j]...)
				}
			}
		}
		hash = tmhash.Sum(concatenatedHash) // 将拼接的哈希值最后再做哈希运算
		if i == 0 {                         // 已经计算到根结点，结束
			break
		}
	}
	if bytes.Compare(mbTreeRootHash, hash) == 0 { //将自底向上计算得到的哈希值和MBTree的根哈希比较
		return nil
	}

	return fmt.Errorf("verification error")
}
