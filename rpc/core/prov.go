package core

import (
	"fmt"
	ctypes "github.com/tendermint/tendermint/rpc/core/types"
	rpctypes "github.com/tendermint/tendermint/rpc/jsonrpc/types"
)

var num = 0

func ProvQuery(ctx *rpctypes.Context, entityId string) (*ctypes.ResultProv, error) {
	r, err := env.BlockStore.LoadProvData(entityId)
	if err != nil {
		return nil, err

	}
	num++
	fmt.Print("~~~~~~~~~~~~~~~~~~~~~~", len(r.ProvDataList), "num=", num)
	return &ctypes.ResultProv{ProvDataList: r}, nil
}
