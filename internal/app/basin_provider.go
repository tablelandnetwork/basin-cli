package app

import (
	"context"

	"github.com/ethereum/go-ethereum/common"

	basincapnp "github.com/tablelandnetwork/basin-cli/pkg/capnp"
)

// DealInfo represents information about a deal.
type DealInfo struct {
	CID        string
	Created    string
	Size       uint32
	IsArchived bool
}

// BasinProvider ...
type BasinProvider interface {
	Create(context.Context, string, string, basincapnp.Schema, common.Address) (bool, error)
	List(context.Context, common.Address) ([]string, error)
	Deals(context.Context, string, string, uint32, uint64) ([]DealInfo, error)
	LatestDeals(context.Context, string, string, uint32) ([]DealInfo, error)
	Reconnect() error
}
