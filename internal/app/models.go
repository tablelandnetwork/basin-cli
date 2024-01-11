package app

import (
	"errors"

	"github.com/ethereum/go-ethereum/common"
)

// Vault represents a vault.
type Vault string

// Account represents an account.
type Account struct {
	address common.Address
}

// NewAccount creates a new account.
func NewAccount(address string) (*Account, error) {
	if !common.IsHexAddress(address) {
		return nil, errors.New("address not valid")
	}

	return &Account{address: common.HexToAddress(address)}, nil
}

// Hex returns the hex-enconded address.
func (a *Account) Hex() string {
	return a.address.Hex()
}

// CacheDuration how long data stays in cache in minutes.
type CacheDuration uint32

// EventInfo represents information about a deal.
type EventInfo struct {
	CID         string `json:"cid"`
	Timestamp   int64  `json:"timestamp"`
	IsArchived  bool   `json:"is_archived"`
	CacheExpiry string `json:"cache_expiry"`
}
