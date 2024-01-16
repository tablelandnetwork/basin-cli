package app

import (
	"context"
	"io"

	"github.com/ipfs/go-cid"
)

// VaultsProvider defines Vaults API.
type VaultsProvider interface {
	CreateVault(context.Context, CreateVaultParams) error
	ListVaults(context.Context, ListVaultsParams) ([]Vault, error)
	ListVaultEvents(context.Context, ListVaultEventsParams) ([]EventInfo, error)
	WriteVaultEvent(context.Context, WriteVaultEventParams) error
	RetrieveEvent(context.Context, RetrieveEventParams, io.Writer) error
}

// CreateVaultParams ...
type CreateVaultParams struct {
	Vault         Vault
	Account       *Account
	CacheDuration CacheDuration
}

// ListVaultsParams ...
type ListVaultsParams struct {
	Account *Account
}

// ListVaultEventsParams ...
type ListVaultEventsParams struct {
	Vault  Vault
	Limit  uint32
	Offset uint32
	Before Timestamp
	After  Timestamp
}

// WriteVaultEventParams ...
type WriteVaultEventParams struct {
	Vault       Vault
	Signature   string
	Timestamp   Timestamp
	Content     io.Reader
	ProgressBar io.Writer
	Size        int64
}

// RetrieveEventParams ...
type RetrieveEventParams struct {
	CID cid.Cid
}