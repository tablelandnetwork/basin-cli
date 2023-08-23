package basinprovider

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"capnproto.org/go/capnp/v3"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/tablelandnetwork/basin-cli/internal/app"
	basincapnp "github.com/tablelandnetwork/basin-cli/pkg/capnp"
	"golang.org/x/crypto/sha3"
)

// BasinProvider implements the app.BasinProvider interface.
type BasinProvider struct {
	s BasinProviderClient
}

var _ app.BasinProvider = (*BasinProvider)(nil)

// New creates a new BasinProvider.
func New(s BasinProviderClient) *BasinProvider {
	return &BasinProvider{
		s: s,
	}
}

// Push pushes Postgres tx to the server.
func (bp *BasinProvider) Push(ctx context.Context, tx basincapnp.Tx, sig []byte) (uint64, error) {
	f, release := bp.s.Push(ctx, func(bp BasinProviderClient_push_Params) error {
		_ = bp.SetTx(tx)
		_ = bp.SetSignature(sig)

		return nil
	})
	defer release()

	res, err := f.Struct()
	if err != nil {
		return 0, err
	}

	return res.Response(), nil
}

// BasinServerMock is a mocked version of a server implementation.
type BasinServerMock struct {
	adddess string
}

// NewBasinServerMock creates new *BasinServerMock.
func NewBasinServerMock(addr string) *BasinServerMock {
	return &BasinServerMock{
		adddess: addr,
	}
}

// Push handles the Push request.
func (s *BasinServerMock) Push(_ context.Context, call BasinProviderClient_push) error {
	res, err := call.AllocResults() // allocate the results struct
	if err != nil {
		return err
	}

	tx, err := call.Args().Tx()
	if err != nil {
		return err
	}

	signature, err := call.Args().Signature()
	if err != nil {
		return err
	}

	data, err := capnp.Canonicalize(tx.ToPtr().Struct())
	if err != nil {
		return fmt.Errorf("canonicalize: %s", err)
	}

	fmt.Println("VERIFIED: ", s.verifySignature(data, signature) == nil)

	res.SetResponse(tx.CommitLSN())
	return nil
}

func (s *BasinServerMock) mustEmbedUnimplementedBasinProviderServer() {} // nolint

func (s *BasinServerMock) verifySignature(messaage []byte, signature []byte) error {
	hash := crypto.Keccak256Hash(messaage)
	sigPublicKey, err := crypto.Ecrecover(hash.Bytes(), signature)
	if err != nil {
		return fmt.Errorf("ecrecover: %s", err)
	}

	publicKeyBytes := common.HexToAddress(s.adddess).Bytes()

	h := sha3.NewLegacyKeccak256()
	h.Write(sigPublicKey[1:])
	if !bytes.Equal(publicKeyBytes, h.Sum(nil)[12:]) {
		return errors.New("failed to verify")
	}

	return nil
}
