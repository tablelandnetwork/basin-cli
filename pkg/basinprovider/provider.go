package basinprovider

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

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
func (bp *BasinProvider) Push(ctx context.Context, pubName string, tx basincapnp.Tx, sig []byte) (uint64, error) {
	f, release := bp.s.Push(ctx, func(bp BasinProviderClient_push_Params) error {
		_ = bp.SetTx(tx)
		_ = bp.SetSignature(sig)

		if strings.Contains(pubName, ".") {
			parts := strings.Split(pubName, ".") // remove the schema from table's name (e.g. public)
			pubName = parts[1]
		}

		_ = bp.SetPubName(pubName)

		return nil
	})
	defer release()

	res, err := f.Struct()
	if err != nil {
		return 0, err
	}

	return res.Response(), nil
}

// Create creates a publication on Basin Provider.
func (bp *BasinProvider) Create(
	ctx context.Context, pubName string, owner common.Address, schema basincapnp.Schema,
) error {
	_, release := bp.s.Create(ctx, func(bp BasinProviderClient_create_Params) error {
		_ = bp.SetName(pubName)
		_ = bp.SetOwner(owner.Hex())
		_ = bp.SetSchema(schema)

		return nil
	})
	defer release()

	return nil
}

// BasinServerMock is a mocked version of a server implementation using for testing.
type BasinServerMock struct {
	owner map[string]string
	txs   map[basincapnp.Tx]bool
}

// NewBasinServerMock creates new *BasinServerMock.
func NewBasinServerMock() *BasinServerMock {
	return &BasinServerMock{
		owner: make(map[string]string),
		txs:   make(map[basincapnp.Tx]bool),
	}
}

// Push handles the Push request.
func (s *BasinServerMock) Push(_ context.Context, call BasinProviderClient_push) error {
	res, err := call.AllocResults() // allocate the results struct
	if err != nil {
		return err
	}

	pubName, err := call.Args().PubName()
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

	err = s.verifySignature(pubName, data, signature)
	s.txs[tx] = err == nil

	slog.Info("Tx received", "verified", err == nil)

	res.SetResponse(tx.CommitLSN())
	return nil
}

// Create handles the Create request.
func (s *BasinServerMock) Create(_ context.Context, call BasinProviderClient_create) error {
	pubName, err := call.Args().Name()
	if err != nil {
		return err
	}

	owner, err := call.Args().Owner()
	if err != nil {
		return err
	}

	schema, err := call.Args().Schema()
	if err != nil {
		return err
	}

	slog.Info("Publication created", "publication", pubName, "owner", owner)

	columns, _ := schema.Columns()
	for i := 0; i < columns.Len(); i++ {
		name, _ := columns.At(i).Name()
		typ, _ := columns.At(i).Type()
		isNull := columns.At(i).IsNullable()
		isPk := columns.At(i).IsPartOfPrimaryKey()
		slog.Info("Column schema", "name", name, "typ", typ, "is_null", isNull, "is_pk", isPk)
	}

	s.owner[pubName] = owner
	return nil
}

func (s *BasinServerMock) mustEmbedUnimplementedBasinProviderServer() {} // nolint

func (s *BasinServerMock) verifySignature(pubName string, message []byte, signature []byte) error {
	hash := crypto.Keccak256Hash(message)
	sigPublicKey, err := crypto.Ecrecover(hash.Bytes(), signature)
	if err != nil {
		return fmt.Errorf("ecrecover: %s", err)
	}

	address, ok := s.owner[pubName]
	if !ok {
		return errors.New("non existent publication")
	}

	publicKeyBytes := common.HexToAddress(address).Bytes()

	h := sha3.NewLegacyKeccak256()
	h.Write(sigPublicKey[1:])
	if !bytes.Equal(publicKeyBytes, h.Sum(nil)[12:]) {
		return errors.New("failed to verify")
	}

	return nil
}
