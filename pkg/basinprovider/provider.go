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
	p Publications
}

var _ app.BasinProvider = (*BasinProvider)(nil)

// New creates a new BasinProvider.
func New(p Publications) *BasinProvider {
	return &BasinProvider{
		p: p,
	}
}

// Push pushes Postgres tx to the server.
func (bp *BasinProvider) Push(ctx context.Context, ns string, rel string, tx basincapnp.Tx, sig []byte) error {
	f, release := bp.p.Push(ctx, func(bp Publications_push_Params) error {
		_ = bp.SetTx(tx)
		_ = bp.SetSig(sig)
		_ = bp.SetNs(ns)

		if strings.Contains(rel, ".") {
			parts := strings.Split(rel, ".") // remove the schema from table's name (e.g. public)
			rel = parts[1]
		}
		_ = bp.SetRel(rel)

		return nil
	})
	defer release()

	_, err := f.Struct()
	return err
}

// Create creates a publication on Basin Provider.
func (bp *BasinProvider) Create(
	ctx context.Context, ns string, rel string, schema basincapnp.Schema, owner common.Address) error {
	_, release := bp.p.Create(ctx, func(bp Publications_create_Params) error {
		_ = bp.SetNs(ns)
		_ = bp.SetRel(rel)
		_ = bp.SetSchema(schema)
		_ = bp.SetOwner(owner.Bytes())

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
func (s *BasinServerMock) Push(_ context.Context, call Publications_push) error {
	ns, err := call.Args().Ns()
	if err != nil {
		return err
	}

	tx, err := call.Args().Tx()
	if err != nil {
		return err
	}

	sig, err := call.Args().Sig()
	if err != nil {
		return err
	}

	data, err := capnp.Canonicalize(tx.ToPtr().Struct())
	if err != nil {
		return fmt.Errorf("canonicalize: %s", err)
	}

	err = s.verifySignature(ns, data, sig)
	s.txs[tx] = err == nil

	slog.Info("Tx received", "verified", err == nil)

	return nil
}

// Create handles the Create request.
func (s *BasinServerMock) Create(_ context.Context, call Publications_create) error {
	ns, err := call.Args().Ns()
	if err != nil {
		return err
	}

	rel, err := call.Args().Rel()
	if err != nil {
		return err
	}

	schema, err := call.Args().Schema()
	if err != nil {
		return err
	}

	owner, err := call.Args().Owner()
	if err != nil {
		return err
	}

	slog.Info("Publication created", "namespace", ns, "relation", rel, "owner", owner)

	columns, _ := schema.Columns()
	for i := 0; i < columns.Len(); i++ {
		name, _ := columns.At(i).Name()
		typ, _ := columns.At(i).Type()
		isNull := columns.At(i).IsNullable()
		isPk := columns.At(i).IsPartOfPrimaryKey()
		slog.Info("Column schema", "name", name, "typ", typ, "is_null", isNull, "is_pk", isPk)
	}

	s.owner[ns] = common.BytesToAddress(owner).Hex()
	return nil
}

func (s *BasinServerMock) mustEmbedUnimplementedBasinProviderServer() {} // nolint

func (s *BasinServerMock) verifySignature(ns string, message []byte, signature []byte) error {
	hash := crypto.Keccak256Hash(message)
	sigPublicKey, err := crypto.Ecrecover(hash.Bytes(), signature)
	if err != nil {
		return fmt.Errorf("ecrecover: %s", err)
	}

	address, ok := s.owner[ns]
	if !ok {
		return errors.New("non existent namespace")
	}

	publicKeyBytes := common.HexToAddress(address).Bytes()

	h := sha3.NewLegacyKeccak256()
	h.Write(sigPublicKey[1:])
	if !bytes.Equal(publicKeyBytes, h.Sum(nil)[12:]) {
		return errors.New("failed to verify")
	}

	return nil
}
