package basinprovider

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"time"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/cenkalti/backoff/v4"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/tablelandnetwork/basin-cli/internal/app"
	basincapnp "github.com/tablelandnetwork/basin-cli/pkg/capnp"
	"golang.org/x/crypto/sha3"
)

// maxPushAttempts is the max number of attempts used to dial the provider.
const maxPushAttempts = 10

// uploadChunk is the size of the chunk when uploading a file.
const uploadChunk = 1 << 20 // 2 ^ 20 bytes = 1MiB

// BasinProvider implements the app.BasinProvider interface.
type BasinProvider struct {
	p        Publications
	provider string
	ctx      context.Context
	cancel   context.CancelFunc
}

var _ app.BasinProvider = (*BasinProvider)(nil)

// New creates a new BasinProvider.
func New(ctx context.Context, provider string) (*BasinProvider, error) {
	client, cancel, err := connectWithBackoff(ctx, provider)
	if err != nil {
		return nil, err
	}

	return &BasinProvider{
		p:        client,
		provider: provider,
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

func connect(ctx context.Context, provider string) (Publications, func(), error) {
	conn, err := net.Dial("tcp", provider)
	if err != nil {
		return Publications{}, func() {}, fmt.Errorf("failed to connect to provider: %s", err)
	}

	rpcConn := rpc.NewConn(rpc.NewStreamTransport(conn), nil)
	cancel := func() {
		if err := rpcConn.Close(); err != nil {
			slog.Error(err.Error())
		}
	}

	return Publications(rpcConn.Bootstrap(ctx)), cancel, nil
}

func connectWithBackoff(ctx context.Context, provider string) (client Publications, cancel func(), err error) {
	exbo := backoff.NewExponentialBackOff()

	for i := 0; i < maxPushAttempts; i++ {
		client, cancel, err = connect(ctx, provider)
		if err != nil {
			wait := exbo.NextBackOff()
			slog.Info("connect failed", "attempt", i+1, "sleep", wait)
			time.Sleep(wait)
			continue
		}
		return
	}

	return
}

// Create creates a publication on Basin Provider.
func (bp *BasinProvider) Create(
	ctx context.Context, ns string, rel string, schema basincapnp.Schema, owner common.Address,
) (bool, error) {
	f, release := bp.p.Create(ctx, func(bp Publications_create_Params) error {
		if err := bp.SetNs(ns); err != nil {
			return fmt.Errorf("setting ns: %s", err)
		}
		if err := bp.SetRel(rel); err != nil {
			return fmt.Errorf("setting rel: %s", err)
		}
		if err := bp.SetSchema(schema); err != nil {
			return fmt.Errorf("setting schema: %s", err)
		}
		if err := bp.SetOwner(owner.Bytes()); err != nil {
			return fmt.Errorf("setting owner: %s", err)
		}
		return nil
	})
	defer release()

	results, err := f.Struct()
	if err != nil {
		return false, fmt.Errorf("waiting for create: %s", err)
	}

	return results.Exists(), nil
}

// Push pushes Postgres tx to the server.
func (bp *BasinProvider) Push(ctx context.Context, ns string, rel string, tx basincapnp.Tx, sig []byte) error {
	f, release := bp.p.Push(ctx, func(bp Publications_push_Params) error {
		if err := bp.SetNs(ns); err != nil {
			return fmt.Errorf("setting ns: %s", err)
		}
		if strings.Contains(rel, ".") {
			parts := strings.Split(rel, ".") // remove the schema from table's name (e.g. public)
			rel = parts[1]
		}
		if err := bp.SetRel(rel); err != nil {
			return fmt.Errorf("setting rel: %s", err)
		}
		if err := bp.SetTx(tx); err != nil {
			return fmt.Errorf("setting rel: %s", err)
		}
		if err := bp.SetSig(sig); err != nil {
			return fmt.Errorf("setting sig: %s", err)
		}
		return nil
	})
	defer release()

	_, err := f.Struct()
	return err
}

// Upload uploads a file to th server.
func (bp *BasinProvider) Upload(
	ctx context.Context, ns string, rel string, r io.Reader, signer *app.Signer, progress io.Writer,
) error {
	uploadFuture, uploadRelease := bp.p.Upload(ctx, func(p Publications_upload_Params) error {
		if err := p.SetNs(ns); err != nil {
			return fmt.Errorf("setting sig: %s", err)
		}

		if err := p.SetRel(rel); err != nil {
			return fmt.Errorf("setting sig: %s", err)
		}

		return nil
	})
	defer uploadRelease()

	callback := uploadFuture.Callback()

	buf := make([]byte, uploadChunk)
	for {
		n, err := r.Read(buf[:cap(buf)])
		buf = buf[:n]
		if err != nil {
			if err != io.EOF {
				return fmt.Errorf("read file: %s", err)
			}
			break
		}
		signer.Sum(buf)

		f, release := callback.Write(ctx, func(p Publications_Callback_write_Params) error {
			return p.SetChunk(buf)
		})
		defer release()
		if _, err := f.Struct(); err != nil {
			return fmt.Errorf("waiting for write: %s", err)
		}

		_, _ = progress.Write(buf)
	}

	doneFuture, doneRelease := callback.Done(ctx, func(p Publications_Callback_done_Params) error {
		signature, err := signer.Sign()
		if err != nil {
			return fmt.Errorf("sign: %s", err)
		}

		if err := p.SetSig(signature); err != nil {
			return fmt.Errorf("setting sig: %s", err)
		}

		return nil
	})
	defer doneRelease()
	if _, err := doneFuture.Struct(); err != nil {
		return fmt.Errorf("done: %s", err)
	}

	if err := bp.p.WaitStreaming(); err != nil {
		return fmt.Errorf("wait streaming: %s", err)
	}

	return nil
}

// Reconnect with the Basin Provider.
func (bp *BasinProvider) Reconnect() error {
	bp.cancel()
	client, cancel, err := connectWithBackoff(bp.ctx, bp.provider)
	if err != nil {
		return err
	}
	bp.p = client
	bp.cancel = cancel
	return nil
}

// Close the connection with the Basin Provider.
func (bp *BasinProvider) Close() {
	bp.cancel()
}

// BasinServerMock is a mocked version of a server implementation using for testing.
type BasinServerMock struct {
	publications map[string]struct{}
	owner        map[string]string
	txs          map[basincapnp.Tx]bool

	uploads map[string]*callbackMock
}

// NewBasinServerMock creates new *BasinServerMock.
func NewBasinServerMock() *BasinServerMock {
	return &BasinServerMock{
		publications: make(map[string]struct{}),
		owner:        make(map[string]string),
		txs:          make(map[basincapnp.Tx]bool),
		uploads:      make(map[string]*callbackMock),
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

	slog.Info("Publication created", "namespace", ns, "relation", rel, "owner", common.BytesToAddress(owner).Hex())

	columns, _ := schema.Columns()
	for i := 0; i < columns.Len(); i++ {
		name, _ := columns.At(i).Name()
		typ, _ := columns.At(i).Type()
		isNull := columns.At(i).IsNullable()
		isPk := columns.At(i).IsPartOfPrimaryKey()
		slog.Info("Column schema", "name", name, "typ", typ, "is_null", isNull, "is_pk", isPk)
	}

	results, _ := call.AllocResults()
	_, ok := s.publications[fmt.Sprintf("%s.%s", ns, rel)]
	fmt.Println(s.publications, ok)
	if ok {
		results.SetExists(true)
	} else {
		s.publications[fmt.Sprintf("%s.%s", ns, rel)] = struct{}{}
		results.SetExists(false)
	}
	fmt.Println(s.publications, ok)

	return nil
}

// Upload handles the Write request.
func (s *BasinServerMock) Upload(_ context.Context, call Publications_upload) error {
	ns, err := call.Args().Ns()
	if err != nil {
		return err
	}

	rel, err := call.Args().Rel()
	if err != nil {
		return err
	}

	results, err := call.AllocResults()
	if err != nil {
		return err
	}

	callback := &callbackMock{
		ns:  ns,
		rel: rel,
	}
	err = results.SetCallback(Publications_Callback(capnp.NewClient(Publications_Callback_NewServer(callback))))
	if err != nil {
		return err
	}

	s.uploads[fmt.Sprintf("%s.%s", ns, rel)] = callback
	return nil
}

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

type callbackMock struct {
	ns    string
	rel   string
	bytes []byte
	sig   []byte
}

func (c *callbackMock) Write(_ context.Context, p Publications_Callback_write) error {
	chunk, err := p.Args().Chunk()
	if err != nil {
		return nil
	}

	c.bytes = append(c.bytes, chunk...)

	return nil
}

func (c *callbackMock) Done(_ context.Context, p Publications_Callback_done) error {
	signature, err := p.Args().Sig()
	if err != nil {
		return nil
	}

	c.sig = signature
	return nil
}
