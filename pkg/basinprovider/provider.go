package basinprovider

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/tls"
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
	secure   bool
	ctx      context.Context
	cancel   context.CancelFunc
}

var _ app.BasinProvider = (*BasinProvider)(nil)

// New creates a new BasinProvider.
func New(ctx context.Context, provider string, secure bool) (*BasinProvider, error) {
	client, cancel, err := connectWithBackoff(ctx, provider, secure)
	if err != nil {
		return nil, err
	}

	return &BasinProvider{
		p:        client,
		provider: provider,
		secure:   secure,
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

func connect(ctx context.Context, provider string, secure bool) (Publications, func(), error) {
	var conn io.ReadWriteCloser
	var err error
	if secure {
		conn, err = tls.Dial("tcp", provider, nil)
		if err != nil {
			return Publications{}, func() {}, fmt.Errorf("failed to connect to provider: %s", err)
		}
	} else {
		conn, err = net.Dial("tcp", provider)
		if err != nil {
			return Publications{}, func() {}, fmt.Errorf("failed to connect to provider: %s", err)
		}
	}

	rpcConn := rpc.NewConn(rpc.NewStreamTransport(conn), nil)
	cancel := func() {
		if err := rpcConn.Close(); err != nil {
			slog.Error(err.Error())
		}
	}

	return Publications(rpcConn.Bootstrap(ctx)), cancel, nil
}

func connectWithBackoff(
	ctx context.Context, provider string, secure bool,
) (client Publications, cancel func(), err error) {
	exbo := backoff.NewExponentialBackOff()

	for i := 0; i < maxPushAttempts; i++ {
		client, cancel, err = connect(ctx, provider, secure)
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
	ctx context.Context, ns string, rel string, schema basincapnp.Schema, owner common.Address, cacheDuration int64,
) (bool, error) {
	if cacheDuration < 0 {
		return false, errors.New("cache duration cannot be less than zero")
	}

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

		bp.SetCache_duration(cacheDuration)

		return nil
	})
	defer release()

	results, err := f.Struct()
	if err != nil {
		return false, fmt.Errorf("waiting for create: %s", err)
	}

	return results.Exists(), nil
}

// Upload uploads a file to th server.
func (bp *BasinProvider) Upload(
	ctx context.Context,
	ns string,
	rel string,
	size uint64,
	r io.Reader,
	signer *app.Signer,
	progress io.Writer,
	timestamp app.Timestamp,
) error {
	uploadFuture, uploadRelease := bp.p.Upload(ctx, func(p Publications_upload_Params) error {
		if err := p.SetNs(ns); err != nil {
			return fmt.Errorf("setting sig: %s", err)
		}

		if err := p.SetRel(rel); err != nil {
			return fmt.Errorf("setting sig: %s", err)
		}

		p.SetSize(size)
		p.SetTimestamp(timestamp.Seconds())

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
		if err := bp.write(ctx, callback, buf); err != nil {
			return fmt.Errorf("write chunk: %s", err)
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

// List lists all publications from a given address.
func (bp *BasinProvider) List(ctx context.Context, owner common.Address) ([]string, error) {
	f, release := bp.p.List(ctx, func(call Publications_list_Params) error {
		if err := call.SetOwner(owner.Bytes()); err != nil {
			return fmt.Errorf("setting owner")
		}
		return nil
	})
	defer release()

	results, err := f.Struct()
	if err != nil {
		return []string{}, fmt.Errorf("waiting list results: %s", err)
	}

	textList, err := results.Publications()
	if err != nil {
		return []string{}, fmt.Errorf("init publications: %s", err)
	}

	pubs := make([]string, textList.Len())
	for i := 0; i < textList.Len(); i++ {
		pubs[i], err = textList.At(i)
		if err != nil {
			return []string{}, fmt.Errorf("failed to get string from text list: %s", err)
		}
	}

	return pubs, nil
}

// Deals lists deals from a given publication.
func (bp *BasinProvider) Deals(
	ctx context.Context, ns string, rel string, limit uint32, offset uint64, before app.Timestamp, after app.Timestamp,
) ([]app.DealInfo, error) {
	f, release := bp.p.Deals(ctx, func(call Publications_deals_Params) error {
		if err := call.SetNs(ns); err != nil {
			return fmt.Errorf("setting ns")
		}

		if err := call.SetRel(rel); err != nil {
			return fmt.Errorf("setting rel")
		}

		call.SetLimit(limit)
		call.SetOffset(offset)
		call.SetBefore(before.Seconds())
		call.SetAfter(after.Seconds())

		return nil
	})
	defer release()

	results, err := f.Struct()
	if err != nil {
		return []app.DealInfo{}, fmt.Errorf("waiting list results: %s", err)
	}

	list, err := results.Deals()
	if err != nil {
		return []app.DealInfo{}, fmt.Errorf("result deals: %s", err)
	}

	deals := make([]app.DealInfo, list.Len())
	for i := 0; i < list.Len(); i++ {
		deal := list.At(i)
		cid, err := deal.Cid()
		if err != nil {
			return []app.DealInfo{}, fmt.Errorf("failed to get cid: %s", err)
		}

		expiresAt, err := deal.ExpiresAt()
		if err != nil {
			return []app.DealInfo{}, fmt.Errorf("failed to get expires at: %s", err)
		}

		deals[i] = app.DealInfo{
			CID:        cid,
			Timestamp:  deal.Timestamp(),
			IsArchived: deal.Archived(),
			Size:       deal.Size(),
			ExpiresAt:  expiresAt,
		}
	}

	return deals, nil
}

// LatestDeals lists latest deals from a given publication.
func (bp *BasinProvider) LatestDeals(
	ctx context.Context, ns string, rel string, n uint32, before app.Timestamp, after app.Timestamp,
) ([]app.DealInfo, error) {
	f, release := bp.p.LatestDeals(ctx, func(call Publications_latestDeals_Params) error {
		if err := call.SetNs(ns); err != nil {
			return fmt.Errorf("setting ns")
		}

		if err := call.SetRel(rel); err != nil {
			return fmt.Errorf("setting rel")
		}

		call.SetN(n)
		call.SetBefore(before.Seconds())
		call.SetAfter(after.Seconds())
		return nil
	})
	defer release()

	results, err := f.Struct()
	if err != nil {
		return []app.DealInfo{}, fmt.Errorf("waiting list results: %s", err)
	}

	list, err := results.Deals()
	if err != nil {
		return []app.DealInfo{}, fmt.Errorf("result deals: %s", err)
	}

	deals := make([]app.DealInfo, list.Len())
	for i := 0; i < list.Len(); i++ {
		deal := list.At(i)
		cid, err := deal.Cid()
		if err != nil {
			return []app.DealInfo{}, fmt.Errorf("failed to get cid: %s", err)
		}

		expiresAt, err := deal.ExpiresAt()
		if err != nil {
			return []app.DealInfo{}, fmt.Errorf("failed to get expires at: %s", err)
		}

		deals[i] = app.DealInfo{
			CID:        cid,
			Timestamp:  deal.Timestamp(),
			IsArchived: deal.Archived(),
			Size:       deal.Size(),
			ExpiresAt:  expiresAt,
		}
	}

	return deals, nil
}

// Reconnect with the Basin Provider.
func (bp *BasinProvider) Reconnect() error {
	bp.cancel()
	client, cancel, err := connectWithBackoff(bp.ctx, bp.provider, bp.secure)
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

func (bp *BasinProvider) write(ctx context.Context, callback Publications_Callback, buf []byte) error {
	f, release := callback.Write(ctx, func(p Publications_Callback_write_Params) error {
		return p.SetChunk(buf)
	})
	defer release()
	if _, err := f.Struct(); err != nil {
		return fmt.Errorf("waiting for write: %s", err)
	}

	return nil
}

// BasinServerMock is a mocked version of a server implementation using for testing.
type BasinServerMock struct {
	publications map[string]struct{}
	owner        map[string]string
	txs          map[basincapnp.Tx]bool

	uploads map[string]map[int]*callbackMock
}

// NewBasinServerMock creates new *BasinServerMock.
func NewBasinServerMock() *BasinServerMock {
	return &BasinServerMock{
		publications: make(map[string]struct{}),
		owner:        make(map[string]string),
		txs:          make(map[basincapnp.Tx]bool),
		uploads:      make(map[string]map[int]*callbackMock),
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
	if ok {
		results.SetExists(true)
	} else {
		s.publications[fmt.Sprintf("%s.%s", ns, rel)] = struct{}{}
		results.SetExists(false)
	}

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

	fmt.Println(call.Args().Size())

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

	pub := fmt.Sprintf("%s.%s", ns, rel)
	_, ok := s.uploads[pub]
	if !ok {
		s.uploads[pub] = make(map[int]*callbackMock)
	}
	s.uploads[pub][len(s.uploads[pub])] = callback

	return nil
}

// Deals lists deals from a given publication.
func (s *BasinServerMock) Deals(_ context.Context, call Publications_deals) error {
	ns, _ := call.Args().Ns()
	rel, _ := call.Args().Rel()
	limit := call.Args().Limit()
	offset := call.Args().Offset()

	results, _ := call.AllocResults()
	dealInfoList, _ := results.NewDeals(min(int32(limit), int32(len(s.uploads))))
	var i uint32
	for pub, deals := range s.uploads {
		parts := strings.Split(pub, ".")
		dealNs, dealRel := parts[0], parts[1]
		if dealNs != ns || dealRel != rel {
			continue
		}

		for id, callback := range deals {
			if uint64(id) < offset {
				continue
			}

			if i >= limit {
				continue
			}

			dealInfo := dealInfoList.At(int(i))
			fakeCID := sha1.Sum(callback.bytes)
			_ = dealInfo.SetCid(string(fakeCID[:]))
			dealInfo.SetTimestamp(0)
			dealInfo.SetArchived(false)
			dealInfo.SetSize(30)
		}

		return nil
	}

	return nil
}

// LatestDeals lists latest deals from a given publication.
func (s *BasinServerMock) LatestDeals(_ context.Context, call Publications_latestDeals) error {
	ns, _ := call.Args().Ns()
	rel, _ := call.Args().Rel()
	n := call.Args().N()

	results, _ := call.AllocResults()
	var i uint32
	for pub, deals := range s.uploads {
		parts := strings.Split(pub, ".")
		dealNs, dealRel := parts[0], parts[1]
		if dealNs != ns || dealRel != rel {
			continue
		}

		dealInfoList, _ := results.NewDeals(int32(len(s.uploads[pub])))
		for _, callback := range deals {
			if i >= n {
				continue
			}

			dealInfo := dealInfoList.At(int(i))
			fakeCID := sha1.Sum(callback.bytes)
			_ = dealInfo.SetCid(string(fakeCID[:]))
			dealInfo.SetTimestamp(0)
			dealInfo.SetArchived(false)
			dealInfo.SetSize(30)
		}

		return nil
	}
	return nil
}

// List lists all publications from a given address.
func (s *BasinServerMock) List(_ context.Context, call Publications_list) error {
	results, err := call.AllocResults()
	if err != nil {
		return err
	}

	list, err := results.NewPublications(int32(len(s.publications)))
	if err != nil {
		return err
	}
	var i int
	for pub := range s.publications {
		if err := list.Set(i, pub); err != nil {
			return err
		}
		i++
	}

	if err := results.SetPublications(list); err != nil {
		return fmt.Errorf("set publications: %s", err)
	}

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
