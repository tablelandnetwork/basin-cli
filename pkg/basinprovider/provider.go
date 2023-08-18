package basinprovider

import (
	"context"

	"capnproto.org/go/capnp/v3"
	"github.com/tablelandnetwork/basin-cli/internal/app"
	basincapnp "github.com/tablelandnetwork/basin-cli/pkg/capnp"
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
func (bp *BasinProvider) Push(ctx context.Context, data app.TxData, sig app.Signature) (uint64, error) {
	f, release := bp.s.Push(ctx, func(bp BasinProviderClient_push_Params) error {
		_ = bp.SetTxData(data)
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
type BasinServerMock struct{}

// Push handles the Push request.
func (s *BasinServerMock) Push(_ context.Context, call BasinProviderClient_push) error {
	res, err := call.AllocResults() // allocate the results struct
	if err != nil {
		return err
	}

	txData, err := call.Args().TxData()
	if err != nil {
		return err
	}

	// let's decode the tx data to see if we get the correct commit LSN
	msg, err := capnp.Unmarshal(txData)
	if err != nil {
		return err
	}

	tx, err := basincapnp.ReadRootTx(msg)
	if err != nil {
		return err
	}
	res.SetResponse(tx.CommitLSN())
	return nil
}

func (s *BasinServerMock) mustEmbedUnimplementedBasinProviderServer() {} // nolint
