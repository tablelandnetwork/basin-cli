package basinprovider

import (
	"context"
	"fmt"

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
type BasinServerMock struct{}

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

	fmt.Println(tx.CommitLSN())

	res.SetResponse(tx.CommitLSN())
	return nil
}

func (s *BasinServerMock) mustEmbedUnimplementedBasinProviderServer() {} // nolint
