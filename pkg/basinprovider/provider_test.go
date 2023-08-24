package basinprovider

import (
	"context"
	"testing"

	capnp "capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
	basincapnp "github.com/tablelandnetwork/basin-cli/pkg/capnp"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
	"google.golang.org/grpc/test/bufconn"
)

func TestBasinProvider_Create(t *testing.T) {
	// in this test we create a fake tx,
	// send to the server, the server deserialize it and send the value back

	bp := newServer()
	err := bp.Create(context.Background(), "t", common.HexToAddress(""), basincapnp.Schema{})
	require.NoError(t, err)
}

func TestBasinProvider_Push(t *testing.T) {
	// in this test we create a fake tx,
	// send to the server, the server deserialize it and send the value back

	bp := newServer()
	tx := newTx(t, &pgrepl.Tx{
		CommitLSN: 333,
		Records: []pgrepl.Record{
			{
				Action: "I",
			},
		},
	})

	response, err := bp.Push(context.Background(), "t", tx, []byte{})
	require.NoError(t, err)
	require.Equal(t, uint64(333), response)
}

func newTx(t *testing.T, tx *pgrepl.Tx) basincapnp.Tx {
	capnpTx, err := basincapnp.FromPgReplTx(tx)
	require.NoError(t, err)

	return capnpTx
}

func newServer() *BasinProvider {
	buffer := 101024 * 1024
	lis := bufconn.Listen(buffer)

	srv := BasinProviderClient_ServerToClient(NewBasinServerMock())
	bootstrapClient := capnp.Client(srv)

	go func() {
		_ = rpc.Serve(lis, bootstrapClient)
	}()

	return New(srv)
}
