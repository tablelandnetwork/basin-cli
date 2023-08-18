package basinprovider

import (
	"context"
	"testing"

	capnp "capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/stretchr/testify/require"
	basincapnp "github.com/tablelandnetwork/basin-cli/pkg/capnp"
	"google.golang.org/grpc/test/bufconn"
)

func TestBasinProvider_Push(t *testing.T) {
	// in this test we create a tx with the value 33,
	// sent to the server, the server deserialize it and send the value back

	bp := newServer()
	txData := newTx(t, 333)

	response, err := bp.Push(context.Background(), txData, []byte{})
	require.NoError(t, err)
	require.Equal(t, uint64(333), response)
}

func newTx(t *testing.T, v uint64) []byte {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	require.NoError(t, err)

	capnpTx, err := basincapnp.NewRootTx(seg)
	require.NoError(t, err)

	// right now i'm just setting this field for testing
	capnpTx.SetCommitLSN(v)

	// txData, err := capnp.Canonicalize(capnpTx.ToPtr().Struct())
	// require.NoError(t, err)

	txData, err := msg.Marshal()
	require.NoError(t, err)

	return txData
}

func newServer() *BasinProvider {
	buffer := 101024 * 1024
	lis := bufconn.Listen(buffer)

	srv := BasinProviderClient_ServerToClient(&BasinServerMock{})
	bootstrapClient := capnp.Client(srv)

	go func() {
		_ = rpc.Serve(lis, bootstrapClient)
	}()

	return New(srv)
}
