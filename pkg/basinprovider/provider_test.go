package basinprovider

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/stretchr/testify/require"
	"github.com/tablelandnetwork/basin-cli/internal/app"
	basincapnp "github.com/tablelandnetwork/basin-cli/pkg/capnp"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
	"google.golang.org/grpc/test/bufconn"
)

func TestBasinProvider_CreateAndList(t *testing.T) {
	// in this test we create a fake tx,
	// send to the server, the server deserialize it and send the value back

	bp, _ := newClientAndServer()
	exists, err := bp.Create(context.Background(), "n", "t", basincapnp.Schema{}, common.HexToAddress(""))
	require.NoError(t, err)
	require.False(t, exists)

	exists, err = bp.Create(context.Background(), "n", "t2", basincapnp.Schema{}, common.HexToAddress(""))
	require.NoError(t, err)
	require.False(t, exists)

	pubs, err := bp.List(context.Background(), common.HexToAddress(""))
	require.NoError(t, err)
	require.Equal(t, []string{"n.t", "n.t2"}, pubs)
}

func TestBasinProvider_Push(t *testing.T) {
	// in this test we create a fake tx,
	// send to the server, the server deserialize it and send the value back

	bp, _ := newClientAndServer()
	tx := newTx(t, &pgrepl.Tx{
		CommitLSN: 333,
		Records: []pgrepl.Record{
			{
				Action: "I",
			},
		},
	})

	err := bp.Push(context.Background(), "n", "t", tx, []byte{})
	require.NoError(t, err)
}

// Tests if the mocked server received the uploaded content.
func TestBasinProvider_Upload(t *testing.T) {
	client, server := newClientAndServer()

	// used for testing
	pk := "f81ab2709b7cf1f2ebbbd50bd730b267879a495318f7aac16bbe7caa8a8f2d8d"
	privateKey, err := crypto.HexToECDSA(pk)
	require.NoError(t, err)

	// Upload data 1
	{
		filedata := []byte{'H', 'e', 'l', 'l', 'o'}

		buf := bytes.NewReader(filedata)
		err := client.Upload(
			context.Background(), "test", "test", uint64(5), buf, app.NewSigner(privateKey), bytes.NewBuffer([]byte{}),
		)
		require.NoError(t, err)
		require.Equal(t, filedata, server.uploads["test.test"].bytes)
		require.Equal(t, "801fb03a3a34fd9d3ac5445f693df74c822d2e8cfa736191e7919e099931d8a51cd0a62fc67da6d8f0aab4302c18aa0cf381c973a8817b7062805f19d03f88ce00", hex.EncodeToString(server.uploads["test.test"].sig)) // nolint
	}
	{
		// Upload data 2
		filedata := []byte{'W', 'o', 'r', 'l', 'd'}

		buf := bytes.NewReader(filedata)
		err := client.Upload(context.Background(), "test2", "test2", uint64(5), buf, app.NewSigner(privateKey), bytes.NewBuffer([]byte{})) // nolint
		require.NoError(t, err)
		require.Equal(t, filedata, server.uploads["test2.test2"].bytes)
		require.Equal(t, "3ad572a3483971285f3c6dc0e71d234a58543876f98b23183dc4e60008c1a92310f42202858b48ad917588535c2234c85413e124a2dcdd0759df9c555a9f585901", hex.EncodeToString(server.uploads["test2.test2"].sig)) // nolint
	}
}

func newTx(t *testing.T, tx *pgrepl.Tx) basincapnp.Tx {
	capnpTx, err := basincapnp.FromPgReplTx(tx)
	require.NoError(t, err)

	return capnpTx
}

// creates a client and a mocked server.
func newClientAndServer() (*BasinProvider, *BasinServerMock) {
	buffer := 1024 * 1024
	lis := bufconn.Listen(buffer)

	mock := NewBasinServerMock()
	p := Publications_ServerToClient(mock)
	bootstrapClient := capnp.Client(p)

	go func() {
		_ = rpc.Serve(lis, bootstrapClient)
	}()

	return &BasinProvider{
		p:        p,
		provider: "mock",
		ctx:      context.Background(),
		cancel: func() {
			close(make(chan struct{}))
		},
	}, mock
}
