package basinprovider

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"testing"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/stretchr/testify/require"
	"github.com/tablelandnetwork/basin-cli/internal/app"
	basincapnp "github.com/tablelandnetwork/basin-cli/pkg/capnp"
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

// Tests if the mocked server received the uploaded content.
func TestBasinProvider_Upload(t *testing.T) {
	client, server := newClientAndServer()

	// used for testing
	pk := "f81ab2709b7cf1f2ebbbd50bd730b267879a495318f7aac16bbe7caa8a8f2d8d"
	privateKey, err := crypto.HexToECDSA(pk)
	require.NoError(t, err)

	// Upload data 1 on test.test
	filedata1 := []byte{'H', 'e', 'l', 'l', 'o'}
	{
		buf := bytes.NewReader(filedata1)
		err := client.Upload(
			context.Background(),
			"test",
			"test",
			uint64(5),
			buf,
			app.NewSigner(privateKey),
			bytes.NewBuffer([]byte{}),
			app.Timestamp{},
		)
		require.NoError(t, err)
		require.Equal(t, filedata1, server.uploads["test.test"][0].bytes)
		require.Equal(t, "801fb03a3a34fd9d3ac5445f693df74c822d2e8cfa736191e7919e099931d8a51cd0a62fc67da6d8f0aab4302c18aa0cf381c973a8817b7062805f19d03f88ce00", hex.EncodeToString(server.uploads["test.test"][0].sig)) // nolint
	}

	// Upload data 2 on test2.test2
	filedata2 := []byte{'W', 'o', 'r', 'l', 'd'}
	{
		buf := bytes.NewReader(filedata2)
		err := client.Upload(context.Background(), "test2", "test2", uint64(5), buf, app.NewSigner(privateKey), bytes.NewBuffer([]byte{}), app.Timestamp{}) // nolint
		require.NoError(t, err)
		require.Equal(t, filedata2, server.uploads["test2.test2"][0].bytes)
		require.Equal(t, "3ad572a3483971285f3c6dc0e71d234a58543876f98b23183dc4e60008c1a92310f42202858b48ad917588535c2234c85413e124a2dcdd0759df9c555a9f585901", hex.EncodeToString(server.uploads["test2.test2"][0].sig)) // nolint
	}

	// Upload data 3 on test.test
	filedata3 := []byte{'W', 'O', 'R', 'L', 'D'}
	{
		buf := bytes.NewReader(filedata3)
		err := client.Upload(context.Background(), "test", "test", uint64(5), buf, app.NewSigner(privateKey), bytes.NewBuffer([]byte{}), app.Timestamp{}) // nolint
		require.NoError(t, err)
		require.Equal(t, filedata3, server.uploads["test.test"][1].bytes)
		require.Equal(t, "94dcba2012dd83edf1e379bbdc640e95321ea30d5318e1f5dfd46154603bba4970729b44a71844e3f4e07955dfb529ccf60d31b74a9971649d64fd8c12a32a7d00", hex.EncodeToString(server.uploads["test.test"][1].sig)) // nolint
	}

	// check latest 2 deals for test2.test2, should return filedata2
	{
		dealInfo, err := client.LatestDeals(context.Background(), "test2", "test2", 2, app.Timestamp{}, app.Timestamp{})
		require.NoError(t, err)
		require.Equal(t, 1, len(dealInfo))
		hash := sha1.Sum(filedata2)
		require.Equal(t, "70c07ec18ef89c5309bbb0937f3a6342411e1fdd", hex.EncodeToString(hash[:]))
	}

	// check deals for test.test, limit 1, offset 1, should return filedata3
	{
		dealInfo, err := client.Deals(context.Background(), "test", "test", 1, 1, app.Timestamp{}, app.Timestamp{})
		require.NoError(t, err)
		require.Equal(t, 1, len(dealInfo))
		hash := sha1.Sum(filedata3)
		require.Equal(t, hex.EncodeToString(hash[:]), "1a5db926797b9ae16ad56ec2c143e51a5172a862")
	}
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
