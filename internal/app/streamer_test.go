package app

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"

	basincapnp "github.com/tablelandnetwork/basin-cli/pkg/capnp"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
)

const (
	pk        = "f81ab2709b7cf1f2ebbbd50bd730b267879a495318f7aac16bbe7caa8a8f2d8d"
	testTable = "t"
	testNS    = "public"
)

var cols = []Column{
	{Name: "id", Typ: "integer", IsNull: false, IsPrimary: true},
	{Name: "name", Typ: "text", IsNull: false, IsPrimary: false},
}

// Test when window threshold is crossed before
// second Tx is received: <T1, W, T2, C>.
func TestBasinStreamerOne(t *testing.T) {
	// used for testing
	privateKey, err := crypto.HexToECDSA(pk)
	require.NoError(t, err)

	// This chan will receive the wal records from the replicator
	feed := make(chan *pgrepl.Tx)
	testDBDir := t.TempDir()
	winSize := 3 * time.Second
	providerMock := &basinProviderMock{
		owner:          make(map[string]string),
		uploaderInputs: make(chan *os.File),
	}
	uploader := NewBasinUploader(testNS, testTable, providerMock, privateKey)
	dbm := NewDBManager(
		testDBDir, testTable, cols, winSize, uploader)

	streamer := NewBasinStreamer(testNS, &replicatorMock{feed: feed}, dbm)
	go func() {
		// start listening to WAL records in a separate goroutine
		err = streamer.Run(context.Background())
		require.NoError(t, err)
	}()

	// receive first tx
	recvWAL(t, WAL1, feed)

	// sleep for winSize time and receive next message
	// to trigger db replacement
	time.Sleep(winSize + 1)

	// receive second tx
	recvWAL(t, WAL2, feed)

	// Assert that ONLY the first tx was replayed
	// by importing the exported parquet file
	file := <-providerMock.uploaderInputs
	rows := importLocalDB(t, file)
	result := queryResult(t, rows)
	require.Equal(t, 1, len(result))
	require.Equal(t, 200232, result[0].id)
	require.Equal(t, "100", result[0].name)

	// simulate starting the replication process again.
	// it will upload all the parquet files in the db dir
	go func() {
		require.NoError(t, dbm.UploadAll(context.Background()))
	}()

	// Assert that the second tx was replayed and uploaded.
	file = <-providerMock.uploaderInputs
	rows = importLocalDB(t, file)
	result = queryResult(t, rows)
	require.Equal(t, 1, len(result))
	require.Equal(t, 200233, result[0].id)
	require.Equal(t, "200", result[0].name)
}

// Test when window threshold is crossed after
// second Tx is received: <T1, T2, W, C>.
func TestBasinStreamerTwo(t *testing.T) {
	privateKey, err := crypto.HexToECDSA(pk)
	require.NoError(t, err)

	// This chan will receive the wal records from the replicator
	feed := make(chan *pgrepl.Tx)
	testDBDir := t.TempDir()
	winSize := 3 * time.Second
	providerMock := &basinProviderMock{
		owner:          make(map[string]string),
		uploaderInputs: make(chan *os.File),
	}
	uploader := NewBasinUploader(testNS, testTable, providerMock, privateKey)
	dbm := NewDBManager(
		testDBDir, testTable, cols, winSize, uploader)
	streamer := NewBasinStreamer(testNS, &replicatorMock{feed: feed}, dbm)
	go func() {
		// start listening to WAL records in a separate goroutine
		err = streamer.Run(context.Background())
		require.NoError(t, err)
	}()

	// 1. receive first tx
	recvWAL(t, WAL1, feed)

	// 2. receive second tx
	recvWAL(t, WAL2, feed)

	// wait for window to pass
	time.Sleep(winSize + 1)

	// nothing should be uploaded because the second tx was received before
	// the window closed. the exports should be uploaded
	// when we replicator is started again
	select {
	case <-providerMock.uploaderInputs:
		t.FailNow() // should not be reached
	default:
		// manually trigger upload to simulate
		// starting the replication process again
		go func() {
			require.NoError(
				t, dbm.UploadAll(context.Background()))
		}()
	}

	// Assert that the both first and second tx
	// were replayed by importing the exported parquet file
	file := <-providerMock.uploaderInputs
	rows := importLocalDB(t, file)
	result := queryResult(t, rows)
	require.Equal(t, 2, len(result))
	require.Equal(t, 200232, result[0].id)
	require.Equal(t, "100", result[0].name)
	require.Equal(t, 200233, result[1].id)
	require.Equal(t, "200", result[1].name)
}

type replicatorMock struct {
	feed chan *pgrepl.Tx
}

var _ Replicator = (*replicatorMock)(nil)

func (rm *replicatorMock) StartReplication(_ context.Context) (chan *pgrepl.Tx, string, error) {
	return rm.feed, "", nil
}

func (rm *replicatorMock) Commit(_ context.Context, _ pglogrepl.LSN) error {
	return nil
}

func (rm *replicatorMock) Shutdown() {
	close(rm.feed)
}

type basinProviderMock struct {
	owner          map[string]string
	uploaderInputs chan *os.File
}

func (bp *basinProviderMock) Create(
	_ context.Context, ns string, _ string, _ basincapnp.Schema, owner common.Address,
) (bool, error) {
	bp.owner[ns] = owner.Hex()
	return false, nil
}

func (bp *basinProviderMock) List(_ context.Context, _ common.Address) ([]string, error) {
	return []string{}, nil
}

func (bp *basinProviderMock) Deals(context.Context, string, string, uint32, uint64) ([]DealInfo, error) {
	return []DealInfo{}, nil
}

func (bp *basinProviderMock) LatestDeals(context.Context, string, string, uint32) ([]DealInfo, error) {
	return []DealInfo{}, nil
}

func (bp *basinProviderMock) Reconnect() error {
	return nil
}

func (bp *basinProviderMock) Upload(
	_ context.Context, _ string, _ string, _ uint64, f io.Reader, _ *Signer, _ io.Writer,
) error {
	file := f.(*os.File)
	file.Fd()

	// re-create a copy of the file for assertions
	// because the original file will be deleted by the uploader
	newFile, err := os.Create(file.Name() + ".copy")
	if err != nil {
		return err
	}
	_, err = io.Copy(newFile, file)
	if err != nil {
		return err
	}

	err = newFile.Sync() // flush to disk
	if err != nil {
		return err
	}

	bp.uploaderInputs <- newFile
	return nil
}
