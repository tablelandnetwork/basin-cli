package app

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
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

// In this test we simulate a feed of wal records that are coming from wal.input file.
func TestBasinStreamer(t *testing.T) {
	// TODO: add more samples to wal.input
	// TODO: add signature ground truth to testdata

	// used for testing
	pk := "f81ab2709b7cf1f2ebbbd50bd730b267879a495318f7aac16bbe7caa8a8f2d8d"
	privateKey, err := crypto.HexToECDSA(pk)
	require.NoError(t, err)

	// we use one channel for injecting transactions to the replicator mock
	// and another channel to get feedback from the basin provider mock push call.
	feed := make(chan *pgrepl.Tx)
	feedback := make(chan struct {
		tx  basincapnp.Tx
		sig []byte
	})

	testDBDir := t.TempDir()
	testTable := "t"
	dbm, err := NewDBManager(context.Background(), testDBDir, testTable, []Column{
		{Name: "id", Typ: "integer", IsNull: false, IsPrimary: true},
		{Name: "name", Typ: "text", IsNull: false, IsPrimary: false},
	}, 1*time.Second)
	dbm.uploadMngr = &MockUploadManager{}

	// TODO: add mock db for testing
	streamer := NewBasinStreamer("n", &replicatorMock{feed: feed}, &basinProviderMock{
		feedback: feedback,
		owner:    make(map[string]string),
	}, dbm, privateKey)
	go func() {
		err = streamer.Run(context.Background())
		fmt.Println("streamer.Run err: ", err)
		require.NoError(t, err)
	}()

	// get test data
	f, err := os.Open("testdata/wal.input")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, f.Close())
	}()

	reader := bufio.NewReader(f)
	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			if err != io.EOF {
				t.Fatal(err)
			}
			break
		}

		var tx pgrepl.Tx
		err = json.Unmarshal(line, &tx)
		require.NoError(t, err)

		// inject transaction and get feedback
		feed <- &tx
		response := <-feedback

		dbm.uploadMngr.Stop()
		dbm.Close()

		// TODO: check signature, require.Equal(t, []byte{}, response.sig)
		require.NoError(t, basincapnp.CompareTx(&tx, response.tx))
	}

}

type MockUploadManager struct{}

func (m *MockUploadManager) Upload() error {
	fmt.Println("uploading...")
	return nil
}

func (m *MockUploadManager) Start() {
	fmt.Println("starting...")
}

func (m *MockUploadManager) Stop() {
	fmt.Println("stopping...")
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
	feedback chan struct {
		tx  basincapnp.Tx
		sig []byte
	}
	owner map[string]string
}

func (bp *basinProviderMock) Create(
	_ context.Context, ns string, _ string, _ basincapnp.Schema, owner common.Address,
) (bool, error) {
	bp.owner[ns] = owner.Hex()
	return false, nil
}

func (bp *basinProviderMock) Push(
	_ context.Context, _ string, _ string, tx basincapnp.Tx, signature []byte,
) error {
	bp.feedback <- struct {
		tx  basincapnp.Tx
		sig []byte
	}{tx, signature}

	return nil
}

func (bp *basinProviderMock) Reconnect() error {
	return nil
}

func (bp *basinProviderMock) Upload(
	ctx context.Context, ns string, rel string, size uint64, f io.Reader, signer *Signer, progress io.Writer,
) error {
	return nil
}
