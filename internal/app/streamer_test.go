package app

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"testing"

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

	streamer := NewBasinStreamer("n", &replicatorMock{feed: feed}, &basinProviderMock{
		feedback: feedback,
		owner:    make(map[string]string),
	}, privateKey)
	go func() {
		err = streamer.Run(context.Background())
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

		// TODO: check signature, require.Equal(t, []byte{}, response.sig)
		require.NoError(t, basincapnp.CompareTx(&tx, response.tx))
	}
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

func (bp *basinProviderMock) List(_ context.Context, _ common.Address) ([]string, error) {
	return []string{}, nil
}

func (bp *basinProviderMock) Deals(
	context.Context, string, string, uint32, uint64, Timestamp, Timestamp,
) ([]DealInfo, error) {
	return []DealInfo{}, nil
}

func (bp *basinProviderMock) LatestDeals(
	context.Context, string, string, uint32, Timestamp, Timestamp,
) ([]DealInfo, error) {
	return []DealInfo{}, nil
}

func (bp *basinProviderMock) Reconnect() error {
	return nil
}
