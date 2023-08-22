package capnp

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
)

func TestConversion(t *testing.T) {
	tx := &pgrepl.Tx{
		CommitLSN: 100,
		Records: []pgrepl.Record{
			{
				Action:    "I",
				XID:       1,
				Lsn:       "1",
				EndLsn:    "1",
				Timestamp: time.Now().String(),
				Schema:    "public",
				Table:     "t",
				Columns: []pgrepl.Column{
					{
						Name:  "a",
						Type:  "integer",
						Value: []byte{0x0, 0x1},
					},
				},
			},
		},
	}

	capnptx, err := FromPgReplTx(tx)
	require.NoError(t, err)
	require.NoError(t, CompareTx(tx, capnptx))
}
