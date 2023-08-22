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
						Value: []byte{},
					},
				},
			},
		},
	}

	capnptx, _, err := FromPgReplTx(tx)
	require.NoError(t, err)

	requireEqualsTx(t, tx, capnptx)
}

func requireEqualsTx(t *testing.T, tx *pgrepl.Tx, capnptx Tx) {
	// commit LSN
	require.Equal(t, uint64(tx.CommitLSN), capnptx.CommitLSN())

	records, err := capnptx.Records()
	require.NoError(t, err)

	// action
	action, err := records.At(0).Action()
	require.NoError(t, err)
	require.Equal(t, tx.Records[0].Action, action)

	// timestamp
	timestamp, err := records.At(0).Timestamp()
	require.NoError(t, err)
	require.Equal(t, tx.Records[0].Timestamp, timestamp)

	// schema
	schema, err := records.At(0).Schema()
	require.NoError(t, err)
	require.Equal(t, tx.Records[0].Schema, schema)

	// table
	table, err := records.At(0).Table()
	require.NoError(t, err)
	require.Equal(t, tx.Records[0].Table, table)

	// columns
	columns, err := records.At(0).Colums()
	require.NoError(t, err)

	colName, err := columns.At(0).Name()
	require.NoError(t, err)
	require.Equal(t, tx.Records[0].Columns[0].Name, colName)

	colType, err := columns.At(0).Type()
	require.NoError(t, err)
	require.Equal(t, tx.Records[0].Columns[0].Type, colType)
}
