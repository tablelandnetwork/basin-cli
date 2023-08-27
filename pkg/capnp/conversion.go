package capnp

import (
	"fmt"

	"capnproto.org/go/capnp/v3"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
)

// FromPgReplTx converts Tx to its capnp verson.
func FromPgReplTx(tx *pgrepl.Tx) (Tx, error) {
	// TODO: better error handling

	_, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return Tx{}, fmt.Errorf("capnp new message: %s", err)
	}

	capnpTx, err := NewRootTx(seg)
	if err != nil {
		return Tx{}, fmt.Errorf("capnp new tx: %s", err)
	}

	capnpTx.SetCommitLSN(uint64(tx.CommitLSN))
	recordsList, err := NewTx_Record_List(seg, int32(len(tx.Records)))
	if err != nil {
		return Tx{}, fmt.Errorf("capnp new columns list: %s", err)
	}
	for i, record := range tx.Records {
		r := recordsList.At(i)
		if err := r.SetAction(record.Action); err != nil {
			return Tx{}, fmt.Errorf("capnp new record: %s", err)
		}
		_ = r.SetTimestamp(record.Timestamp)
		_ = r.SetSchema(record.Schema)
		_ = r.SetTable(record.Table)

		columnsList, err := NewTx_Record_Column_List(seg, int32(len(record.Columns)))
		if err != nil {
			return Tx{}, fmt.Errorf("capnp new columns list: %s", err)
		}

		for index, column := range record.Columns {
			col := columnsList.At(index)
			_ = col.SetName(column.Name)
			_ = col.SetType(column.Type)
			_ = col.SetValue(column.Value)

			_ = columnsList.Set(index, col)
		}
		_ = r.SetColumns(columnsList)

		pkList, err := NewTx_Record_PrimaryKey_List(seg, int32(len(record.PrimaryKey)))
		if err != nil {
			return Tx{}, fmt.Errorf("capnp new pk list: %s", err)
		}
		for index, primarKey := range record.PrimaryKey {
			pk := pkList.At(index)
			_ = pk.SetName(primarKey.Name)
			_ = pk.SetType(primarKey.Type)

			_ = pkList.Set(index, pk)
		}
		_ = r.SetPrimaryKey(pkList)
		_ = recordsList.Set(i, r)
	}

	_ = capnpTx.SetRecords(recordsList)

	return capnpTx, nil
}
