package pgrepl

import (
	"fmt"

	"capnproto.org/go/capnp/v3"
	"github.com/jackc/pglogrepl"
	basincapnp "github.com/tablelandnetwork/basin-cli/pkg/capnp"
)

// Tx is an slice of records.
type Tx struct {
	CommitLSN pglogrepl.LSN `json:"commit_lsn"`
	Records   []Record      `json:"records"`
}

// Record is the WAL record information encoded in JSON.
type Record struct {
	Action     string       `json:"action"`
	XID        int64        `json:"xid"`
	Lsn        string       `json:"lsn"`
	EndLsn     string       `json:"nextlsn"`
	Timestamp  string       `json:"timestamp"`
	Schema     string       `json:"schema"`
	Table      string       `json:"table"`
	Columns    []Column     `json:"columns"`
	PrimaryKey []PrimaryKey `json:"pk"`
}

// Column contains column information.
type Column struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value any    `json:"value"`
}

// PrimaryKey contains primary key information.
type PrimaryKey struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// ToCapNProto encodes Tx in a capnp.Tx.
func (tx *Tx) ToCapNProto() (basincapnp.Tx, *capnp.Message, error) {
	// TODO: better error handling

	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return basincapnp.Tx{}, nil, fmt.Errorf("capnp new message: %s", err)
	}

	capnpTx, err := basincapnp.NewRootTx(seg)
	if err != nil {
		return basincapnp.Tx{}, nil, fmt.Errorf("capnp new tx: %s", err)
	}

	capnpTx.SetCommitLSN(uint64(tx.CommitLSN))
	recordsList, err := basincapnp.NewTx_Record_List(seg, int32(len(tx.Records)))
	if err != nil {
		return basincapnp.Tx{}, nil, fmt.Errorf("capnp new columns list: %s", err)
	}
	for i, record := range tx.Records {
		r, err := basincapnp.NewTx_Record(seg)
		if err != nil {
			return basincapnp.Tx{}, nil, fmt.Errorf("capnp new record: %s", err)
		}

		if err := r.SetAction(record.Action); err != nil {
			return basincapnp.Tx{}, nil, fmt.Errorf("capnp new record: %s", err)
		}
		_ = r.SetTimestamp(record.Timestamp)
		_ = r.SetSchema(record.Schema)
		_ = r.SetTable(record.Table)

		columnsList, err := basincapnp.NewTx_Record_Column_List(seg, int32(len(record.Columns)))
		if err != nil {
			return basincapnp.Tx{}, nil, fmt.Errorf("capnp new columns list: %s", err)
		}
		for index, column := range record.Columns {
			col, err := basincapnp.NewTx_Record_Column(seg)
			if err != nil {
				return basincapnp.Tx{}, nil, fmt.Errorf("capnp new col: %s", err)
			}
			_ = col.SetName(column.Name)
			_ = col.SetType(column.Type)
			_ = col.SetValue(col.Segment().Data())

			_ = columnsList.Set(index, col)
		}
		_ = r.SetColums(columnsList)

		pkList, err := basincapnp.NewTx_Record_PrimaryKey_List(seg, int32(len(record.PrimaryKey)))
		if err != nil {
			return basincapnp.Tx{}, nil, fmt.Errorf("capnp new pk list: %s", err)
		}
		for index, primarKey := range record.PrimaryKey {
			pk, err := basincapnp.NewTx_Record_PrimaryKey(seg)
			if err != nil {
				return basincapnp.Tx{}, nil, fmt.Errorf("capnp new pk: %s", err)
			}
			_ = pk.SetName(primarKey.Name)
			_ = pk.SetType(primarKey.Type)

			_ = pkList.Set(index, pk)
		}
		_ = r.SetPrimaryKey(pkList)
		_ = recordsList.Set(i, r)
	}

	return capnpTx, msg, nil
}
