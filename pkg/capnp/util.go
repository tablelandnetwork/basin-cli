package capnp

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
)

// CompareTx compares the two types of Tx.
// Used for testing.
func CompareTx(tx *pgrepl.Tx, capnptx Tx) error {
	// commit LSN
	if !cmp.Equal(uint64(tx.CommitLSN), capnptx.CommitLSN()) {
		return fmt.Errorf("commit lsn not equal")
	}

	records, err := capnptx.Records()
	if err != nil {
		return fmt.Errorf("records: %s", err)
	}

	for i := 0; i < records.Len(); i++ {
		if err := compareRecord(tx.Records[i], records.At(i)); err != nil {
			return fmt.Errorf("compare record: %s", err)
		}
	}

	return nil
}

func compareRecord(record pgrepl.Record, capnpRecord Tx_Record) error {
	// action
	action, err := capnpRecord.Action()
	if err != nil {
		return fmt.Errorf("records action: %s", err)
	}

	if !cmp.Equal(record.Action, action) {
		return fmt.Errorf("action not equals")
	}

	// timestamp
	timestamp, err := capnpRecord.Timestamp()
	if err != nil {
		return fmt.Errorf("records timestamp: %s", err)
	}

	if !cmp.Equal(record.Timestamp, timestamp) {
		return fmt.Errorf("timestamp not equals")
	}

	// schema
	schema, err := capnpRecord.Schema()
	if err != nil {
		return fmt.Errorf("records schema: %s", err)
	}
	if !cmp.Equal(record.Schema, schema) {
		return fmt.Errorf("schema not equals")
	}

	// table
	table, err := capnpRecord.Table()
	if err != nil {
		return fmt.Errorf("records table: %s", err)
	}
	if !cmp.Equal(record.Table, table) {
		return fmt.Errorf("table not equals")
	}

	// columns
	columns, err := capnpRecord.Columns()
	if err != nil {
		return fmt.Errorf("records columns: %s", err)
	}

	for i := 0; i < columns.Len(); i++ {
		if err := compareColumn(record.Columns[i], columns.At(i)); err != nil {
			return fmt.Errorf("compare column: %s", err)
		}
	}

	// pk
	pk, err := capnpRecord.PrimaryKey()
	if err != nil {
		return fmt.Errorf("records columns: %s", err)
	}

	for i := 0; i < pk.Len(); i++ {
		if err := comparePrimaKey(record.PrimaryKey[i], pk.At(i)); err != nil {
			return fmt.Errorf("compare column: %s", err)
		}
	}

	return nil
}

func compareColumn(column pgrepl.Column, capnpColumn Tx_Record_Column) error {
	colName, err := capnpColumn.Name()
	if err != nil {
		return fmt.Errorf("column name: %s", err)
	}
	if !cmp.Equal(column.Name, colName) {
		return fmt.Errorf("column name not equals: (%s, %s)", column.Name, colName)
	}

	colType, err := capnpColumn.Type()
	if err != nil {
		return fmt.Errorf("column type: %s", err)
	}
	if !cmp.Equal(column.Type, colType) {
		return fmt.Errorf("column type not equals")
	}

	value, err := capnpColumn.Value()
	if err != nil {
		return fmt.Errorf("column value: %s", err)
	}

	if !cmp.Equal([]byte(column.Value), value) {
		return fmt.Errorf("column value not equals")
	}

	return nil
}

func comparePrimaKey(primaryKey pgrepl.PrimaryKey, capnpPrimaryKey Tx_Record_PrimaryKey) error {
	colName, err := capnpPrimaryKey.Name()
	if err != nil {
		return fmt.Errorf("primary key name: %s", err)
	}
	if !cmp.Equal(primaryKey.Name, colName) {
		return fmt.Errorf("primarky name not equals")
	}

	colType, err := capnpPrimaryKey.Type()
	if err != nil {
		return fmt.Errorf("primary key type: %s", err)
	}
	if !cmp.Equal(primaryKey.Type, colType) {
		return fmt.Errorf("primary key type not equals")
	}

	return nil
}
