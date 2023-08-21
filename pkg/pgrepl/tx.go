package pgrepl

import (
	"github.com/jackc/pglogrepl"
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
