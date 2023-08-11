package pgrepl

import "github.com/jackc/pglogrepl"

// Wal is the record replicated from Postgres.
type Wal struct {
	Timeline int32
	Pos      pglogrepl.LSN
	Payload  JSONWal
}

// JSONWal is the WAL record information encoded in JSON.
type JSONWal struct {
	Action    string          `json:"action"`
	Timestamp string          `json:"timestamp"`
	Schema    string          `json:"schema"`
	Table     string          `json:"table"`
	Columns   []JSONWalColumn `json:"columns"`
}

// JSONWalColumn offers column information encoded in JSON.
type JSONWalColumn struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value any    `json:"value"`
}
