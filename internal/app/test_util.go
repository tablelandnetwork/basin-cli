package app

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
)

// recvWAL reads one line from the reader and unmarshals it into a transaction.
func recvWAL(t *testing.T, jsonIn []byte, feed chan *pgrepl.Tx) {
	var tx pgrepl.Tx
	require.NoError(t, json.Unmarshal(jsonIn, &tx))
	feed <- &tx
}

type testRow struct {
	id   int
	name string
}

func importLocalDB(t *testing.T, file *os.File) *sql.Rows {
	db, err := sql.Open("duckdb", path.Join(t.TempDir(), "temp.db"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("INSTALL parquet; LOAD parquet;")
	if err != nil {
		t.Fatal(err)
	}

	parquetQuery := fmt.Sprintf(
		"SELECT * FROM read_parquet('%s')", file.Name())
	rows, err := db.Query(parquetQuery)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func queryResult(t *testing.T, rows *sql.Rows) (result []testRow) {
	var id int
	var name string
	for rows.Next() {
		require.NoError(t, rows.Scan(&id, &name))
		row := testRow{
			id:   id,
			name: name,
		}
		result = append(result, row)
	}

	return result
}
