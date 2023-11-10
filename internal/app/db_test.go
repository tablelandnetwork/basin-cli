package app

import (
	"bufio"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
)

func TestQueryFromWAL(t *testing.T) {
	dbm := NewDBManager(
		t.TempDir(), testTable, cols, 3*time.Second, nil)

	f, err := os.Open("testdata/wal.input")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, f.Close())
	}()
	reader := bufio.NewReader(f)

	wal1, _, err := reader.ReadLine()
	require.NoError(t, err)

	var tx pgrepl.Tx
	require.NoError(t, json.Unmarshal(wal1, &tx))

	expected := "insert into t (id, name) values (200232, 100), (200242, 400)"
	require.Equal(t, expected, dbm.queryFromWAL(&tx))
}
