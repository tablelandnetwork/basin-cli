package ecmh

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"

	"github.com/bwesterb/go-ristretto"
	"github.com/stretchr/testify/require"

	// Register duckdb driver.
	_ "github.com/marcboeker/go-duckdb"
)

func TestECMHInsertRemove(t *testing.T) {
	testCases := []struct {
		items []string
	}{
		{
			items: []string{"apple", "banana", "cherry"},
		},
		{
			items: []string{"apple", "banana", "cherry", "apple"},
		}, // multisets
	}
	for _, tc := range testCases {
		currentHash := NewMultisetHash()
		for _, item := range tc.items {
			newItem := ristretto.Point{}
			currentHash.Insert(newItem.DeriveDalek([]byte(item)))
		}
		cr1 := currentHash.String()
		fmt.Println("cr1", cr1)

		// check if item is in the set?
		newItem := ristretto.Point{}
		currentHash.Remove(newItem.DeriveDalek([]byte(tc.items[0])))
		cr2 := currentHash.String()
		fmt.Println("cr2", cr2)

		currentHash.Insert(newItem.DeriveDalek([]byte(tc.items[0])))
		cr3 := currentHash.String()
		fmt.Println("cr3", cr3)
		require.Equal(t, cr1, cr3)
	}
}

func TestECMHUnionDiff(t *testing.T) {
	testCases := []struct {
		items1 []string
		items2 []string
	}{
		{
			items1: []string{"apple", "banana", "cherry"},
			items2: []string{"apple", "banana", "cherry"},
		},
		{
			items1: []string{"apple", "banana", "cherry"},
			items2: []string{"apple", "banana", "cherry", "apple"},
		}, // multisets
	}
	for _, tc := range testCases {
		currentHash1 := NewMultisetHash()
		for _, item := range tc.items1 {
			newItem := ristretto.Point{}
			currentHash1.Insert(newItem.DeriveDalek([]byte(item)))
		}

		currentHash2 := NewMultisetHash()
		for _, item := range tc.items2 {
			newItem := ristretto.Point{}
			currentHash2.Insert(newItem.DeriveDalek([]byte(item)))
		}

		currentHash1.Union(currentHash2)
		cr1 := currentHash1.String()

		currentHash1.Difference(currentHash2)

		currentHash1.Union(currentHash2)
		cr3 := currentHash1.String()
		fmt.Println("cr3", cr3)
		require.Equal(t, cr1, cr3)
	}
}

func createDBWindow(t *testing.T, dbDir string) string {
	dbName := fmt.Sprintf("%d.db", rand.Int())
	db, err := sql.Open("duckdb", path.Join(dbDir, dbName))
	require.NoError(t, err)

	_, err = db.Exec("create table test (id integer)")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = db.Exec("insert into test values (?)", rand.Int31())
		require.NoError(t, err)
	}
	exportPath := path.Join(dbDir, fmt.Sprintf("%s.parquet", dbName))
	exportQuery := fmt.Sprintf(
		`INSTALL parquet;
		LOAD parquet;
		COPY (SELECT * FROM test) TO '%s' (FORMAT PARQUET)`,
		exportPath)
	_, err = db.Exec(exportQuery)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	return exportPath
}

func TestECMHMatch(t *testing.T) {
	dbDir := t.TempDir()

	// Create two export files from random data
	// and insert into a hash set
	hashSet1 := NewMultisetHash()

	exportPath1 := createDBWindow(t, dbDir)
	data1, err := os.ReadFile(exportPath1)
	require.NoError(t, err)
	newItem := ristretto.Point{}
	hashSet1.Insert(newItem.DeriveDalek(data1))

	exportPath2 := createDBWindow(t, dbDir)
	data2, err := os.ReadFile(exportPath2)
	require.NoError(t, err)
	newItem = ristretto.Point{}
	hashSet1.Insert(newItem.DeriveDalek(data2))

	// Read the previously created export files
	// in reverse order and insert into a new hash set
	hashSet2 := NewMultisetHash()

	data3, err := os.ReadFile(exportPath2)
	require.NoError(t, err)
	newItem = ristretto.Point{}
	hashSet2.Insert(newItem.DeriveDalek(data3))

	data4, err := os.ReadFile(exportPath1)
	require.NoError(t, err)
	newItem = ristretto.Point{}
	hashSet2.Insert(newItem.DeriveDalek(data4))

	// Assert the two hashset are equal
	require.Equal(t, hashSet1.String(), hashSet2.String())
}

func TestECMHMissMatch(t *testing.T) {
	dbDir := t.TempDir()

	// Create two export files from random data
	// and insert into a hash set
	hashSet1 := NewMultisetHash()

	exportPath1 := createDBWindow(t, dbDir)
	data1, err := os.ReadFile(exportPath1)
	require.NoError(t, err)
	newItem := ristretto.Point{}
	hashSet1.Insert(newItem.DeriveDalek(data1))

	exportPath2 := createDBWindow(t, dbDir)
	data2, err := os.ReadFile(exportPath2)
	require.NoError(t, err)
	newItem = ristretto.Point{}
	hashSet1.Insert(newItem.DeriveDalek(data2))

	exportPath3 := createDBWindow(t, dbDir)
	data3, err := os.ReadFile(exportPath3)
	require.NoError(t, err)
	newItem = ristretto.Point{}
	hashSet1.Insert(newItem.DeriveDalek(data3))

	// Read the previously created export files
	// in reverse order and insert into a new hash set
	hashSet2 := NewMultisetHash()

	data4, err := os.ReadFile(exportPath2)
	require.NoError(t, err)
	newItem = ristretto.Point{}
	hashSet2.Insert(newItem.DeriveDalek(data4))

	data5, err := os.ReadFile(exportPath1)
	require.NoError(t, err)
	newItem = ristretto.Point{}
	hashSet2.Insert(newItem.DeriveDalek(data5))

	// Assert the two hashset are not equal because
	// we ddidn't insert the third export file data
	require.NotEqual(t, hashSet1.String(), hashSet2.String())
}
