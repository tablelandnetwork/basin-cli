package ecmh

import (
	"testing"

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
			currentHash.Insert([]byte(item))
		}
		cr1 := currentHash.String()

		// check if item is in the set?
		currentHash.Remove([]byte(tc.items[0]))

		currentHash.Insert([]byte(tc.items[0]))
		cr3 := currentHash.String()
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
			currentHash1.Insert([]byte(item))
		}

		currentHash2 := NewMultisetHash()
		for _, item := range tc.items2 {
			currentHash2.Insert([]byte(item))
		}

		currentHash1.Union(currentHash2)
		cr1 := currentHash1.String()

		currentHash1.Difference(currentHash2)

		currentHash1.Union(currentHash2)
		cr3 := currentHash1.String()
		require.Equal(t, cr1, cr3)
	}
}
