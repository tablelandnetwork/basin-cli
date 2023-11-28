package ecmh

import (
	"fmt"
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
