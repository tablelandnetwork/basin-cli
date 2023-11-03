package ecmh

import (
	"fmt"
	"testing"

	"github.com/bwesterb/go-ristretto"
)

func TestECMH(t *testing.T) {
	currentHash := NewRistrettoMultisetHash()
	items := []string{
		"apple",
		"banana",
		"cherry",
		"date",
		"elderberry",
		"fig",
		"grape",
		"honeydew",
		"imbe",
		"jackfruit",
		"kiwi",
		"lemon",
		"mango",
		"nectarine",
		"orange",
		"papaya",
		"quince",
		"raspberry",
		"strawberry",
		"tangerine",
		"ugli",
		"vanilla",
		"watermelon",
		"xylophone",
		"yuzu",
		"zucchini",
	}

	for _, item := range items {
		newItem := ristretto.Point{}
		currentHash.Insert(newItem.DeriveDalek([]byte(item)))
	}

	fmt.Println("finale currentHash", currentHash.accumulator)

	// check if item is in the set?
	newItem := ristretto.Point{}
	currentHash.Remove(newItem.DeriveDalek([]byte("apple")))
	fmt.Println("currentHash without apple", currentHash.accumulator)
	currentHash.Insert(newItem.DeriveDalek([]byte("apple")))
	fmt.Println("currentHash with apple", currentHash.accumulator)
}
