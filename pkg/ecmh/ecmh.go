package ecmh

import "github.com/bwesterb/go-ristretto"

// MultisetHash is a multiset hash based on ECMH
// implementated using ristretto points.
type MultisetHash struct {
	accumulator *ristretto.Point
}

// NewMultisetHash creates a new multiset hash.
func NewMultisetHash() *MultisetHash {
	p := ristretto.Point{}
	p.SetZero()

	return &MultisetHash{
		accumulator: &p,
	}
}

// String returns the string representation of the multiset hash.
func (h *MultisetHash) String() string {
	return h.accumulator.String()
}

// Bytes returns the byte representation of the multiset hash.
func (h *MultisetHash) Bytes() []byte {
	return h.accumulator.Bytes()
}

// Insert inserts a new item (byte array) into the multiset hash.
func (h *MultisetHash) Insert(item []byte) {
	p := ristretto.Point{}
	h.accumulator.Add(h.accumulator, p.DeriveDalek(item))
}

// InsertAll inserts all items into the multiset hash.
func (h *MultisetHash) InsertAll(items [][]byte) {
	for _, item := range items {
		h.Insert(item)
	}
}

// Union unions two multisets.
func (h *MultisetHash) Union(other *MultisetHash) {
	h.accumulator.Add(h.accumulator, other.accumulator)
}

// Difference computes the diff between two multisets.
func (h *MultisetHash) Difference(other *MultisetHash) {
	h.accumulator.Sub(h.accumulator, other.accumulator)
}

// Remove removes an item (byte array) from the multiset hash.
func (h *MultisetHash) Remove(item []byte) {
	p := ristretto.Point{}
	h.accumulator.Sub(h.accumulator, p.DeriveDalek(item))
}

// RemoveAll removes all items (points) from the multiset hash.
func (h *MultisetHash) RemoveAll(items [][]byte) {
	for _, item := range items {
		h.Remove(item)
	}
}
