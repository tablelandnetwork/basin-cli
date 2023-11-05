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
	// shoud we use SetZero() here? base is the generator
	// point
	p.SetBase()

	return &MultisetHash{
		accumulator: &p,
	}
}

// String returns the string representation of the multiset hash.
func (h *MultisetHash) String() string {
	return h.accumulator.String()
}

// Insert inserts a new item (point) into the multiset hash.
func (h *MultisetHash) Insert(p *ristretto.Point) {
	h.accumulator.Add(h.accumulator, p)
}

// InsertAll inserts all items (points) into the multiset hash.
func (h *MultisetHash) InsertAll(ps []*ristretto.Point) {
	for _, item := range ps {
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

// Remove removes an item (point) from the multiset hash.
func (h *MultisetHash) Remove(p *ristretto.Point) {
	h.accumulator.Sub(h.accumulator, p)
}

// RemoveAll removes all items (points) from the multiset hash.
func (h *MultisetHash) RemoveAll(ps []*ristretto.Point) {
	for _, item := range ps {
		h.Remove(item)
	}
}
