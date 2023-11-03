package ecmh

import (
	"github.com/bwesterb/go-ristretto"
)

type RistrettoMultisetHash struct {
	accumulator *ristretto.Point
}

func NewRistrettoMultisetHash() *RistrettoMultisetHash {
	p := ristretto.Point{}
	// shoud we use SetZero() here? base is the generator
	// point
	p.SetBase()

	return &RistrettoMultisetHash{
		accumulator: &p,
	}
}

func (h *RistrettoMultisetHash) String() string {
	return h.accumulator.String()
}

func (h *RistrettoMultisetHash) Insert(p *ristretto.Point) {
	h.accumulator.Add(h.accumulator, p)
}

func (h *RistrettoMultisetHash) InsertAll(ps []*ristretto.Point) {
	for _, item := range ps {
		h.Insert(item)
	}
}

func (h *RistrettoMultisetHash) Union(other *RistrettoMultisetHash) {
	h.accumulator.Add(h.accumulator, other.accumulator)
}

func (h *RistrettoMultisetHash) Difference(other *RistrettoMultisetHash) {
	h.accumulator.Sub(h.accumulator, other.accumulator)
}

func (h *RistrettoMultisetHash) Remove(p *ristretto.Point) {
	h.accumulator.Sub(h.accumulator, p)
}

func (h *RistrettoMultisetHash) RemoveAll(ps []*ristretto.Point) {
	for _, item := range ps {
		h.Remove(item)
	}
}
