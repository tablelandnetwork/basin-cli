// Code generated by capnpc-go. DO NOT EDIT.

package capnp

import (
	capnp "capnproto.org/go/capnp/v3"
	text "capnproto.org/go/capnp/v3/encoding/text"
	schemas "capnproto.org/go/capnp/v3/schemas"
)

type Tx capnp.Struct

// Tx_TypeID is the unique identifier for the type Tx.
const Tx_TypeID = 0xe9135d071d75f95f

func NewTx(s *capnp.Segment) (Tx, error) {
	st, err := capnp.NewStruct(s, capnp.ObjectSize{DataSize: 8, PointerCount: 0})
	return Tx(st), err
}

func NewRootTx(s *capnp.Segment) (Tx, error) {
	st, err := capnp.NewRootStruct(s, capnp.ObjectSize{DataSize: 8, PointerCount: 0})
	return Tx(st), err
}

func ReadRootTx(msg *capnp.Message) (Tx, error) {
	root, err := msg.Root()
	return Tx(root.Struct()), err
}

func (s Tx) String() string {
	str, _ := text.Marshal(0xe9135d071d75f95f, capnp.Struct(s))
	return str
}

func (s Tx) EncodeAsPtr(seg *capnp.Segment) capnp.Ptr {
	return capnp.Struct(s).EncodeAsPtr(seg)
}

func (Tx) DecodeFromPtr(p capnp.Ptr) Tx {
	return Tx(capnp.Struct{}.DecodeFromPtr(p))
}

func (s Tx) ToPtr() capnp.Ptr {
	return capnp.Struct(s).ToPtr()
}
func (s Tx) IsValid() bool {
	return capnp.Struct(s).IsValid()
}

func (s Tx) Message() *capnp.Message {
	return capnp.Struct(s).Message()
}

func (s Tx) Segment() *capnp.Segment {
	return capnp.Struct(s).Segment()
}
func (s Tx) CommitLSN() uint64 {
	return capnp.Struct(s).Uint64(0)
}

func (s Tx) SetCommitLSN(v uint64) {
	capnp.Struct(s).SetUint64(0, v)
}

// Tx_List is a list of Tx.
type Tx_List = capnp.StructList[Tx]

// NewTx creates a new list of Tx.
func NewTx_List(s *capnp.Segment, sz int32) (Tx_List, error) {
	l, err := capnp.NewCompositeList(s, capnp.ObjectSize{DataSize: 8, PointerCount: 0}, sz)
	return capnp.StructList[Tx](l), err
}

// Tx_Future is a wrapper for a Tx promised by a client call.
type Tx_Future struct{ *capnp.Future }

func (f Tx_Future) Struct() (Tx, error) {
	p, err := f.Future.Ptr()
	return Tx(p.Struct()), err
}

const schema_8c49da2775b6e7db = "x\xda\x12\x08r`1\xe4\xdd\xcf\xc8\xc0\x14(\xc2\xca" +
	"\xf6?\xfeg\xa9,{\xac\xf0K\x86@aF\xc6\xff" +
	"\xb7\x9fo+U\xbf\xe5\xd9\xc3\xc0\xc2\xce\xc0 xt" +
	"\x93\xe0Y\x10}\xd2\x9eA\xf7\x7fAv\xba~rb" +
	"A\x1ec\x81~I\x85^rb\x81|^\x81UH" +
	"E\x00#c \x0b3\x0b\x03\x03\x0b#\x03\x83 o" +
	"\x10\x03C \x0f3c\xa0\x04\x13\xe3\xff\xe4\xfc\xdc\xdc" +
	"\xcc\x12\x9f`\x06F?FN\x06&FN\x06F@" +
	"\x00\x00\x00\xff\xff\xd1\xb1\x1f\xc6"

func RegisterSchema(reg *schemas.Registry) {
	reg.Register(&schemas.Schema{
		String: schema_8c49da2775b6e7db,
		Nodes: []uint64{
			0xe9135d071d75f95f,
		},
		Compressed: true,
	})
}
