package postings

import (
	"encoding/binary"
	"fmt"

	"github.com/RoaringBitmap/roaring"
)

// List wraps a roaring bitmap for a term's doc-ID set.
type List struct {
	bm *roaring.Bitmap
}

func New() *List { return &List{bm: roaring.New()} }

func FromBitmap(bm *roaring.Bitmap) *List { return &List{bm: bm} }

func (l *List) Add(docID uint32) { l.bm.Add(docID) }

func (l *List) Bitmap() *roaring.Bitmap { return l.bm }

func (l *List) Cardinality() uint64 { return l.bm.GetCardinality() }

// Serialize returns a portable roaring binary encoding.
func (l *List) Serialize() ([]byte, error) {
	buf, err := l.bm.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("postings serialize: %w", err)
	}
	return buf, nil
}

// Deserialize parses a roaring binary encoding.
func Deserialize(data []byte) (*List, error) {
	bm := roaring.New()
	if err := bm.UnmarshalBinary(data); err != nil {
		return nil, fmt.Errorf("postings deserialize: %w", err)
	}
	return &List{bm: bm}, nil
}

// PositionsEntry holds per-doc positions for a term in one field.
// Encoded as: [docID varint][count varint][pos varint delta ...] repeated.
type PositionsEntry struct {
	DocID     uint32
	Positions []uint32 // sorted, absolute positions
}

// EncodePositions encodes a slice of PositionsEntry as a byte blob.
func EncodePositions(entries []PositionsEntry) []byte {
	var buf []byte
	for _, e := range entries {
		buf = appendVarint(buf, uint64(e.DocID))
		buf = appendVarint(buf, uint64(len(e.Positions)))
		prev := uint32(0)
		for _, p := range e.Positions {
			buf = appendVarint(buf, uint64(p-prev))
			prev = p
		}
	}
	return buf
}

// DecodePositions parses position data produced by EncodePositions.
func DecodePositions(data []byte) ([]PositionsEntry, error) {
	var entries []PositionsEntry
	offset := 0
	for offset < len(data) {
		docID, n := binary.Uvarint(data[offset:])
		if n <= 0 {
			return nil, fmt.Errorf("positions: bad docID varint at %d", offset)
		}
		offset += n
		count, n := binary.Uvarint(data[offset:])
		if n <= 0 {
			return nil, fmt.Errorf("positions: bad count varint at %d", offset)
		}
		offset += n
		positions := make([]uint32, count)
		prev := uint32(0)
		for i := uint64(0); i < count; i++ {
			delta, n := binary.Uvarint(data[offset:])
			if n <= 0 {
				return nil, fmt.Errorf("positions: bad delta varint at %d", offset)
			}
			offset += n
			positions[i] = prev + uint32(delta)
			prev = positions[i]
		}
		entries = append(entries, PositionsEntry{DocID: uint32(docID), Positions: positions})
	}
	return entries, nil
}

func appendVarint(buf []byte, v uint64) []byte {
	var tmp [10]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(buf, tmp[:n]...)
}
