package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/blevesearch/vellum"
)

const hotcacheMagic = "S3HC"

// TermOffsets holds byte ranges into the .seg file for one term.
type TermOffsets struct {
	PostOff uint64
	PostLen uint64
	PosOff  uint64
	PosLen  uint64
}

// FieldHotcache holds the in-memory data for one field loaded from .hotcache.
type FieldHotcache struct {
	Name       string
	FST        *vellum.FST
	TermTable  []TermOffsets // indexed by term ID from FST
	Doclens    []uint32      // per docID (text fields only)
	AvgDoclen  float32
}

// Hotcache is the fully loaded hotcache for one segment.
type Hotcache struct {
	Fields            []*FieldHotcache
	DocstoreIdx       []byte // raw encoded docstore index (BlockEntry array)
	DocCount          uint32
	NumDocstoreBlocks uint32 // number of compressed blocks in the docstore section
	fieldIndex        map[string]*FieldHotcache
}

func (h *Hotcache) Field(name string) *FieldHotcache {
	return h.fieldIndex[name]
}

// EncodeHotcache serializes segment hotcache data.
func EncodeHotcache(
	fields []HotcacheFieldInput,
	docstoreIdx []byte,
	docCount uint32,
	numDocstoreBlocks uint32,
) ([]byte, error) {
	var buf bytes.Buffer

	// magic + version
	buf.WriteString(hotcacheMagic)
	writeU32(&buf, 1)
	writeU32(&buf, docCount)
	writeU32(&buf, numDocstoreBlocks)
	writeU32(&buf, uint32(len(fields)))

	// docstore index
	writeBlob(&buf, docstoreIdx)

	for _, f := range fields {
		// name
		writeBlob(&buf, []byte(f.Name))
		// FST bytes
		writeBlob(&buf, f.FSTBytes)
		// term offsets table: len(terms) × 32 bytes
		tblBytes := make([]byte, len(f.TermOffsets)*32)
		for i, to := range f.TermOffsets {
			base := i * 32
			binary.LittleEndian.PutUint64(tblBytes[base:], to.PostOff)
			binary.LittleEndian.PutUint64(tblBytes[base+8:], to.PostLen)
			binary.LittleEndian.PutUint64(tblBytes[base+16:], to.PosOff)
			binary.LittleEndian.PutUint64(tblBytes[base+24:], to.PosLen)
		}
		writeBlob(&buf, tblBytes)
		// doclen array (may be nil for keyword/i64 fields)
		doclensBytes := make([]byte, len(f.Doclens)*4)
		for i, d := range f.Doclens {
			binary.LittleEndian.PutUint32(doclensBytes[i*4:], d)
		}
		writeBlob(&buf, doclensBytes)
		// avg doclen (float32 as uint32 bits)
		writeU32(&buf, math32bits(f.AvgDoclen))
	}
	return buf.Bytes(), nil
}

// HotcacheFieldInput is passed to EncodeHotcache.
type HotcacheFieldInput struct {
	Name        string
	FSTBytes    []byte
	TermOffsets []TermOffsets
	Doclens     []uint32
	AvgDoclen   float32
}

// DecodeHotcache parses a hotcache blob.
func DecodeHotcache(data []byte) (*Hotcache, error) {
	if len(data) < 4 || string(data[:4]) != hotcacheMagic {
		return nil, fmt.Errorf("hotcache: bad magic")
	}
	off := 4
	getU32 := func() uint32 {
		v := binary.LittleEndian.Uint32(data[off:])
		off += 4
		return v
	}
	getBlob := func() []byte {
		l := int(getU32())
		b := data[off : off+l]
		off += l
		return b
	}

	_ = getU32() // version
	docCount := getU32()
	numDocstoreBlocks := getU32()
	numFields := getU32()
	docstoreIdx := getBlob()

	hc := &Hotcache{
		DocCount:          docCount,
		NumDocstoreBlocks: numDocstoreBlocks,
		DocstoreIdx:       docstoreIdx,
		fieldIndex:        make(map[string]*FieldHotcache),
	}

	for i := 0; i < int(numFields); i++ {
		name := string(getBlob())
		fstBytes := getBlob()
		tblBytes := getBlob()
		doclensBytes := getBlob()
		avgDoclenbits := getU32()

		fst, err := vellum.Load(fstBytes)
		if err != nil {
			return nil, fmt.Errorf("hotcache: field %s fst: %w", name, err)
		}

		numTerms := len(tblBytes) / 32
		termTable := make([]TermOffsets, numTerms)
		for j := range termTable {
			base := j * 32
			termTable[j].PostOff = binary.LittleEndian.Uint64(tblBytes[base:])
			termTable[j].PostLen = binary.LittleEndian.Uint64(tblBytes[base+8:])
			termTable[j].PosOff = binary.LittleEndian.Uint64(tblBytes[base+16:])
			termTable[j].PosLen = binary.LittleEndian.Uint64(tblBytes[base+24:])
		}

		var doclens []uint32
		if len(doclensBytes) > 0 {
			doclens = make([]uint32, len(doclensBytes)/4)
			for j := range doclens {
				doclens[j] = binary.LittleEndian.Uint32(doclensBytes[j*4:])
			}
		}

		fhc := &FieldHotcache{
			Name:      name,
			FST:       fst,
			TermTable: termTable,
			Doclens:   doclens,
			AvgDoclen: bitsToFloat32(avgDoclenbits),
		}
		hc.Fields = append(hc.Fields, fhc)
		hc.fieldIndex[name] = fhc
	}
	return hc, nil
}

// LookupTerm returns the TermOffsets for a term in a field, or (zero, false).
func (fhc *FieldHotcache) LookupTerm(term string) (TermOffsets, bool) {
	if fhc.FST == nil {
		return TermOffsets{}, false
	}
	id, exists, err := fhc.FST.Get([]byte(term))
	if err != nil || !exists {
		return TermOffsets{}, false
	}
	if int(id) >= len(fhc.TermTable) {
		return TermOffsets{}, false
	}
	return fhc.TermTable[id], true
}

func writeU32(buf *bytes.Buffer, v uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	buf.Write(b[:])
}

func writeBlob(buf *bytes.Buffer, data []byte) {
	writeU32(buf, uint32(len(data)))
	buf.Write(data)
}

func math32bits(f float32) uint32 {
	return math.Float32bits(f)
}

func bitsToFloat32(v uint32) float32 {
	return math.Float32frombits(v)
}

