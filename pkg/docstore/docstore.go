package docstore

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

const blockSize = 64 * 1024 // 64KB uncompressed target

// BlockEntry is stored in the hotcache: docID → (block index, byte offset within block).
type BlockEntry struct {
	Block  uint32
	Offset uint32
}

// Writer builds a doc store: compresses doc JSON into ~64KB blocks.
type Writer struct {
	blocks   [][]byte // compressed blocks
	index    []BlockEntry
	curBuf   bytes.Buffer
	curDocs  uint32
	curStart uint32 // first docID in current block
	enc      *zstd.Encoder
}

func NewWriter() (*Writer, error) {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	return &Writer{enc: enc}, nil
}

// Add appends a doc (raw JSON bytes) to the store. docID must be monotonically increasing.
func (w *Writer) Add(docID uint32, rawJSON []byte) error {
	// length-prefix each doc
	var lenbuf [4]byte
	binary.LittleEndian.PutUint32(lenbuf[:], uint32(len(rawJSON)))
	w.curBuf.Write(lenbuf[:])
	w.curBuf.Write(rawJSON)

	blockIdx := uint32(len(w.blocks))
	offset := uint32(w.curBuf.Len()) - uint32(len(rawJSON)) - 4
	w.index = append(w.index, BlockEntry{Block: blockIdx, Offset: offset})

	if w.curBuf.Len() >= blockSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) flushBlock() error {
	if w.curBuf.Len() == 0 {
		return nil
	}
	compressed := w.enc.EncodeAll(w.curBuf.Bytes(), nil)
	w.blocks = append(w.blocks, compressed)
	// Fix up block indices for docs written to this (now committed) block.
	commitIdx := uint32(len(w.blocks) - 1)
	for i := len(w.index) - 1; i >= 0; i-- {
		if w.index[i].Block == commitIdx {
			break
		}
		// They were assigned the speculative next block index; correct to committed.
		w.index[i].Block = commitIdx
	}
	w.curBuf.Reset()
	return nil
}

// Finalize flushes any remaining docs and returns (blocks, index).
func (w *Writer) Finalize() (blocks [][]byte, index []BlockEntry, err error) {
	if err = w.flushBlock(); err != nil {
		return
	}
	return w.blocks, w.index, nil
}

// EncodeIndex serializes BlockEntry slice to bytes.
func EncodeIndex(idx []BlockEntry) []byte {
	buf := make([]byte, 8*len(idx))
	for i, e := range idx {
		binary.LittleEndian.PutUint32(buf[i*8:], e.Block)
		binary.LittleEndian.PutUint32(buf[i*8+4:], e.Offset)
	}
	return buf
}

// DecodeIndex deserializes BlockEntry slice.
func DecodeIndex(data []byte) ([]BlockEntry, error) {
	if len(data)%8 != 0 {
		return nil, fmt.Errorf("docstore index: invalid length %d", len(data))
	}
	n := len(data) / 8
	out := make([]BlockEntry, n)
	for i := range out {
		out[i].Block = binary.LittleEndian.Uint32(data[i*8:])
		out[i].Offset = binary.LittleEndian.Uint32(data[i*8+4:])
	}
	return out, nil
}

// Reader reads docs from a sequence of compressed blocks.
type Reader struct {
	blocks [][]byte // raw compressed blocks (range-fetched)
	dec    *zstd.Decoder
}

func NewReader(blocks [][]byte) (*Reader, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	return &Reader{blocks: blocks, dec: dec}, nil
}

// Get fetches the raw JSON for a doc given its BlockEntry.
func (r *Reader) Get(entry BlockEntry) ([]byte, error) {
	if int(entry.Block) >= len(r.blocks) {
		return nil, fmt.Errorf("docstore: block %d out of range", entry.Block)
	}
	raw, err := r.dec.DecodeAll(r.blocks[entry.Block], nil)
	if err != nil {
		return nil, fmt.Errorf("docstore decompress: %w", err)
	}
	off := entry.Offset
	if int(off)+4 > len(raw) {
		return nil, fmt.Errorf("docstore: offset %d out of range", off)
	}
	docLen := binary.LittleEndian.Uint32(raw[off:])
	start := off + 4
	end := start + docLen
	if int(end) > len(raw) {
		return nil, fmt.Errorf("docstore: doc length %d overruns block", docLen)
	}
	out := make([]byte, docLen)
	copy(out, raw[start:end])
	return out, nil
}

// GetJSON deserializes a doc into v.
func (r *Reader) GetJSON(entry BlockEntry, v interface{}) error {
	raw, err := r.Get(entry)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}
