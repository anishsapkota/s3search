package segment

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/RoaringBitmap/roaring"

	"github.com/anishsapkota/s3-search/pkg/docstore"
	"github.com/anishsapkota/s3-search/pkg/postings"
	"github.com/anishsapkota/s3-search/pkg/store"
)

// Reader fetches data from a .seg file via range-GETs.
type Reader struct {
	bs     store.BlobStore
	segKey string
	footer Footer
}

// NewReader creates a Reader for a segment. It fetches and parses the footer.
func NewReader(ctx context.Context, bs store.BlobStore, segKey string) (*Reader, error) {
	size, err := bs.Head(ctx, segKey)
	if err != nil {
		return nil, fmt.Errorf("segment reader head: %w", err)
	}
	if size < FooterSize {
		return nil, fmt.Errorf("segment %s too small: %d bytes", segKey, size)
	}
	footerBytes, err := bs.GetRange(ctx, segKey, size-FooterSize, FooterSize)
	if err != nil {
		return nil, fmt.Errorf("segment reader footer: %w", err)
	}
	footer, err := DecodeFooter(append(make([]byte, FooterSize), footerBytes...))
	if err != nil {
		return nil, err
	}
	return &Reader{bs: bs, segKey: segKey, footer: footer}, nil
}

// GetPostings fetches and deserializes the posting list for a term given its offsets.
func (r *Reader) GetPostings(ctx context.Context, to TermOffsets) (*postings.List, error) {
	if to.PostLen == 0 {
		return postings.New(), nil
	}
	data, err := r.bs.GetRange(ctx, r.segKey, int64(to.PostOff), int64(to.PostLen))
	if err != nil {
		return nil, fmt.Errorf("get postings: %w", err)
	}
	return postings.Deserialize(data)
}

// GetPositions fetches and decodes position data for a term.
func (r *Reader) GetPositions(ctx context.Context, to TermOffsets) ([]postings.PositionsEntry, error) {
	if to.PosLen == 0 {
		return nil, nil
	}
	data, err := r.bs.GetRange(ctx, r.segKey, int64(to.PosOff), int64(to.PosLen))
	if err != nil {
		return nil, fmt.Errorf("get positions: %w", err)
	}
	return postings.DecodePositions(data)
}

// GetDocBlock fetches a single compressed docstore block.
// numBlocks is the total number of blocks in this segment (from Hotcache.NumDocstoreBlocks).
func (r *Reader) GetDocBlock(ctx context.Context, blockIdx, numBlocks uint32) ([]byte, error) {
	if numBlocks == 0 {
		return nil, fmt.Errorf("GetDocBlock: numBlocks is 0")
	}
	tableOff := r.footer.DocstoreOffset
	// Offset table = (numBlocks+1) uint64 entries (last entry = sentinel = total size).
	tableSize := int64(numBlocks+1) * 8
	tblData, err := r.bs.GetRange(ctx, r.segKey, int64(tableOff), tableSize)
	if err != nil {
		return nil, fmt.Errorf("docstore offset table: %w", err)
	}
	if int((blockIdx+2)*8) > len(tblData) {
		return nil, fmt.Errorf("block index %d out of range (table has %d entries)", blockIdx, len(tblData)/8)
	}
	blkRelStart := binary.LittleEndian.Uint64(tblData[blockIdx*8:])
	blkRelEnd := binary.LittleEndian.Uint64(tblData[(blockIdx+1)*8:])
	blkLen := blkRelEnd - blkRelStart

	// Blocks start immediately after the offset table.
	blkAbsStart := int64(tableOff) + tableSize + int64(blkRelStart)
	return r.bs.GetRange(ctx, r.segKey, blkAbsStart, int64(blkLen))
}

// IterateDocs fetches all docstore blocks and calls fn for each non-deleted doc.
// delBitmap may be nil (no deletes). Stops on first fn error.
func (r *Reader) IterateDocs(ctx context.Context, hc *Hotcache, delBitmap *roaring.Bitmap, fn func(docID uint32, rawJSON []byte) error) error {
	dsIdx, err := docstore.DecodeIndex(hc.DocstoreIdx)
	if err != nil {
		return fmt.Errorf("decode docstore index: %w", err)
	}

	// Fetch all compressed blocks up-front.
	blocks := make([][]byte, hc.NumDocstoreBlocks)
	for i := uint32(0); i < hc.NumDocstoreBlocks; i++ {
		blk, err := r.GetDocBlock(ctx, i, hc.NumDocstoreBlocks)
		if err != nil {
			return fmt.Errorf("fetch block %d: %w", i, err)
		}
		blocks[i] = blk
	}

	dsReader, err := docstore.NewReader(blocks)
	if err != nil {
		return err
	}

	for docID := uint32(0); docID < hc.DocCount; docID++ {
		if delBitmap != nil && delBitmap.Contains(docID) {
			continue
		}
		if int(docID) >= len(dsIdx) {
			continue
		}
		rawJSON, err := dsReader.Get(dsIdx[docID])
		if err != nil {
			return fmt.Errorf("read doc %d: %w", docID, err)
		}
		if err := fn(docID, rawJSON); err != nil {
			return err
		}
	}
	return nil
}

// Footer returns the parsed footer.
func (r *Reader) Footer() Footer { return r.footer }
