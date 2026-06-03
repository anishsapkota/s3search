package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"

	"github.com/blevesearch/vellum"

	"github.com/anishsapkota/s3-search/pkg/docstore"
	"github.com/anishsapkota/s3-search/pkg/postings"
)

// TermPosting accumulates one term's occurrence in one document.
type TermPosting struct {
	Term     string
	DocID    uint32
	Position uint32
}

// FieldAccumulator collects postings for one field.
type FieldAccumulator struct {
	Name    string
	// term → sorted list of docIDs (roaring)
	PostingLists map[string]*postings.List
	// term → per-doc positions
	Positions    map[string]map[uint32][]uint32
	// per-docID length (number of tokens)
	Doclens      []uint32
}

func NewFieldAccumulator(name string) *FieldAccumulator {
	return &FieldAccumulator{
		Name:         name,
		PostingLists: make(map[string]*postings.List),
		Positions:    make(map[string]map[uint32][]uint32),
	}
}

func (fa *FieldAccumulator) AddToken(docID uint32, term string, pos uint32) {
	pl, ok := fa.PostingLists[term]
	if !ok {
		pl = postings.New()
		fa.PostingLists[term] = pl
	}
	pl.Add(docID)
	if fa.Positions[term] == nil {
		fa.Positions[term] = make(map[uint32][]uint32)
	}
	fa.Positions[term][docID] = append(fa.Positions[term][docID], pos)
}

// SetDoclen sets the token count for docID (called once per doc per field).
func (fa *FieldAccumulator) SetDoclen(docID uint32, length uint32) {
	for uint32(len(fa.Doclens)) <= docID {
		fa.Doclens = append(fa.Doclens, 0)
	}
	fa.Doclens[docID] = length
}

// Result is the output of Build.
type Result struct {
	SegBytes      []byte
	HotcacheBytes []byte
	DocCount      uint32
}

// Build constructs .seg + .hotcache byte slices.
func Build(fields []*FieldAccumulator, ds *docstore.Writer, docCount uint32) (*Result, error) {
	dsBlocks, dsIdx, err := ds.Finalize()
	if err != nil {
		return nil, fmt.Errorf("segment build: docstore: %w", err)
	}

	// Build postings + positions into separate buffers; record relative offsets.
	type termRecord struct {
		field   string
		term    string
		postOff uint64 // relative to start of postings section
		postLen uint64
		posOff  uint64 // relative to start of positions section
		posLen  uint64
	}
	var postBuf, posBuf bytes.Buffer
	var termRecords []termRecord

	for _, fa := range fields {
		// Sort terms for FST (lexicographic order required).
		terms := make([]string, 0, len(fa.PostingLists))
		for t := range fa.PostingLists {
			terms = append(terms, t)
		}
		sort.Strings(terms)

		for _, term := range terms {
			pl := fa.PostingLists[term]
			plBytes, err := pl.Serialize()
			if err != nil {
				return nil, fmt.Errorf("serialize postings %s:%s: %w", fa.Name, term, err)
			}

			// Build sorted positions entries.
			docPos := fa.Positions[term]
			docIDs := make([]uint32, 0, len(docPos))
			for docID := range docPos {
				docIDs = append(docIDs, docID)
			}
			sort.Slice(docIDs, func(i, j int) bool { return docIDs[i] < docIDs[j] })
			posEntries := make([]postings.PositionsEntry, len(docIDs))
			for i, docID := range docIDs {
				sortedPos := make([]uint32, len(docPos[docID]))
				copy(sortedPos, docPos[docID])
				sort.Slice(sortedPos, func(a, b int) bool { return sortedPos[a] < sortedPos[b] })
				posEntries[i] = postings.PositionsEntry{DocID: docID, Positions: sortedPos}
			}
			posBytes := postings.EncodePositions(posEntries)

			termRecords = append(termRecords, termRecord{
				field:   fa.Name,
				term:    term,
				postOff: uint64(postBuf.Len()),
				postLen: uint64(len(plBytes)),
				posOff:  uint64(posBuf.Len()),
				posLen:  uint64(len(posBytes)),
			})
			postBuf.Write(plBytes)
			posBuf.Write(posBytes)
		}
	}

	// Build docstore section: block offset table + compressed blocks.
	var dsBuf bytes.Buffer
	blockOffsets := make([]byte, (len(dsBlocks)+1)*8)
	cumOff := uint64(0)
	for i, blk := range dsBlocks {
		binary.LittleEndian.PutUint64(blockOffsets[i*8:], cumOff)
		cumOff += uint64(len(blk))
	}
	binary.LittleEndian.PutUint64(blockOffsets[len(dsBlocks)*8:], cumOff)
	dsBuf.Write(blockOffsets)
	for _, blk := range dsBlocks {
		dsBuf.Write(blk)
	}

	// Build doclens section: per-field, length-prefixed uint32 array.
	var doclenBuf bytes.Buffer
	for _, fa := range fields {
		dl := fa.Doclens
		lbuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lbuf, uint32(len(dl)))
		doclenBuf.Write(lbuf)
		dlBytes := make([]byte, len(dl)*4)
		for i, v := range dl {
			binary.LittleEndian.PutUint32(dlBytes[i*4:], v)
		}
		doclenBuf.Write(dlBytes)
	}

	// Assemble .seg.
	var seg bytes.Buffer
	seg.WriteString(Magic)
	var vbuf [4]byte
	binary.LittleEndian.PutUint32(vbuf[:], Version)
	seg.Write(vbuf[:])

	postBase := uint64(seg.Len())
	seg.Write(postBuf.Bytes())
	posBase := uint64(seg.Len())
	seg.Write(posBuf.Bytes())
	dsBase := uint64(seg.Len())
	seg.Write(dsBuf.Bytes())
	dlBase := uint64(seg.Len())
	seg.Write(doclenBuf.Bytes())

	footer := Footer{
		PostingsOffset:  postBase,
		PostingsLen:     uint64(postBuf.Len()),
		PositionsOffset: posBase,
		PositionsLen:    uint64(posBuf.Len()),
		DocstoreOffset:  dsBase,
		DocstoreLen:     uint64(dsBuf.Len()),
		DoclensOffset:   dlBase,
		DoclensLen:      uint64(doclenBuf.Len()),
		DocCount:        docCount,
	}
	seg.Write(EncodeFooter(footer))

	// Build hotcache: group termRecords by field, build FST per field.
	type fieldGroup struct {
		terms   []string
		offsets []TermOffsets
		doclens []uint32
	}
	fgMap := make(map[string]*fieldGroup)
	for _, tr := range termRecords {
		fg := fgMap[tr.field]
		if fg == nil {
			fg = &fieldGroup{}
			fgMap[tr.field] = fg
		}
		fg.terms = append(fg.terms, tr.term)
		fg.offsets = append(fg.offsets, TermOffsets{
			PostOff: postBase + tr.postOff,
			PostLen: tr.postLen,
			PosOff:  posBase + tr.posOff,
			PosLen:  tr.posLen,
		})
	}
	for _, fa := range fields {
		if fg := fgMap[fa.Name]; fg != nil {
			fg.doclens = fa.Doclens
		}
	}

	// Build FST per field in parallel — each field is independent.
	hcFieldInputs := make([]HotcacheFieldInput, len(fields))
	var fstErr error
	var fstMu sync.Mutex
	var wg sync.WaitGroup
	for i, fa := range fields {
		wg.Add(1)
		go func(i int, fa *FieldAccumulator) {
			defer wg.Done()
			fg := fgMap[fa.Name]
			if fg == nil {
				fg = &fieldGroup{}
			}
			var fstBuf bytes.Buffer
			b, err := vellum.New(&fstBuf, nil)
			if err != nil {
				fstMu.Lock()
				if fstErr == nil {
					fstErr = fmt.Errorf("fst new: %w", err)
				}
				fstMu.Unlock()
				return
			}
			for j, term := range fg.terms {
				if err := b.Insert([]byte(term), uint64(j)); err != nil {
					fstMu.Lock()
					if fstErr == nil {
						fstErr = fmt.Errorf("fst insert %q: %w", term, err)
					}
					fstMu.Unlock()
					return
				}
			}
			if err := b.Close(); err != nil {
				fstMu.Lock()
				if fstErr == nil {
					fstErr = fmt.Errorf("fst close: %w", err)
				}
				fstMu.Unlock()
				return
			}

			avgDoclen := float32(0)
			if len(fa.Doclens) > 0 {
				sum := uint64(0)
				for _, d := range fa.Doclens {
					sum += uint64(d)
				}
				avgDoclen = float32(sum) / float32(len(fa.Doclens))
			}

			hcFieldInputs[i] = HotcacheFieldInput{
				Name:        fa.Name,
				FSTBytes:    fstBuf.Bytes(),
				TermOffsets: fg.offsets,
				Doclens:     fa.Doclens,
				AvgDoclen:   avgDoclen,
			}
		}(i, fa)
	}
	wg.Wait()
	if fstErr != nil {
		return nil, fstErr
	}

	dsIdxBytes := docstore.EncodeIndex(dsIdx)
	hcBytes, err := EncodeHotcache(hcFieldInputs, dsIdxBytes, docCount, uint32(len(dsBlocks)))
	if err != nil {
		return nil, fmt.Errorf("encode hotcache: %w", err)
	}

	return &Result{
		SegBytes:      seg.Bytes(),
		HotcacheBytes: hcBytes,
		DocCount:      docCount,
	}, nil
}
