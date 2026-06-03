package query

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/RoaringBitmap/roaring"

	"github.com/anishsapkota/s3-search/pkg/analyze"
	"github.com/anishsapkota/s3-search/pkg/docstore"
	"github.com/anishsapkota/s3-search/pkg/segment"
	"github.com/anishsapkota/s3-search/pkg/store"
)

// MaxSearchSize caps the number of hits returned per request.
// Prevents accidental unbounded result sets and runaway memory.
const MaxSearchSize = 10_000

// blockFetchConcurrency limits parallel S3 range-GETs per segment for doc blocks.
const blockFetchConcurrency = 16

// Hit is one search result.
type Hit struct {
	DocID  uint32          `json:"doc_id"`
	Score  float64         `json:"score"`
	Source json.RawMessage `json:"source,omitempty"` // embedded JSON, not base64
}

// PartialResult is the output of searching one segment.
type PartialResult struct {
	Hits      []Hit
	TotalHits uint64
}

// SegmentSearch searches one segment using its hotcache + range-GET reader.
// delBitmap is nil if no deletes exist for this segment.
func SegmentSearch(
	ctx context.Context,
	bs store.BlobStore,
	segKey, hcKey string,
	hc *segment.Hotcache,
	q *Node,
	topK int,
	tok analyze.Tokenizer,
	delBitmap *roaring.Bitmap,
) (*PartialResult, error) {
	reader, err := segment.NewReader(ctx, bs, segKey)
	if err != nil {
		return nil, fmt.Errorf("segment reader: %w", err)
	}

	scoredDocs, err := execNode(ctx, reader, hc, q, tok, delBitmap)
	if err != nil {
		return nil, err
	}

	total := uint64(len(scoredDocs))
	sort.Slice(scoredDocs, func(i, j int) bool {
		return scoredDocs[i].Score > scoredDocs[j].Score
	})
	if len(scoredDocs) > topK {
		scoredDocs = scoredDocs[:topK]
	}

	dsIdx, err := docstore.DecodeIndex(hc.DocstoreIdx)
	if err != nil {
		return nil, fmt.Errorf("decode docstore index: %w", err)
	}

	// Collect the distinct block indices needed for the top-K hits.
	neededBlocks := make(map[uint32]struct{})
	for _, h := range scoredDocs {
		if int(h.DocID) < len(dsIdx) {
			neededBlocks[dsIdx[h.DocID].Block] = struct{}{}
		}
	}

	// Fetch blocks in parallel (bounded by blockFetchConcurrency).
	blockSet := make(map[uint32][]byte, len(neededBlocks))
	var (
		mu      sync.Mutex
		fetchErr error
	)
	sem := make(chan struct{}, blockFetchConcurrency)
	var wg sync.WaitGroup
	for blkIdx := range neededBlocks {
		wg.Add(1)
		go func(idx uint32) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			data, err := reader.GetDocBlock(ctx, idx, hc.NumDocstoreBlocks)
			mu.Lock()
			if err != nil && fetchErr == nil {
				fetchErr = fmt.Errorf("fetch doc block %d: %w", idx, err)
			} else if err == nil {
				blockSet[idx] = data
			}
			mu.Unlock()
		}(blkIdx)
	}
	wg.Wait()
	if fetchErr != nil {
		return nil, fetchErr
	}

	maxBlk := uint32(0)
	for b := range blockSet {
		if b > maxBlk {
			maxBlk = b
		}
	}
	blocks := make([][]byte, maxBlk+1)
	for b, data := range blockSet {
		blocks[b] = data
	}
	dsReader, err := docstore.NewReader(blocks)
	if err != nil {
		return nil, err
	}

	hits := make([]Hit, 0, len(scoredDocs))
	for _, h := range scoredDocs {
		if int(h.DocID) >= len(dsIdx) {
			hits = append(hits, h)
			continue
		}
		src, err := dsReader.Get(dsIdx[h.DocID])
		if err != nil {
			return nil, err
		}
		h.Source = src
		hits = append(hits, h)
	}
	return &PartialResult{Hits: hits, TotalHits: total}, nil
}

// SearchDocIDs returns all matching docIDs for a query (no source fetch).
// Used by delete-by-query. delBitmap filters already-deleted docs.
func SearchDocIDs(
	ctx context.Context,
	bs store.BlobStore,
	segKey string,
	hc *segment.Hotcache,
	q *Node,
	tok analyze.Tokenizer,
	delBitmap *roaring.Bitmap,
) (*roaring.Bitmap, error) {
	reader, err := segment.NewReader(ctx, bs, segKey)
	if err != nil {
		return nil, err
	}
	bm, _, err := evalBitmap(ctx, reader, hc, q, tok)
	if err != nil {
		return nil, err
	}
	if bm == nil {
		return roaring.New(), nil
	}
	if delBitmap != nil {
		bm.AndNot(delBitmap)
	}
	return bm, nil
}

func execNode(ctx context.Context, r *segment.Reader, hc *segment.Hotcache, q *Node, tok analyze.Tokenizer, delBitmap *roaring.Bitmap) ([]Hit, error) {
	bm, scores, err := evalBitmap(ctx, r, hc, q, tok)
	if err != nil {
		return nil, err
	}
	if bm == nil {
		return nil, nil
	}
	if delBitmap != nil {
		bm.AndNot(delBitmap)
	}
	it := bm.Iterator()
	hits := make([]Hit, 0, bm.GetCardinality())
	for it.HasNext() {
		docID := it.Next()
		score := float64(0)
		if scores != nil {
			score = scores[docID]
		}
		hits = append(hits, Hit{DocID: docID, Score: score})
	}
	return hits, nil
}

func evalBitmap(ctx context.Context, r *segment.Reader, hc *segment.Hotcache, q *Node, tok analyze.Tokenizer) (*roaring.Bitmap, map[uint32]float64, error) {
	N := uint64(hc.DocCount)

	switch q.Type {
	case NodeMatch:
		tokens := tok.Tokenize(q.Value)
		if len(tokens) == 0 {
			return roaring.New(), nil, nil
		}
		var combined *roaring.Bitmap
		scores := make(map[uint32]float64)
		for _, t := range tokens {
			bm, sc, err := termBitmap(ctx, r, hc, q.Field, t.Term, N)
			if err != nil {
				return nil, nil, err
			}
			if combined == nil {
				combined = bm
			} else {
				combined.And(bm)
			}
			for docID, s := range sc {
				scores[docID] += s
			}
		}
		if combined == nil {
			combined = roaring.New()
		}
		return combined, scores, nil

	case NodeTerm, NodeKeyword:
		return termBitmap(ctx, r, hc, q.Field, q.Value, N)

	case NodePrefix:
		return prefixBitmap(ctx, r, hc, q.Field, q.Value, N)

	case NodePhrase:
		return phraseBitmap(ctx, r, hc, q.Field, q.Terms, N)

	case NodeBool:
		return evalBool(ctx, r, hc, q, tok, N)

	case NodeRange:
		return roaring.New(), nil, nil

	default:
		return nil, nil, fmt.Errorf("unknown query node type %q", q.Type)
	}
}

func termBitmap(ctx context.Context, r *segment.Reader, hc *segment.Hotcache, field, term string, N uint64) (*roaring.Bitmap, map[uint32]float64, error) {
	if field == "_all" {
		var union *roaring.Bitmap
		allScores := make(map[uint32]float64)
		for _, fhc := range hc.Fields {
			bm, scores, err := termBitmapField(ctx, r, fhc, term, N)
			if err != nil {
				return nil, nil, err
			}
			if union == nil {
				union = bm
			} else {
				union.Or(bm)
			}
			for d, s := range scores {
				if s > allScores[d] {
					allScores[d] = s
				}
			}
		}
		if union == nil {
			union = roaring.New()
		}
		return union, allScores, nil
	}
	fhc := hc.Field(field)
	if fhc == nil {
		return roaring.New(), nil, nil
	}
	return termBitmapField(ctx, r, fhc, term, N)
}

func termBitmapField(ctx context.Context, r *segment.Reader, fhc *segment.FieldHotcache, term string, N uint64) (*roaring.Bitmap, map[uint32]float64, error) {
	to, ok := fhc.LookupTerm(term)
	if !ok {
		return roaring.New(), nil, nil
	}
	pl, err := r.GetPostings(ctx, to)
	if err != nil {
		return nil, nil, err
	}
	df := pl.Cardinality()
	avgDoclen := float64(fhc.AvgDoclen)
	if avgDoclen == 0 {
		avgDoclen = 1
	}
	scores := make(map[uint32]float64)
	it := pl.Bitmap().Iterator()
	for it.HasNext() {
		docID := it.Next()
		docLen := float64(1)
		if len(fhc.Doclens) > int(docID) {
			docLen = float64(fhc.Doclens[docID])
		}
		scores[docID] = BM25Score(1, docLen, avgDoclen, df, N)
	}
	return pl.Bitmap(), scores, nil
}

func prefixBitmap(ctx context.Context, r *segment.Reader, hc *segment.Hotcache, field, prefix string, N uint64) (*roaring.Bitmap, map[uint32]float64, error) {
	fhc := hc.Field(field)
	if fhc == nil || fhc.FST == nil {
		return roaring.New(), nil, nil
	}
	union := roaring.New()
	scores := make(map[uint32]float64)
	it, err := fhc.FST.Iterator([]byte(prefix), nil)
	if err != nil {
		return roaring.New(), nil, nil
	}
	for err == nil {
		key, id := it.Current()
		if len(key) < len(prefix) || string(key[:len(prefix)]) != prefix {
			break
		}
		if int(id) < len(fhc.TermTable) {
			to := fhc.TermTable[id]
			pl, e := r.GetPostings(ctx, to)
			if e != nil {
				return nil, nil, e
			}
			union.Or(pl.Bitmap())
			df := pl.Cardinality()
			it2 := pl.Bitmap().Iterator()
			for it2.HasNext() {
				docID := it2.Next()
				s := BM25Score(1, 1, float64(fhc.AvgDoclen), df, N)
				if s > scores[docID] {
					scores[docID] = s
				}
			}
		}
		err = it.Next()
	}
	return union, scores, nil
}

func phraseBitmap(ctx context.Context, r *segment.Reader, hc *segment.Hotcache, field string, terms []string, N uint64) (*roaring.Bitmap, map[uint32]float64, error) {
	if len(terms) == 0 {
		return roaring.New(), nil, nil
	}
	fhc := hc.Field(field)
	if fhc == nil {
		return roaring.New(), nil, nil
	}
	tds := make([]termData, len(terms))
	for i, term := range terms {
		to, ok := fhc.LookupTerm(term)
		if !ok {
			return roaring.New(), nil, nil
		}
		pl, err := r.GetPostings(ctx, to)
		if err != nil {
			return nil, nil, err
		}
		posEntries, err := r.GetPositions(ctx, to)
		if err != nil {
			return nil, nil, err
		}
		posMap := make(map[uint32][]uint32, len(posEntries))
		for _, pe := range posEntries {
			posMap[pe.DocID] = pe.Positions
		}
		tds[i] = termData{pl: pl.Bitmap(), pos: posMap}
	}
	combined := tds[0].pl.Clone()
	for i := 1; i < len(tds); i++ {
		combined.And(tds[i].pl)
	}
	result := roaring.New()
	scores := make(map[uint32]float64)
	it := combined.Iterator()
	for it.HasNext() {
		docID := it.Next()
		if phraseMatch(docID, tds) {
			result.Add(docID)
			scores[docID] = BM25Score(float64(len(terms)), 1, float64(fhc.AvgDoclen), combined.GetCardinality(), N)
		}
	}
	return result, scores, nil
}

type termData struct {
	pl  *roaring.Bitmap
	pos map[uint32][]uint32
}

func phraseMatch(docID uint32, tds []termData) bool {
	if len(tds) == 0 {
		return false
	}
	for _, start := range tds[0].pos[docID] {
		match := true
		for i := 1; i < len(tds); i++ {
			needed := start + uint32(i)
			found := false
			for _, p := range tds[i].pos[docID] {
				if p == needed {
					found = true
					break
				}
				if p > needed {
					break
				}
			}
			if !found {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func evalBool(ctx context.Context, r *segment.Reader, hc *segment.Hotcache, q *Node, tok analyze.Tokenizer, N uint64) (*roaring.Bitmap, map[uint32]float64, error) {
	scores := make(map[uint32]float64)
	var mustBM *roaring.Bitmap

	for _, child := range q.Must {
		bm, sc, err := evalBitmap(ctx, r, hc, child, tok)
		if err != nil {
			return nil, nil, err
		}
		if mustBM == nil {
			mustBM = bm
		} else {
			mustBM.And(bm)
		}
		for d, s := range sc {
			scores[d] += s
		}
	}
	for _, child := range q.Filter {
		bm, _, err := evalBitmap(ctx, r, hc, child, tok)
		if err != nil {
			return nil, nil, err
		}
		if mustBM == nil {
			mustBM = bm
		} else {
			mustBM.And(bm)
		}
	}
	if len(q.Should) > 0 {
		var shouldBM *roaring.Bitmap
		for _, child := range q.Should {
			bm, sc, err := evalBitmap(ctx, r, hc, child, tok)
			if err != nil {
				return nil, nil, err
			}
			if shouldBM == nil {
				shouldBM = bm
			} else {
				shouldBM.Or(bm)
			}
			for d, s := range sc {
				scores[d] += s
			}
		}
		if mustBM == nil {
			mustBM = shouldBM
		} else {
			it := shouldBM.Iterator()
			for it.HasNext() {
				d := it.Next()
				scores[d] *= 1.1
			}
		}
	}
	for _, child := range q.MustNot {
		bm, _, err := evalBitmap(ctx, r, hc, child, tok)
		if err != nil {
			return nil, nil, err
		}
		if mustBM == nil {
			allBM := roaring.New()
			allBM.AddRange(0, uint64(N))
			mustBM = allBM
		}
		mustBM.AndNot(bm)
	}
	if mustBM == nil {
		bm := roaring.New()
		bm.AddRange(0, uint64(N))
		return bm, scores, nil
	}
	return mustBM, scores, nil
}
