package search

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/RoaringBitmap/roaring"

	"github.com/anishsapkota/s3-search/pkg/analyze"
	"github.com/anishsapkota/s3-search/pkg/cache"
	"github.com/anishsapkota/s3-search/pkg/index"
	"github.com/anishsapkota/s3-search/pkg/query"
	"github.com/anishsapkota/s3-search/pkg/segment"
	"github.com/anishsapkota/s3-search/pkg/store"
)

const maxSegmentConcurrency = 8

// Request is a search request.
type Request struct {
	Index string
	Query *query.Node
	Size  int
	From  int // zero-based offset into the global sorted result set
}

// Response is the result of a search request.
type Response struct {
	TotalHits uint64      `json:"total_hits"`
	From      int         `json:"from"`
	Size      int         `json:"size"`
	Hits      []query.Hit `json:"hits"`
}

// Searcher coordinates multi-segment search over an S3 index.
type Searcher struct {
	bs        store.BlobStore
	hcLRU     *cache.HotcacheLRU
	tokenizer analyze.Tokenizer
}

func New(bs store.BlobStore, hcCapacity int) *Searcher {
	return &Searcher{
		bs:        bs,
		hcLRU:     cache.NewHotcacheLRU(hcCapacity),
		tokenizer: analyze.StandardTokenizer{},
	}
}

// Search executes a search request against all active segments of an index.
func (s *Searcher) Search(ctx context.Context, ms *index.ManifestStore, req Request) (*Response, error) {
	manifest, err := ms.Latest(ctx)
	if err != nil {
		return nil, fmt.Errorf("search: manifest: %w", err)
	}
	if manifest == nil {
		return &Response{}, nil
	}

	segments := manifest.ActiveSegments()
	if len(segments) == 0 {
		return &Response{}, nil
	}

	type segResult struct {
		partial *query.PartialResult
		err     error
	}
	results := make([]segResult, len(segments))
	sem := make(chan struct{}, maxSegmentConcurrency)
	var wg sync.WaitGroup

	// Each segment must return enough candidates to cover the global from+size window.
	segTopK := req.From + req.Size

	for i, seg := range segments {
		wg.Add(1)
		go func(i int, seg index.SegmentMeta) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			partial, err := s.searchSegment(ctx, req.Index, seg, req.Query, segTopK)
			results[i] = segResult{partial: partial, err: err}
		}(i, seg)
	}
	wg.Wait()

	var allHits []query.Hit
	totalHits := uint64(0)
	for _, r := range results {
		if r.err != nil {
			slog.Warn("segment search error", "err", r.err)
			continue
		}
		if r.partial == nil {
			continue
		}
		totalHits += r.partial.TotalHits
		allHits = append(allHits, r.partial.Hits...)
	}

	sort.Slice(allHits, func(i, j int) bool {
		return allHits[i].Score > allHits[j].Score
	})

	// Apply global from/size window.
	from := req.From
	if from > len(allHits) {
		from = len(allHits)
	}
	allHits = allHits[from:]
	if len(allHits) > req.Size {
		allHits = allHits[:req.Size]
	}

	return &Response{
		TotalHits: totalHits,
		From:      req.From,
		Size:      len(allHits),
		Hits:      allHits,
	}, nil
}

func (s *Searcher) searchSegment(ctx context.Context, indexName string, meta index.SegmentMeta, q *query.Node, topK int) (*query.PartialResult, error) {
	hcKey := index.SegmentKey(indexName, meta.ID, ".hotcache")
	segKey := index.SegmentKey(indexName, meta.ID, ".seg")

	hc, ok := s.hcLRU.Get(hcKey)
	if !ok {
		data, err := s.bs.Get(ctx, hcKey)
		if err != nil {
			return nil, fmt.Errorf("fetch hotcache %s: %w", hcKey, err)
		}
		hc, err = segment.DecodeHotcache(data)
		if err != nil {
			return nil, fmt.Errorf("decode hotcache %s: %w", hcKey, err)
		}
		s.hcLRU.Put(hcKey, hc)
	}

	// Fetch del bitmap if this segment has deletes.
	var delBitmap *roaring.Bitmap
	if meta.DelVersion > 0 {
		var err error
		delBitmap, err = index.ReadDelBitmap(ctx, s.bs, indexName, meta.ID, meta.DelVersion)
		if err != nil {
			slog.Warn("fetch del bitmap", "seg", meta.ID, "err", err)
			// Non-fatal: continue without del filtering.
		}
	}

	return query.SegmentSearch(ctx, s.bs, segKey, hcKey, hc, q, topK, s.tokenizer, delBitmap)
}
