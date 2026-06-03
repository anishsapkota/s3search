package index

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/bits"
	"sort"
	"strings"
	"time"

	"github.com/RoaringBitmap/roaring"

	"github.com/anishsapkota/s3-search/pkg/analyze"
	"github.com/anishsapkota/s3-search/pkg/obs"
	"github.com/anishsapkota/s3-search/pkg/schema"
	"github.com/anishsapkota/s3-search/pkg/segment"
	"github.com/anishsapkota/s3-search/pkg/store"
)

const (
	minMergeCount  = 4          // min segments in a tier to trigger merge
	gcGracePeriod  = 5 * time.Minute
)

// Compactor performs size-tiered segment merging and orphan GC for one index.
type Compactor struct {
	bs        store.BlobStore
	ms        *ManifestStore
	sc        *schema.Schema
	tok       analyze.Tokenizer
	indexName string
}

func NewCompactor(bs store.BlobStore, ms *ManifestStore, sc *schema.Schema, indexName string) *Compactor {
	return &Compactor{
		bs:        bs,
		ms:        ms,
		sc:        sc,
		tok:       analyze.StandardTokenizer{},
		indexName: indexName,
	}
}

// Run starts a background compaction loop that fires at interval.
func (c *Compactor) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := c.CompactOnce(ctx); err != nil {
				slog.Warn("compact failed", "index", c.indexName, "err", err)
			}
		}
	}
}

// CompactOnce finds the best merge candidate and executes one merge if found.
func (c *Compactor) CompactOnce(ctx context.Context) error {
	manifest, err := c.ms.Latest(ctx)
	if err != nil {
		return err
	}
	if manifest == nil {
		return nil
	}

	active := manifest.ActiveSegments()
	candidates := c.pickMergeCandidates(active)
	if len(candidates) < 2 {
		return nil
	}

	slog.Info("compacting", "index", c.indexName, "segments", len(candidates))
	return c.merge(ctx, candidates)
}

// pickMergeCandidates selects segments for size-tiered merge.
// Returns the group of same-size-tier segments to merge (≥ minMergeCount).
func (c *Compactor) pickMergeCandidates(segs []SegmentMeta) []SegmentMeta {
	if len(segs) == 0 {
		return nil
	}
	// Bucket by floor(log2(bytes)) — segments within the same bit-length bucket.
	type bucket struct {
		tier int
		segs []SegmentMeta
	}
	tierMap := make(map[int][]SegmentMeta)
	for _, s := range segs {
		b := int64(s.Bytes)
		if b <= 0 {
			b = 1
		}
		tier := bits.Len64(uint64(b)) // log2 floor (1-indexed bit length)
		tierMap[tier] = append(tierMap[tier], s)
	}
	// Find the largest tier with ≥ minMergeCount segments.
	var best []SegmentMeta
	for _, v := range tierMap {
		if len(v) >= minMergeCount && len(v) > len(best) {
			best = v
		}
	}
	if len(best) == 0 {
		// No tier meets minMergeCount. If total segments is large, merge smallest 4.
		if len(segs) >= minMergeCount {
			sorted := make([]SegmentMeta, len(segs))
			copy(sorted, segs)
			sort.Slice(sorted, func(i, j int) bool { return sorted[i].Bytes < sorted[j].Bytes })
			return sorted[:minMergeCount]
		}
		return nil
	}
	return best
}

// merge re-indexes all non-deleted docs from candidates into a new segment,
// then publishes a new manifest marking the source segments deleted.
func (c *Compactor) merge(ctx context.Context, candidates []SegmentMeta) error {
	// Sort candidates by CreatedAt ascending (oldest first) so that for _id
	// dedup, we process newest segments last — but we iterate in reverse below.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].CreatedAt < candidates[j].CreatedAt
	})

	// Track seen IDs for _id dedup (newest segment wins).
	seenIDs := make(map[string]bool)
	idField := c.sc.IDField

	mt, err := NewMemtable(c.sc, c.tok)
	if err != nil {
		return err
	}

	// Process segments newest-first so that for duplicate _id, the newest doc
	// gets indexed and older duplicates are skipped.
	for i := len(candidates) - 1; i >= 0; i-- {
		meta := candidates[i]
		if err := c.reindexSegment(ctx, meta, mt, seenIDs, idField); err != nil {
			return fmt.Errorf("reindex seg %s: %w", meta.ID, err)
		}
	}

	if mt.DocCount() == 0 {
		// All docs were deleted — just mark source segments deleted.
		return c.markDeleted(ctx, candidates)
	}

	result, err := mt.Build()
	if err != nil {
		return fmt.Errorf("compact: build segment: %w", err)
	}

	newSegID := newSegmentID()
	segKey := SegmentKey(c.indexName, newSegID, ".seg")
	hcKey := SegmentKey(c.indexName, newSegID, ".hotcache")

	if err := c.bs.Put(ctx, segKey, result.SegBytes, store.PutOpts{}); err != nil {
		return fmt.Errorf("upload compacted seg: %w", err)
	}
	if err := c.bs.Put(ctx, hcKey, result.HotcacheBytes, store.PutOpts{}); err != nil {
		return fmt.Errorf("upload compacted hotcache: %w", err)
	}

	sourceIDs := make(map[string]bool, len(candidates))
	for _, s := range candidates {
		sourceIDs[s.ID] = true
	}
	newMeta := SegmentMeta{
		ID:        newSegID,
		DocCount:  result.DocCount,
		Bytes:     int64(len(result.SegBytes)),
		CreatedAt: time.Now().UnixMilli(),
	}

	if _, err := c.ms.PublishTransform(ctx, func(segs []SegmentMeta) []SegmentMeta {
		out := make([]SegmentMeta, 0, len(segs)+1)
		for _, s := range segs {
			if sourceIDs[s.ID] {
				s.Deleted = true
			}
			out = append(out, s)
		}
		out = append(out, newMeta)
		return out
	}, 5); err != nil {
		return fmt.Errorf("compact: publish: %w", err)
	}

	obs.CompactionTotal.WithLabelValues(c.indexName, "ok").Inc()
	obs.CompactionMergedSegments.WithLabelValues(c.indexName).Observe(float64(len(candidates)))
	slog.Info("compact done", "index", c.indexName, "merged", len(candidates), "new_seg", newSegID, "docs", result.DocCount)
	return nil
}

func (c *Compactor) reindexSegment(ctx context.Context, meta SegmentMeta, mt *Memtable, seenIDs map[string]bool, idField string) error {
	hcKey := SegmentKey(c.indexName, meta.ID, ".hotcache")
	hcData, err := c.bs.Get(ctx, hcKey)
	if err != nil {
		return fmt.Errorf("fetch hotcache: %w", err)
	}
	hc, err := segment.DecodeHotcache(hcData)
	if err != nil {
		return err
	}

	// Load del bitmap.
	var delBitmap *roaring.Bitmap
	if meta.DelVersion > 0 {
		delBitmap, err = ReadDelBitmap(ctx, c.bs, c.indexName, meta.ID, meta.DelVersion)
		if err != nil {
			slog.Warn("read del bitmap", "seg", meta.ID, "err", err)
		}
	}

	segKey := SegmentKey(c.indexName, meta.ID, ".seg")
	reader, err := segment.NewReader(ctx, c.bs, segKey)
	if err != nil {
		return err
	}

	return reader.IterateDocs(ctx, hc, delBitmap, func(_ uint32, rawJSON []byte) error {
		// _id dedup: skip if we already indexed this ID from a newer segment.
		if idField != "" {
			id := extractField(rawJSON, idField)
			if id != "" {
				if seenIDs[id] {
					return nil // skip duplicate
				}
				seenIDs[id] = true
			}
		}
		return mt.Add(rawJSON)
	})
}

func (c *Compactor) markDeleted(ctx context.Context, candidates []SegmentMeta) error {
	ids := make(map[string]bool, len(candidates))
	for _, s := range candidates {
		ids[s.ID] = true
	}
	_, err := c.ms.PublishTransform(ctx, func(segs []SegmentMeta) []SegmentMeta {
		for i := range segs {
			if ids[segs[i].ID] {
				segs[i].Deleted = true
			}
		}
		return segs
	}, 5)
	return err
}

// GC deletes segment files for segments not active in the latest manifest.
// Segments marked deleted=true (after compaction) are removed.
// keepManifests is kept for API compatibility but ignored — we use the latest manifest only.
func (c *Compactor) GC(ctx context.Context, _ int) error {
	manifest, err := c.ms.Latest(ctx)
	if err != nil {
		return err
	}
	if manifest == nil {
		return nil
	}

	// Only protect segments that are ACTIVE (not deleted) in the latest manifest.
	protected := make(map[string]bool)
	for _, s := range manifest.ActiveSegments() {
		protected[s.ID] = true
	}

	prefix := c.indexName + "/segments/"
	keys, err := c.bs.List(ctx, prefix)
	if err != nil {
		return err
	}

	deleted := 0
	for _, key := range keys {
		segID := extractSegIDFromKey(key)
		if segID == "" || protected[segID] {
			continue
		}
		if err := c.bs.Delete(ctx, key); err != nil {
			slog.Warn("gc: delete failed", "key", key, "err", err)
			continue
		}
		deleted++
	}
	slog.Info("gc done", "index", c.indexName, "deleted_objects", deleted)
	return nil
}

// extractSegIDFromKey parses the segment ID from a key like "{index}/segments/{id}.{ext}".
func extractSegIDFromKey(key string) string {
	slash := strings.LastIndex(key, "/")
	if slash < 0 {
		return ""
	}
	name := key[slash+1:]
	dot := strings.Index(name, ".")
	if dot < 0 {
		return name
	}
	return name[:dot]
}

// extractField reads one string field from raw JSON without full unmarshal.
func extractField(rawJSON []byte, field string) string {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(rawJSON, &m); err != nil {
		return ""
	}
	v, ok := m[field]
	if !ok {
		return ""
	}
	var s string
	if err := json.Unmarshal(v, &s); err != nil {
		return ""
	}
	return s
}
