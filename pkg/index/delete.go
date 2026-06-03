package index

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/RoaringBitmap/roaring"

	"github.com/anishsapkota/s3-search/pkg/analyze"
	"github.com/anishsapkota/s3-search/pkg/query"
	"github.com/anishsapkota/s3-search/pkg/segment"
	"github.com/anishsapkota/s3-search/pkg/store"
)

// delKey returns the S3 key for a segment's delete bitmap at the given version.
func delKey(indexName, segID string, version int) string {
	return fmt.Sprintf("%s/segments/%s.del.%d", indexName, segID, version)
}

// WriteDelBitmap serializes bm and writes it as .del.{version} for segID.
func WriteDelBitmap(ctx context.Context, bs store.BlobStore, indexName, segID string, version int, bm *roaring.Bitmap) error {
	data, err := bm.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal del bitmap: %w", err)
	}
	key := delKey(indexName, segID, version)
	return bs.Put(ctx, key, data, store.PutOpts{ContentType: "application/octet-stream"})
}

// ReadDelBitmap fetches the .del.{version} bitmap for segID. Returns nil if version == 0 or not found.
func ReadDelBitmap(ctx context.Context, bs store.BlobStore, indexName, segID string, version int) (*roaring.Bitmap, error) {
	if version == 0 {
		return nil, nil
	}
	data, err := bs.Get(ctx, delKey(indexName, segID, version))
	if err != nil {
		var nf store.ErrNotFound
		if errors.As(err, &nf) {
			return nil, nil
		}
		return nil, err
	}
	bm := roaring.New()
	if err := bm.UnmarshalBinary(data); err != nil {
		return nil, fmt.Errorf("unmarshal del bitmap: %w", err)
	}
	return bm, nil
}

// DeleteByQuery finds all docs matching q across all active segments and marks them deleted.
// Returns total number of newly deleted docs.
func DeleteByQuery(
	ctx context.Context,
	bs store.BlobStore,
	ms *ManifestStore,
	indexName string,
	q *query.Node,
	tok analyze.Tokenizer,
) (uint64, error) {
	manifest, err := ms.Latest(ctx)
	if err != nil {
		return 0, err
	}
	if manifest == nil {
		return 0, nil
	}

	type segDel struct {
		segID      string
		newDelBM   *roaring.Bitmap
		newVersion int
	}
	var updates []segDel
	totalDeleted := uint64(0)

	for _, meta := range manifest.ActiveSegments() {
		// Load existing del bitmap.
		existing, err := ReadDelBitmap(ctx, bs, indexName, meta.ID, meta.DelVersion)
		if err != nil {
			return totalDeleted, fmt.Errorf("read del bitmap seg %s: %w", meta.ID, err)
		}

		// Load hotcache.
		hcKey := SegmentKey(indexName, meta.ID, ".hotcache")
		hcData, err := bs.Get(ctx, hcKey)
		if err != nil {
			return totalDeleted, fmt.Errorf("fetch hotcache %s: %w", hcKey, err)
		}
		hc, err := segment.DecodeHotcache(hcData)
		if err != nil {
			return totalDeleted, fmt.Errorf("decode hotcache %s: %w", hcKey, err)
		}

		// Find matching docIDs in this segment.
		segKey := SegmentKey(indexName, meta.ID, ".seg")
		newMatches, err := query.SearchDocIDs(ctx, bs, segKey, hc, q, tok, existing)
		if err != nil {
			return totalDeleted, fmt.Errorf("search docIDs seg %s: %w", meta.ID, err)
		}
		if newMatches.IsEmpty() {
			continue
		}

		// Merge with existing deletes.
		merged := roaring.New()
		if existing != nil {
			merged.Or(existing)
		}
		merged.Or(newMatches)

		newVersion := meta.DelVersion + 1
		updates = append(updates, segDel{
			segID:      meta.ID,
			newDelBM:   merged,
			newVersion: newVersion,
		})
		totalDeleted += newMatches.GetCardinality()
	}

	if len(updates) == 0 {
		return 0, nil
	}

	// Write del bitmaps before updating manifest (so readers never see stale state).
	for _, u := range updates {
		if err := WriteDelBitmap(ctx, bs, indexName, u.segID, u.newVersion, u.newDelBM); err != nil {
			return totalDeleted, fmt.Errorf("write del bitmap seg %s: %w", u.segID, err)
		}
	}

	// Publish manifest update: bump del_version for affected segments.
	buildUpdateMap := func() map[string]int {
		m := make(map[string]int, len(updates))
		for _, u := range updates {
			m[u.segID] = u.newVersion
		}
		return m
	}()

	if _, err := ms.PublishTransform(ctx, func(segs []SegmentMeta) []SegmentMeta {
		for i := range segs {
			if v, ok := buildUpdateMap[segs[i].ID]; ok {
				segs[i].DelVersion = v
			}
		}
		return segs
	}, 5); err != nil {
		return totalDeleted, fmt.Errorf("publish del manifest: %w", err)
	}

	slog.Info("delete by query done", "index", indexName, "deleted", totalDeleted)
	return totalDeleted, nil
}
