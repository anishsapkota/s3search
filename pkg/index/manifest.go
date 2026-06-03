package index

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/anishsapkota/s3-search/pkg/store"
)

// SegmentMeta describes one segment listed in a manifest.
type SegmentMeta struct {
	ID       string `json:"id"`
	DocCount uint32 `json:"doc_count"`
	Bytes    int64  `json:"bytes"`
	// MinTS / MaxTS: unix milliseconds, 0 if unknown.
	MinTS     int64  `json:"min_ts,omitempty"`
	MaxTS     int64  `json:"max_ts,omitempty"`
	CreatedAt int64  `json:"created_at"`
	DelVersion int   `json:"del_version,omitempty"` // current .del version; 0 = no deletes
	Deleted   bool   `json:"deleted,omitempty"`
}

// Manifest lists all segments for an index.
type Manifest struct {
	Version  uint64        `json:"version"`
	IndexName string       `json:"index_name"`
	Segments []SegmentMeta `json:"segments"`
	CreatedAt int64        `json:"created_at"`
}

// ActiveSegments returns non-deleted segments.
func (m *Manifest) ActiveSegments() []SegmentMeta {
	var out []SegmentMeta
	for _, s := range m.Segments {
		if !s.Deleted {
			out = append(out, s)
		}
	}
	return out
}

// ManifestStore handles reading and publishing versioned manifests.
type ManifestStore struct {
	bs        store.BlobStore
	indexName string
}

func NewManifestStore(bs store.BlobStore, indexName string) *ManifestStore {
	return &ManifestStore{bs: bs, indexName: indexName}
}

func (ms *ManifestStore) manifestKey(version uint64) string {
	return fmt.Sprintf("%s/manifest-%06d.json", ms.indexName, version)
}

// Latest loads the highest-version manifest. Returns nil, nil if none exists.
func (ms *ManifestStore) Latest(ctx context.Context) (*Manifest, error) {
	prefix := ms.indexName + "/manifest-"
	keys, err := ms.bs.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}
	sort.Strings(keys)
	latestKey := keys[len(keys)-1]
	data, err := ms.bs.Get(ctx, latestKey)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("manifest parse %s: %w", latestKey, err)
	}
	return &m, nil
}

// publishNext writes manifest version = current+1 with conditional PUT.
// Returns ErrManifestConflict if another writer beat us.
func (ms *ManifestStore) publishNext(ctx context.Context, current *Manifest, segments []SegmentMeta) (*Manifest, error) {
	nextVersion := uint64(1)
	if current != nil {
		nextVersion = current.Version + 1
	}
	m := &Manifest{
		Version:   nextVersion,
		IndexName: ms.indexName,
		Segments:  segments,
		CreatedAt: time.Now().UnixMilli(),
	}
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	key := ms.manifestKey(nextVersion)
	err = ms.bs.Put(ctx, key, data, store.PutOpts{IfNoneMatch: "*", ContentType: "application/json"})
	if err != nil {
		var pf store.ErrPreconditionFailed
		if errors.As(err, &pf) {
			return nil, ErrManifestConflict{Version: nextVersion}
		}
		return nil, fmt.Errorf("publish manifest: %w", err)
	}
	return m, nil
}

// Publish appends newSegments to current and publishes a new manifest version.
func (ms *ManifestStore) Publish(ctx context.Context, current *Manifest, newSegments []SegmentMeta) (*Manifest, error) {
	var segs []SegmentMeta
	if current != nil {
		segs = append(segs, current.Segments...)
	}
	segs = append(segs, newSegments...)
	return ms.publishNext(ctx, current, segs)
}

// PublishWithRetry appends newSeg and retries on conflict.
func (ms *ManifestStore) PublishWithRetry(ctx context.Context, newSeg SegmentMeta, maxRetries int) (*Manifest, error) {
	for attempt := 0; attempt < maxRetries; attempt++ {
		current, err := ms.Latest(ctx)
		if err != nil {
			return nil, err
		}
		m, err := ms.Publish(ctx, current, []SegmentMeta{newSeg})
		if err == nil {
			return m, nil
		}
		var conflict ErrManifestConflict
		if !errors.As(err, &conflict) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("publish: exceeded %d retries", maxRetries)
}

// PublishTransform applies fn to the current segment list and publishes.
// fn receives a copy of current segments; its return value becomes the new list.
// Retries on conflict up to maxRetries times.
func (ms *ManifestStore) PublishTransform(ctx context.Context, fn func([]SegmentMeta) []SegmentMeta, maxRetries int) (*Manifest, error) {
	for attempt := 0; attempt < maxRetries; attempt++ {
		current, err := ms.Latest(ctx)
		if err != nil {
			return nil, err
		}
		var segs []SegmentMeta
		if current != nil {
			segs = make([]SegmentMeta, len(current.Segments))
			copy(segs, current.Segments)
		}
		newSegs := fn(segs)
		m, err := ms.publishNext(ctx, current, newSegs)
		if err == nil {
			return m, nil
		}
		var conflict ErrManifestConflict
		if !errors.As(err, &conflict) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("publishTransform: exceeded %d retries", maxRetries)
}

// ListAll returns all manifest versions sorted ascending.
func (ms *ManifestStore) ListAll(ctx context.Context) ([]*Manifest, error) {
	prefix := ms.indexName + "/manifest-"
	keys, err := ms.bs.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	manifests := make([]*Manifest, 0, len(keys))
	for _, k := range keys {
		data, err := ms.bs.Get(ctx, k)
		if err != nil {
			continue
		}
		var m Manifest
		if err := json.Unmarshal(data, &m); err != nil {
			continue
		}
		manifests = append(manifests, &m)
	}
	return manifests, nil
}

// ErrManifestConflict is returned when conditional PUT detects a concurrent writer.
type ErrManifestConflict struct{ Version uint64 }

func (e ErrManifestConflict) Error() string {
	return fmt.Sprintf("manifest conflict at version %d", e.Version)
}

// SegmentKey returns the S3 key for a segment file.
func SegmentKey(indexName, segID, ext string) string {
	return fmt.Sprintf("%s/segments/%s%s", indexName, segID, ext)
}

// IsManifestKey reports whether key looks like a manifest file.
func IsManifestKey(key string) bool {
	base := key[strings.LastIndex(key, "/")+1:]
	return strings.HasPrefix(base, "manifest-") && strings.HasSuffix(base, ".json")
}
