package index

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/anishsapkota/s3-search/pkg/analyze"
	"github.com/anishsapkota/s3-search/pkg/obs"
	"github.com/anishsapkota/s3-search/pkg/schema"
	"github.com/anishsapkota/s3-search/pkg/store"
	"github.com/anishsapkota/s3-search/pkg/wal"
)

// FlushConfig controls memtable flush triggers.
type FlushConfig struct {
	MaxDocs  uint32
	MaxBytes int
	MaxAge   time.Duration
}

var DefaultFlushConfig = FlushConfig{
	MaxDocs:  100_000,
	MaxBytes: 128 * 1024 * 1024, // 128MB
	MaxAge:   60 * time.Second,
}

// Index manages ingest for a single named index.
type Index struct {
	name      string
	schema    *schema.Schema
	bs        store.BlobStore
	ms        *ManifestStore
	tokenizer analyze.Tokenizer
	walDir    string

	mu        sync.Mutex
	memtable  *Memtable
	wal       *wal.WAL
	lastFlush time.Time
	flushCfg  FlushConfig

	// Async flush state. flushing != nil iff a background flush goroutine
	// is running. flushDone is closed by that goroutine on completion.
	// flushOff is the WAL byte offset captured at swap; truncated up to
	// after the flush succeeds. flushErr surfaces async errors back to
	// the next synchronous caller of AddBatch/Add.
	flushing  *Memtable
	flushOff  int64
	flushDone chan struct{}
	flushErr  error
}

// Open opens (or attaches to) an existing index, replaying WAL if needed.
func Open(ctx context.Context, name string, sc *schema.Schema, bs store.BlobStore, walDir string, flushCfg FlushConfig) (*Index, error) {
	ms := NewManifestStore(bs, name)
	tok := analyze.StandardTokenizer{}

	idx := &Index{
		name:      name,
		schema:    sc,
		bs:        bs,
		ms:        ms,
		tokenizer: tok,
		walDir:    walDir,
		flushCfg:  flushCfg,
		lastFlush: time.Now(),
	}

	w, err := wal.Open(walDir)
	if err != nil {
		return nil, fmt.Errorf("index %s: open wal: %w", name, err)
	}
	idx.wal = w

	mt, err := NewMemtable(sc, tok)
	if err != nil {
		return nil, err
	}
	idx.memtable = mt

	// Replay WAL if non-empty.
	empty, err := w.IsEmpty()
	if err != nil {
		return nil, err
	}
	if !empty {
		slog.Info("replaying WAL", "index", name)
		if err := w.Replay(func(rawJSON []byte) error {
			return mt.Add(rawJSON)
		}); err != nil {
			return nil, fmt.Errorf("index %s: wal replay: %w", name, err)
		}
		obs.WALReplaysTotal.WithLabelValues(name).Inc()
		obs.WALReplayDocs.WithLabelValues(name).Observe(float64(mt.DocCount()))
		slog.Info("WAL replay done", "index", name, "docs", mt.DocCount())
	}

	// Persist schema on S3 (idempotent).
	if err := idx.persistSchema(ctx, sc); err != nil {
		return nil, err
	}

	return idx, nil
}

// Add appends a document to WAL and memtable.
func (idx *Index) Add(ctx context.Context, rawJSON []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if err := idx.consumeFlushErrLocked(); err != nil {
		return err
	}

	if err := idx.wal.Append(rawJSON); err != nil {
		return fmt.Errorf("wal append: %w", err)
	}
	if err := idx.wal.Flush(); err != nil {
		return fmt.Errorf("wal flush: %w", err)
	}
	if err := idx.memtable.Add(rawJSON); err != nil {
		return fmt.Errorf("memtable add: %w", err)
	}

	if idx.shouldFlush() && idx.flushing == nil {
		return idx.triggerAsyncFlushLocked()
	}
	return nil
}

// FlushNow forces a synchronous flush regardless of trigger conditions.
// Waits for any in-flight async flush before snapshotting.
func (idx *Index) FlushNow(ctx context.Context) error {
	idx.mu.Lock()
	idx.waitForFlushLocked()
	if err := idx.consumeFlushErrLocked(); err != nil {
		idx.mu.Unlock()
		return err
	}
	if idx.memtable.DocCount() == 0 {
		idx.mu.Unlock()
		return nil
	}

	// Snapshot + swap (same as triggerAsyncFlushLocked) but run inline.
	walOff, err := idx.wal.Position()
	if err != nil {
		idx.mu.Unlock()
		return err
	}
	snap := idx.memtable
	newMt, err := NewMemtable(idx.schema, idx.tokenizer)
	if err != nil {
		idx.mu.Unlock()
		return err
	}
	idx.memtable = newMt
	idx.mu.Unlock()

	if err := idx.runFlush(ctx, snap); err != nil {
		return err
	}
	idx.mu.Lock()
	if terr := idx.wal.TruncatePrefix(walOff); terr != nil {
		slog.Warn("wal truncate failed", "err", terr)
	}
	idx.lastFlush = time.Now()
	idx.mu.Unlock()
	return nil
}

// waitForFlushLocked releases the lock and waits for any in-flight async
// flush to complete, then re-acquires the lock. Caller must hold idx.mu
// on entry and will hold it on return.
func (idx *Index) waitForFlushLocked() {
	for idx.flushing != nil {
		ch := idx.flushDone
		idx.mu.Unlock()
		<-ch
		idx.mu.Lock()
	}
}

// consumeFlushErrLocked returns and clears any error from the last async flush.
// Caller must hold idx.mu.
func (idx *Index) consumeFlushErrLocked() error {
	if idx.flushErr != nil {
		err := idx.flushErr
		idx.flushErr = nil
		return err
	}
	return nil
}

// triggerAsyncFlushLocked snapshots the current memtable, swaps in a fresh one,
// and spawns a background flush goroutine. Caller must hold idx.mu and have
// already verified idx.flushing == nil.
func (idx *Index) triggerAsyncFlushLocked() error {
	walOff, err := idx.wal.Position()
	if err != nil {
		return fmt.Errorf("wal position: %w", err)
	}
	snap := idx.memtable
	newMt, err := NewMemtable(idx.schema, idx.tokenizer)
	if err != nil {
		return err
	}
	idx.memtable = newMt
	idx.flushing = snap
	idx.flushOff = walOff
	idx.flushDone = make(chan struct{})
	go idx.flushAsync(context.Background(), snap, walOff, idx.flushDone)
	return nil
}

// flushAsync runs in a background goroutine. Builds + uploads the snapshot,
// then truncates the WAL prefix under the index lock.
func (idx *Index) flushAsync(ctx context.Context, snap *Memtable, walOff int64, done chan struct{}) {
	defer close(done)
	err := idx.runFlush(ctx, snap)
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if err != nil {
		slog.Error("async flush failed", "index", idx.name, "err", err)
		idx.flushErr = err
	} else {
		if terr := idx.wal.TruncatePrefix(walOff); terr != nil {
			slog.Warn("wal truncate failed after async flush", "err", terr)
		}
		idx.lastFlush = time.Now()
	}
	idx.flushing = nil
	idx.flushDone = nil
}

func (idx *Index) shouldFlush() bool {
	m := idx.memtable
	if m.DocCount() >= idx.flushCfg.MaxDocs {
		return true
	}
	if m.EstimatedBytes() >= idx.flushCfg.MaxBytes {
		return true
	}
	if time.Since(idx.lastFlush) >= idx.flushCfg.MaxAge {
		return true
	}
	return false
}

// runFlush builds + uploads a snapshot to S3 + publishes the manifest.
// Does NOT swap the active memtable or touch the WAL — that is the caller's
// responsibility. Safe to call without holding idx.mu (snap is immutable).
func (idx *Index) runFlush(ctx context.Context, snap *Memtable) error {
	docCount := snap.DocCount()
	slog.Info("flushing segment", "index", idx.name, "docs", docCount)
	t0 := time.Now()

	result, err := snap.Build()
	if err != nil {
		obs.FlushTotal.WithLabelValues(idx.name, "err").Inc()
		return fmt.Errorf("flush: build segment: %w", err)
	}

	segID := newSegmentID()
	segKey := SegmentKey(idx.name, segID, ".seg")
	hcKey := SegmentKey(idx.name, segID, ".hotcache")

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := idx.bs.Put(gctx, segKey, result.SegBytes, store.PutOpts{}); err != nil {
			return fmt.Errorf("flush: upload seg: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		if err := idx.bs.Put(gctx, hcKey, result.HotcacheBytes, store.PutOpts{}); err != nil {
			return fmt.Errorf("flush: upload hotcache: %w", err)
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		obs.FlushTotal.WithLabelValues(idx.name, "err").Inc()
		return err
	}

	meta := SegmentMeta{
		ID:        segID,
		DocCount:  result.DocCount,
		Bytes:     int64(len(result.SegBytes)),
		CreatedAt: time.Now().UnixMilli(),
	}
	if _, err := idx.ms.PublishWithRetry(ctx, meta, 5); err != nil {
		obs.FlushTotal.WithLabelValues(idx.name, "err").Inc()
		return fmt.Errorf("flush: publish manifest: %w", err)
	}

	obs.FlushTotal.WithLabelValues(idx.name, "ok").Inc()
	obs.FlushDuration.WithLabelValues(idx.name).Observe(time.Since(t0).Seconds())
	obs.FlushDocCount.WithLabelValues(idx.name).Observe(float64(docCount))
	slog.Info("segment flushed", "index", idx.name, "seg", segID, "docs", result.DocCount, "dur", time.Since(t0))
	return nil
}

func (idx *Index) persistSchema(ctx context.Context, sc *schema.Schema) error {
	key := idx.name + "/schema.json"
	data, err := schema.Marshal(sc)
	if err != nil {
		return err
	}
	// Best-effort: ignore precondition failure (schema already exists).
	err = idx.bs.Put(ctx, key, data, store.PutOpts{
		IfNoneMatch: "*",
		ContentType: "application/json",
	})
	if err != nil {
		var pf store.ErrPreconditionFailed
		if isAs(err, &pf) {
			return nil // already exists
		}
		return err
	}
	return nil
}

// Close flushes pending data (waiting for any in-flight async flush)
// and closes resources.
func (idx *Index) Close(ctx context.Context) error {
	if err := idx.FlushNow(ctx); err != nil {
		slog.Warn("close flush failed", "err", err)
	}
	// FlushNow already waited for prior async; no in-flight at this point.
	return idx.wal.Close()
}

// LoadSchema fetches the schema for an index from S3.
func LoadSchema(ctx context.Context, bs store.BlobStore, indexName string) (*schema.Schema, error) {
	key := indexName + "/schema.json"
	data, err := bs.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("load schema %s: %w", indexName, err)
	}
	return schema.Parse(data)
}

func (idx *Index) Name() string                  { return idx.name }
func (idx *Index) Schema() *schema.Schema         { return idx.schema }
func (idx *Index) ManifestStore() *ManifestStore  { return idx.ms }
func (idx *Index) BlobStore() store.BlobStore     { return idx.bs }

// NewCompactor returns a Compactor for this index.
func (idx *Index) NewCompactor() *Compactor {
	return NewCompactor(idx.bs, idx.ms, idx.schema, idx.name)
}

func (idx *Index) AddBatch(ctx context.Context, docs []json.RawMessage) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Backpressure: if a prior async flush is still in flight AND we'd trigger
	// another, block this writer until prior completes. Single-writer model.
	if idx.flushing != nil && idx.wouldTriggerAfterLocked(len(docs)) {
		idx.waitForFlushLocked()
	}
	if err := idx.consumeFlushErrLocked(); err != nil {
		return err
	}

	for _, doc := range docs {
		if err := idx.wal.Append(doc); err != nil {
			// Discard the partially-written batch from the WAL buffer so the
			// next successful AddBatch doesn't inherit stale bytes.
			idx.wal.DiscardBuffer()
			return err
		}
		if err := idx.memtable.Add(doc); err != nil {
			idx.wal.DiscardBuffer()
			return err
		}
	}
	if err := idx.wal.Flush(); err != nil {
		return err
	}
	if idx.shouldFlush() && idx.flushing == nil {
		return idx.triggerAsyncFlushLocked()
	}
	return nil
}

// wouldTriggerAfterLocked reports whether adding `n` more docs would push the
// memtable past any flush trigger. Conservative — uses current EstimatedBytes
// without projecting growth. Caller must hold idx.mu.
func (idx *Index) wouldTriggerAfterLocked(n int) bool {
	m := idx.memtable
	if uint32(int(m.DocCount())+n) >= idx.flushCfg.MaxDocs {
		return true
	}
	if m.EstimatedBytes() >= idx.flushCfg.MaxBytes {
		return true
	}
	if time.Since(idx.lastFlush) >= idx.flushCfg.MaxAge {
		return true
	}
	return false
}
