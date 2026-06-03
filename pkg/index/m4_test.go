package index_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/anishsapkota/s3-search/internal/testutil"
	"github.com/anishsapkota/s3-search/pkg/analyze"
	"github.com/anishsapkota/s3-search/pkg/index"
	"github.com/anishsapkota/s3-search/pkg/query"
	"github.com/anishsapkota/s3-search/pkg/schema"
	"github.com/anishsapkota/s3-search/pkg/search"
)

func logSchema() *schema.Schema {
	return &schema.Schema{
		Fields: []schema.Field{
			{Name: "message", Type: schema.FieldTypeText, Store: true},
			{Name: "level", Type: schema.FieldTypeKeyword, Store: true},
		},
	}
}

func openIdx(t *testing.T, ctx context.Context, bs *testutil.MemStore, name string) *index.Index {
	t.Helper()
	idx, err := index.Open(ctx, name, logSchema(), bs, t.TempDir(), index.FlushConfig{
		MaxDocs:  1000,
		MaxBytes: 1024 * 1024,
		MaxAge:   time.Hour,
	})
	if err != nil {
		t.Fatalf("open index: %v", err)
	}
	return idx
}

func ingestAndFlush(t *testing.T, ctx context.Context, idx *index.Index, docs []json.RawMessage) {
	t.Helper()
	if err := idx.AddBatch(ctx, docs); err != nil {
		t.Fatalf("add batch: %v", err)
	}
	if err := idx.FlushNow(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func searchHits(t *testing.T, ctx context.Context, bs *testutil.MemStore, indexName, queryStr string) uint64 {
	t.Helper()
	ms := index.NewManifestStore(bs, indexName)
	s := search.New(bs, 16)
	q, err := query.ParseQueryString(queryStr)
	if err != nil {
		t.Fatalf("parse query %q: %v", queryStr, err)
	}
	resp, err := s.Search(ctx, ms, search.Request{Index: indexName, Query: q, Size: 100})
	if err != nil {
		t.Fatalf("search %q: %v", queryStr, err)
	}
	return resp.TotalHits
}

// --- TestDeleteByQuery ---

func TestDeleteByQuery(t *testing.T) {
	ctx := context.Background()
	bs := testutil.NewMemStore()
	idx := openIdx(t, ctx, bs, "del-test")
	defer idx.Close(ctx)

	docs := []json.RawMessage{
		json.RawMessage(`{"message":"error in payment service","level":"ERROR"}`),
		json.RawMessage(`{"message":"error in auth service","level":"ERROR"}`),
		json.RawMessage(`{"message":"health check passed","level":"INFO"}`),
		json.RawMessage(`{"message":"user login successful","level":"INFO"}`),
	}
	ingestAndFlush(t, ctx, idx, docs)

	// Verify all 4 docs are searchable before delete.
	if got := searchHits(t, ctx, bs, "del-test", "error"); got != 2 {
		t.Fatalf("before delete: expected 2 hits for 'error', got %d", got)
	}

	// Delete all ERROR docs.
	ms := index.NewManifestStore(bs, "del-test")
	q, _ := query.ParseQueryString("error")
	deleted, err := index.DeleteByQuery(ctx, bs, ms, "del-test", q, analyze.StandardTokenizer{})
	if err != nil {
		t.Fatalf("delete by query: %v", err)
	}
	if deleted != 2 {
		t.Errorf("expected 2 deleted, got %d", deleted)
	}

	// After delete: 'error' should return 0 hits.
	if got := searchHits(t, ctx, bs, "del-test", "error"); got != 0 {
		t.Errorf("after delete: expected 0 hits for 'error', got %d", got)
	}
	// Non-deleted docs still searchable.
	if got := searchHits(t, ctx, bs, "del-test", "health"); got != 1 {
		t.Errorf("after delete: expected 1 hit for 'health', got %d", got)
	}
}

// TestDeleteByQueryIdempotent: re-deleting already-deleted docs is safe.
func TestDeleteByQueryIdempotent(t *testing.T) {
	ctx := context.Background()
	bs := testutil.NewMemStore()
	idx := openIdx(t, ctx, bs, "del-idem")
	defer idx.Close(ctx)

	ingestAndFlush(t, ctx, idx, []json.RawMessage{
		json.RawMessage(`{"message":"timeout connecting to db","level":"ERROR"}`),
	})

	ms := index.NewManifestStore(bs, "del-idem")
	q, _ := query.ParseQueryString("timeout")
	tok := analyze.StandardTokenizer{}

	n1, err := index.DeleteByQuery(ctx, bs, ms, "del-idem", q, tok)
	if err != nil {
		t.Fatal(err)
	}
	n2, err := index.DeleteByQuery(ctx, bs, ms, "del-idem", q, tok)
	if err != nil {
		t.Fatal(err)
	}
	if n1 != 1 {
		t.Errorf("first delete: want 1, got %d", n1)
	}
	if n2 != 0 {
		t.Errorf("second delete: want 0 (already deleted), got %d", n2)
	}
}

// --- TestCompaction ---

func TestCompaction(t *testing.T) {
	ctx := context.Background()
	bs := testutil.NewMemStore()

	sc := logSchema()
	// Use a small flush threshold so each AddBatch produces a separate segment.
	flushCfg := index.FlushConfig{
		MaxDocs:  2,
		MaxBytes: 1024 * 1024,
		MaxAge:   time.Hour,
	}
	idx, err := index.Open(ctx, "compact-test", sc, bs, t.TempDir(), flushCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close(ctx)

	// Ingest 8 docs in 4 batches of 2 → 4 segments (MaxDocs=2 triggers flush).
	batches := [][]json.RawMessage{
		{
			json.RawMessage(`{"message":"connection refused port 5432","level":"ERROR"}`),
			json.RawMessage(`{"message":"retry attempt 1","level":"WARN"}`),
		},
		{
			json.RawMessage(`{"message":"cache miss user profile","level":"DEBUG"}`),
			json.RawMessage(`{"message":"login success alice","level":"INFO"}`),
		},
		{
			json.RawMessage(`{"message":"disk usage warning 85 percent","level":"WARN"}`),
			json.RawMessage(`{"message":"connection refused port 6379","level":"ERROR"}`),
		},
		{
			json.RawMessage(`{"message":"payment processed order 42","level":"INFO"}`),
			json.RawMessage(`{"message":"rate limit exceeded","level":"WARN"}`),
		},
	}
	for _, batch := range batches {
		if err := idx.AddBatch(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	// Flush any remaining memtable.
	if err := idx.FlushNow(ctx); err != nil {
		t.Fatal(err)
	}

	ms := index.NewManifestStore(bs, "compact-test")
	manifest, err := ms.Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	preCompactSegs := len(manifest.ActiveSegments())
	if preCompactSegs < 2 {
		t.Fatalf("expected multiple segments before compaction, got %d", preCompactSegs)
	}

	// Record hit counts before compaction.
	hitsBefore := map[string]uint64{
		"connection": searchHits(t, ctx, bs, "compact-test", "connection"),
		"error":      searchHits(t, ctx, bs, "compact-test", "error"),
		"login":      searchHits(t, ctx, bs, "compact-test", "login"),
	}
	if hitsBefore["connection"] == 0 {
		t.Fatal("expected hits for 'connection' before compaction")
	}

	// Run compaction.
	compactor := idx.NewCompactor()
	if err := compactor.CompactOnce(ctx); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// Verify query results are equivalent after compaction.
	for term, before := range hitsBefore {
		after := searchHits(t, ctx, bs, "compact-test", term)
		if after != before {
			t.Errorf("term %q: hits before=%d after=%d (mismatch)", term, before, after)
		}
	}

	// Manifest should have more total entries (source marked deleted + new merged).
	manifestPost, _ := ms.Latest(ctx)
	activePost := len(manifestPost.ActiveSegments())
	if activePost >= preCompactSegs {
		t.Errorf("expected fewer active segments after compaction: before=%d after=%d", preCompactSegs, activePost)
	}
}

// TestCompactionPreservesDeletes: deleted docs should remain absent after compaction.
func TestCompactionPreservesDeletes(t *testing.T) {
	ctx := context.Background()
	bs := testutil.NewMemStore()

	flushCfg := index.FlushConfig{MaxDocs: 2, MaxBytes: 1024 * 1024, MaxAge: time.Hour}
	idx, err := index.Open(ctx, "compact-del", logSchema(), bs, t.TempDir(), flushCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close(ctx)

	// Two batches → two segments.
	if err := idx.AddBatch(ctx, []json.RawMessage{
		json.RawMessage(`{"message":"fatal crash detected","level":"ERROR"}`),
		json.RawMessage(`{"message":"service started ok","level":"INFO"}`),
	}); err != nil {
		t.Fatal(err)
	}
	if err := idx.AddBatch(ctx, []json.RawMessage{
		json.RawMessage(`{"message":"another fatal error occurred","level":"ERROR"}`),
		json.RawMessage(`{"message":"backup complete","level":"INFO"}`),
	}); err != nil {
		t.Fatal(err)
	}
	if err := idx.FlushNow(ctx); err != nil {
		t.Fatal(err)
	}

	// Delete ERROR docs.
	ms := index.NewManifestStore(bs, "compact-del")
	q, _ := query.ParseQueryString("fatal")
	deleted, err := index.DeleteByQuery(ctx, bs, ms, "compact-del", q, analyze.StandardTokenizer{})
	if err != nil {
		t.Fatal(err)
	}
	if deleted == 0 {
		t.Fatal("expected to delete at least 1 doc")
	}

	// Compact.
	if err := idx.NewCompactor().CompactOnce(ctx); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// Deleted docs must still be gone after compaction.
	if got := searchHits(t, ctx, bs, "compact-del", "fatal"); got != 0 {
		t.Errorf("after compact: expected 0 hits for 'fatal', got %d", got)
	}
	// Non-deleted docs survive.
	if got := searchHits(t, ctx, bs, "compact-del", "service"); got == 0 {
		t.Error("after compact: expected hits for 'service', got 0")
	}
}

// --- TestGC ---

func TestGC(t *testing.T) {
	ctx := context.Background()
	bs := testutil.NewMemStore()

	flushCfg := index.FlushConfig{MaxDocs: 2, MaxBytes: 1024 * 1024, MaxAge: time.Hour}
	idx, err := index.Open(ctx, "gc-test", logSchema(), bs, t.TempDir(), flushCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close(ctx)

	// Produce 4 segments → compact → 1 merged segment + 4 deleted source segments.
	for i := 0; i < 4; i++ {
		if err := idx.AddBatch(ctx, []json.RawMessage{
			json.RawMessage(`{"message":"log entry alpha","level":"INFO"}`),
			json.RawMessage(`{"message":"log entry beta","level":"INFO"}`),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := idx.FlushNow(ctx); err != nil {
		t.Fatal(err)
	}

	compactor := idx.NewCompactor()
	if err := compactor.CompactOnce(ctx); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// Count segment objects before GC.
	keys, _ := bs.List(ctx, "gc-test/segments/")
	segsBefore := countSegFiles(keys)

	// Run GC keeping only the 2 most recent manifests.
	if err := compactor.GC(ctx, 2); err != nil {
		t.Fatalf("gc: %v", err)
	}

	// Count after GC — orphan source segments should be deleted.
	keys, _ = bs.List(ctx, "gc-test/segments/")
	segsAfter := countSegFiles(keys)
	if segsAfter >= segsBefore {
		t.Errorf("expected fewer segment files after GC: before=%d after=%d", segsBefore, segsAfter)
	}

	// Search still works after GC.
	if got := searchHits(t, ctx, bs, "gc-test", "alpha"); got == 0 {
		t.Error("expected hits for 'alpha' after GC")
	}
}

func countSegFiles(keys []string) int {
	n := 0
	for _, k := range keys {
		if len(k) > 4 && k[len(k)-4:] == ".seg" {
			n++
		}
	}
	return n
}
