package index_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/anishsapkota/s3-search/internal/testutil"
	"github.com/anishsapkota/s3-search/pkg/index"
	"github.com/anishsapkota/s3-search/pkg/query"
	"github.com/anishsapkota/s3-search/pkg/schema"
	"github.com/anishsapkota/s3-search/pkg/search"
)

// edifactDoc wraps an EDIFACT interchange as a searchable JSON document.
// raw = full EDIFACT text; message_type, sender, receiver, reference = parsed fields.
type edifactDoc struct {
	Raw         string `json:"raw"`
	MessageType string `json:"message_type"`
	Sender      string `json:"sender"`
	Receiver    string `json:"receiver"`
	Reference   string `json:"reference"`
}

func mustJSON(v interface{}) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func edifactSchema() *schema.Schema {
	return &schema.Schema{
		Fields: []schema.Field{
			{Name: "raw", Type: schema.FieldTypeText, Store: true},
			{Name: "message_type", Type: schema.FieldTypeKeyword, Store: true},
			{Name: "sender", Type: schema.FieldTypeKeyword, Store: true},
			{Name: "receiver", Type: schema.FieldTypeKeyword, Store: true},
			{Name: "reference", Type: schema.FieldTypeKeyword, Store: true},
		},
	}
}

func openEdifactIndex(t *testing.T, ctx context.Context, bs *testutil.MemStore) *index.Index {
	t.Helper()
	idx, err := index.Open(ctx, "edifact", edifactSchema(), bs, t.TempDir(), index.FlushConfig{
		MaxDocs:  1000,
		MaxBytes: 1024 * 1024,
		MaxAge:   60,
	})
	if err != nil {
		t.Fatalf("open edifact index: %v", err)
	}
	return idx
}

func TestEdifactSearch(t *testing.T) {
	ctx := context.Background()
	bs := testutil.NewMemStore()
	idx := openEdifactIndex(t, ctx, bs)
	defer idx.Close(ctx)

	const utilmdMsg = "UNA:+.? '\nUNB+UNOC:3+9901028000009:500+9907813000006:500+260521:1104+2614100428469'\nUNH+1+UTILMD:D:11A:UN:S2.1'\nBGM+Z07+26141004284691'\nDTM+137:202605211104?+00:303'\nNAD+MS+9901028000009::293'\nNAD+MR+9907813000006::293'\nIDE+24+BEL260521130434000000000000000qc001'\nDTM+159:202605312200?+00:303'\nLOC+Z15+DE0000000102811YV00000178137P0NZR'\nRFF+Z13:55063'\nUNT+10+1'\nUNZ+1+2614100428469'"

	docs := []json.RawMessage{
		mustJSON(edifactDoc{
			Raw:         utilmdMsg,
			MessageType: "UTILMD",
			Sender:      "9901028000009",
			Receiver:    "9907813000006",
			Reference:   "2614100428469",
		}),
		// unrelated doc — should NOT appear in EDIFACT-specific queries
		mustJSON(edifactDoc{
			Raw:         "regular log entry: service started successfully",
			MessageType: "NONE",
			Sender:      "system",
			Receiver:    "admin",
			Reference:   "0",
		}),
	}

	if err := idx.AddBatch(ctx, docs); err != nil {
		t.Fatalf("add batch: %v", err)
	}
	if err := idx.FlushNow(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	ms := index.NewManifestStore(bs, "edifact")
	searcher := search.New(bs, 16)

	tests := []struct {
		name      string
		queryStr  string
		wantHits  int    // minimum hits expected
		wantFirst string // substring expected in first hit's raw field
	}{
		{
			name:      "message type UTILMD",
			queryStr:  "UTILMD",
			wantHits:  1,
			wantFirst: "UTILMD",
		},
		{
			name:      "sender ID",
			queryStr:  "9901028000009",
			wantHits:  1,
			wantFirst: "9901028000009",
		},
		{
			name:      "reference number RFF",
			queryStr:  "55063",
			wantHits:  1,
			wantFirst: "55063",
		},
		{
			name:      "location code",
			queryStr:  "DE0000000102811YV00000178137P0NZR",
			wantHits:  1,
			wantFirst: "DE0000000102811YV00000178137P0NZR",
		},
		{
			name:      "interchange ref in UNB",
			queryStr:  "2614100428469",
			wantHits:  1,
			wantFirst: "2614100428469",
		},
		{
			name:      "segment identifier UNH",
			queryStr:  "UNH",
			wantHits:  1,
			wantFirst: "UNH",
		},
		{
			name:      "unrelated term not in EDIFACT",
			queryStr:  "xyzzy99nonexistent",
			wantHits:  0,
			wantFirst: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := query.ParseQueryString(tt.queryStr)
			if err != nil {
				t.Fatalf("parse query %q: %v", tt.queryStr, err)
			}
			resp, err := searcher.Search(ctx, ms, search.Request{
				Index: "edifact",
				Query: q,
				Size:  10,
			})
			if err != nil {
				t.Fatalf("search %q: %v", tt.queryStr, err)
			}
			if tt.wantHits == 0 {
				if resp.TotalHits != 0 {
					t.Errorf("query %q: expected 0 hits, got %d", tt.queryStr, resp.TotalHits)
				}
				return
			}
			if resp.TotalHits < uint64(tt.wantHits) {
				t.Errorf("query %q: expected >= %d hits, got %d", tt.queryStr, tt.wantHits, resp.TotalHits)
				return
			}
			if len(resp.Hits) == 0 {
				t.Errorf("query %q: no hits returned", tt.queryStr)
				return
			}
			if tt.wantFirst != "" {
				var doc edifactDoc
				if err := json.Unmarshal(resp.Hits[0].Source, &doc); err != nil {
					t.Fatalf("unmarshal source: %v", err)
				}
				if doc.Raw == "" && doc.MessageType == "" {
					t.Errorf("query %q: first hit has empty source", tt.queryStr)
				}
			}
		})
	}
}

func TestIngestAndSearch(t *testing.T) {
	ctx := context.Background()
	bs := testutil.NewMemStore()

	sc := &schema.Schema{
		Fields: []schema.Field{
			{Name: "message", Type: schema.FieldTypeText, Store: true},
			{Name: "level", Type: schema.FieldTypeKeyword, Store: true},
		},
	}

	idx, err := index.Open(ctx, "test", sc, bs, t.TempDir(), index.FlushConfig{
		MaxDocs:  10,
		MaxBytes: 1024 * 1024,
		MaxAge:   60,
	})
	if err != nil {
		t.Fatalf("open index: %v", err)
	}
	defer idx.Close(ctx)

	docs := []json.RawMessage{
		json.RawMessage(`{"message":"database connection failed","level":"ERROR"}`),
		json.RawMessage(`{"message":"user login successful","level":"INFO"}`),
		json.RawMessage(`{"message":"disk space low warning","level":"WARN"}`),
	}
	if err := idx.AddBatch(ctx, docs); err != nil {
		t.Fatalf("add batch: %v", err)
	}
	if err := idx.FlushNow(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	ms := index.NewManifestStore(bs, "test")
	manifest, err := ms.Latest(ctx)
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if manifest == nil {
		t.Fatal("manifest is nil after flush")
	}
	if len(manifest.ActiveSegments()) == 0 {
		t.Fatal("no segments in manifest")
	}

	searcher := search.New(bs, 16)
	q, err := query.ParseQueryString("database")
	if err != nil {
		t.Fatalf("parse query: %v", err)
	}
	resp, err := searcher.Search(ctx, ms, search.Request{
		Index: "test",
		Query: q,
		Size:  10,
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if resp.TotalHits == 0 {
		t.Error("expected hits for 'database', got 0")
	}
	if len(resp.Hits) == 0 {
		t.Error("expected non-empty hits")
	}
}
