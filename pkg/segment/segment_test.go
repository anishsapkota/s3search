package segment_test

import (
	"testing"

	"github.com/anishsapkota/s3-search/pkg/docstore"
	"github.com/anishsapkota/s3-search/pkg/segment"
)

func TestBuildRoundTrip(t *testing.T) {
	fa := segment.NewFieldAccumulator("body")
	fa.AddToken(0, "hello", 0)
	fa.AddToken(0, "world", 1)
	fa.AddToken(1, "hello", 0)
	fa.AddToken(1, "foo", 1)
	fa.SetDoclen(0, 2)
	fa.SetDoclen(1, 2)

	ds, err := docstore.NewWriter()
	if err != nil {
		t.Fatal(err)
	}
	_ = ds.Add(0, []byte(`{"body":"hello world"}`))
	_ = ds.Add(1, []byte(`{"body":"hello foo"}`))

	result, err := segment.Build([]*segment.FieldAccumulator{fa}, ds, 2)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(result.SegBytes) == 0 {
		t.Error("empty seg bytes")
	}
	if len(result.HotcacheBytes) == 0 {
		t.Error("empty hotcache bytes")
	}

	hc, err := segment.DecodeHotcache(result.HotcacheBytes)
	if err != nil {
		t.Fatalf("decode hotcache: %v", err)
	}
	if hc.DocCount != 2 {
		t.Errorf("doc count: got %d want 2", hc.DocCount)
	}
	fhc := hc.Field("body")
	if fhc == nil {
		t.Fatal("field 'body' not in hotcache")
	}

	to, ok := fhc.LookupTerm("hello")
	if !ok {
		t.Fatal("term 'hello' not found in FST")
	}
	if to.PostLen == 0 {
		t.Error("postings length 0 for 'hello'")
	}
}

func TestFooterRoundTrip(t *testing.T) {
	f := segment.Footer{
		PostingsOffset:  100,
		PostingsLen:     200,
		PositionsOffset: 300,
		PositionsLen:    400,
		DocstoreOffset:  500,
		DocstoreLen:     600,
		DoclensOffset:   700,
		DoclensLen:      800,
		DocCount:        42,
	}
	encoded := segment.EncodeFooter(f)
	if len(encoded) != segment.FooterSize {
		t.Errorf("encoded footer len %d want %d", len(encoded), segment.FooterSize)
	}
	// DecodeFooter expects the full file tail, so wrap in a full-size slice.
	decoded, err := segment.DecodeFooter(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.PostingsOffset != f.PostingsOffset {
		t.Errorf("PostingsOffset: got %d want %d", decoded.PostingsOffset, f.PostingsOffset)
	}
	if decoded.DocCount != f.DocCount {
		t.Errorf("DocCount: got %d want %d", decoded.DocCount, f.DocCount)
	}
}
