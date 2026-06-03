package index

import (
	"fmt"
	"strings"

	"github.com/valyala/fastjson"

	"github.com/anishsapkota/s3-search/pkg/analyze"
	"github.com/anishsapkota/s3-search/pkg/docstore"
	"github.com/anishsapkota/s3-search/pkg/schema"
	"github.com/anishsapkota/s3-search/pkg/segment"
)

// Shared parser pool: fastjson parsers are pooled to avoid per-doc allocation.
var parserPool fastjson.ParserPool

// Memtable accumulates documents in memory before flushing to a segment.
type Memtable struct {
	schema    *schema.Schema
	tokenizer analyze.Tokenizer
	fields    map[string]*segment.FieldAccumulator
	ds        *docstore.Writer
	docCount  uint32
	rawBytes  int // cumulative raw JSON bytes added (for accurate flush trigger)
}

func NewMemtable(sc *schema.Schema, tok analyze.Tokenizer) (*Memtable, error) {
	ds, err := docstore.NewWriter()
	if err != nil {
		return nil, err
	}
	fields := make(map[string]*segment.FieldAccumulator)
	for _, f := range sc.Fields {
		if f.Type == schema.FieldTypeText || f.Type == schema.FieldTypeKeyword {
			fields[f.Name] = segment.NewFieldAccumulator(f.Name)
		}
	}
	return &Memtable{
		schema:    sc,
		tokenizer: tok,
		fields:    fields,
		ds:        ds,
	}, nil
}

// Add indexes one document. rawJSON must be a valid JSON object.
func (m *Memtable) Add(rawJSON []byte) error {
	p := parserPool.Get()
	defer parserPool.Put(p)

	v, err := p.ParseBytes(rawJSON)
	if err != nil {
		return fmt.Errorf("memtable: invalid JSON: %w", err)
	}

	docID := m.docCount

	for _, f := range m.schema.Fields {
		fv := v.Get(f.Name)
		if fv == nil {
			continue
		}
		fa, hasFa := m.fields[f.Name]

		switch f.Type {
		case schema.FieldTypeText:
			sb := fv.GetStringBytes()
			if sb == nil || !hasFa {
				continue
			}
			// Tokenizer needs string; this is the one unavoidable copy per text field.
			tokens := m.tokenizer.Tokenize(string(sb))
			for _, tok := range tokens {
				fa.AddToken(docID, tok.Term, tok.Position)
			}
			fa.SetDoclen(docID, uint32(len(tokens)))

		case schema.FieldTypeKeyword:
			sb := fv.GetStringBytes()
			if sb == nil || !hasFa {
				continue
			}
			fa.AddToken(docID, strings.ToLower(string(sb)), 0)
			fa.SetDoclen(docID, 1)

		case schema.FieldTypeI64:
			// i64 fields: stored in doc store only (no postings in v1).
		}
	}

	if err := m.ds.Add(docID, rawJSON); err != nil {
		return fmt.Errorf("memtable: docstore: %w", err)
	}
	m.docCount++
	m.rawBytes += len(rawJSON)
	return nil
}

// Build finalizes the memtable into a segment Result.
func (m *Memtable) Build() (*segment.Result, error) {
	// Order fields deterministically.
	fieldSlice := make([]*segment.FieldAccumulator, 0, len(m.fields))
	for _, f := range m.schema.Fields {
		if fa, ok := m.fields[f.Name]; ok {
			fieldSlice = append(fieldSlice, fa)
		}
	}
	return segment.Build(fieldSlice, m.ds, m.docCount)
}

// DocCount returns number of docs added.
func (m *Memtable) DocCount() uint32 { return m.docCount }

// EstimatedBytes returns cumulative raw JSON bytes added.
// Used as a flush trigger; approximates final segment size to within 2-3×.
func (m *Memtable) EstimatedBytes() int { return m.rawBytes }
