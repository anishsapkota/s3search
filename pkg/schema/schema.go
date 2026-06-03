package schema

import (
	"encoding/json"
	"fmt"
)

// FieldType enumerates supported field types.
type FieldType string

const (
	FieldTypeText    FieldType = "text"
	FieldTypeKeyword FieldType = "keyword"
	FieldTypeI64     FieldType = "i64"
)

// Field describes one field in an index schema.
type Field struct {
	Name  string    `json:"name"`
	Type  FieldType `json:"type"`
	Store bool      `json:"store"` // include in doc store
}

// Schema is the fixed mapping for an index, declared at creation time.
type Schema struct {
	Fields []Field `json:"fields"`
	// IDField is the optional field to use as document primary key.
	IDField string `json:"id_field,omitempty"`
}

// Validate checks schema consistency.
func (s *Schema) Validate() error {
	seen := make(map[string]bool)
	for _, f := range s.Fields {
		if seen[f.Name] {
			return fmt.Errorf("duplicate field %q", f.Name)
		}
		seen[f.Name] = true
		switch f.Type {
		case FieldTypeText, FieldTypeKeyword, FieldTypeI64:
		default:
			return fmt.Errorf("field %q: unknown type %q", f.Name, f.Type)
		}
	}
	if s.IDField != "" && !seen[s.IDField] {
		return fmt.Errorf("id_field %q not in fields", s.IDField)
	}
	return nil
}

// Field returns the field descriptor for name, or nil.
func (s *Schema) Field(name string) *Field {
	for i := range s.Fields {
		if s.Fields[i].Name == name {
			return &s.Fields[i]
		}
	}
	return nil
}

// TextFields returns names of all text-type fields.
func (s *Schema) TextFields() []string {
	var out []string
	for _, f := range s.Fields {
		if f.Type == FieldTypeText {
			out = append(out, f.Name)
		}
	}
	return out
}

// MarshalJSON / Unmarshal passthrough so callers can use json.Marshal on Schema directly.
func Parse(data []byte) (*Schema, error) {
	var s Schema
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return &s, nil
}

func Marshal(s *Schema) ([]byte, error) {
	return json.MarshalIndent(s, "", "  ")
}
