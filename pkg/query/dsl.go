package query

import (
	"encoding/json"
	"fmt"
)

// NodeType identifies a query node kind.
type NodeType string

const (
	NodeTerm   NodeType = "term"
	NodeMatch  NodeType = "match"
	NodeBool   NodeType = "bool"
	NodeRange  NodeType = "range"
	NodePhrase NodeType = "phrase"
	NodePrefix NodeType = "prefix"
)

// Node is a query tree node.
type Node struct {
	Type NodeType

	// term / match / keyword / prefix
	Field string
	Value string

	// range
	GTE interface{} // int64 or nil
	LTE interface{}
	GT  interface{}
	LT  interface{}

	// bool
	Must    []*Node
	Should  []*Node
	MustNot []*Node
	Filter  []*Node

	// phrase: ordered terms in Field
	Terms []string
}

// MatchAllQuery returns a query that matches all docs (empty bool).
func MatchAll() *Node { return &Node{Type: NodeBool} }

// rawNode is used for flexible JSON unmarshaling.
type rawNode struct {
	Type    string          `json:"type"`
	Field   string          `json:"field"`
	Value   string          `json:"value"`
	GTE     json.RawMessage `json:"gte"`
	LTE     json.RawMessage `json:"lte"`
	GT      json.RawMessage `json:"gt"`
	LT      json.RawMessage `json:"lt"`
	Must    []json.RawMessage `json:"must"`
	Should  []json.RawMessage `json:"should"`
	MustNot []json.RawMessage `json:"must_not"`
	Filter  []json.RawMessage `json:"filter"`
	Terms   []string          `json:"terms"`
	// top-level: {"query": <string or node>}
	Query json.RawMessage `json:"query"`
}

// SearchParams holds parsed search parameters from a request body.
type SearchParams struct {
	Query *Node
	Size  int
	From  int
}

// ParseRequest parses an HTTP request body into SearchParams.
// Accepts {"query": "foo AND bar"} or {"query": {<dsl node>}}.
func ParseRequest(data []byte) (*SearchParams, error) {
	var req struct {
		Query json.RawMessage `json:"query"`
		Size  int             `json:"size"`
		From  int             `json:"from"`
	}
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}

	size := req.Size
	if size <= 0 {
		size = 10
	}
	if size > MaxSearchSize {
		size = MaxSearchSize
	}

	from := req.From
	if from < 0 {
		from = 0
	}
	// from + size cannot exceed MaxSearchSize to prevent deep-pagination abuse.
	if from+size > MaxSearchSize {
		from = 0
		size = MaxSearchSize
	}

	var node *Node
	var err error
	if len(req.Query) == 0 || string(req.Query) == "null" {
		node = MatchAll()
	} else if req.Query[0] == '"' {
		var qs string
		if err = json.Unmarshal(req.Query, &qs); err != nil {
			return nil, err
		}
		node, err = ParseQueryString(qs)
		if err != nil {
			return nil, err
		}
	} else {
		node, err = ParseNode(req.Query)
		if err != nil {
			return nil, err
		}
	}

	return &SearchParams{Query: node, Size: size, From: from}, nil
}

// ParseNode parses a JSON DSL node object.
func ParseNode(data []byte) (*Node, error) {
	var raw rawNode
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	nt := NodeType(raw.Type)
	node := &Node{Type: nt, Field: raw.Field, Value: raw.Value, Terms: raw.Terms}

	switch nt {
	case NodeTerm, NodeMatch, NodeKeyword, NodePrefix:
		if node.Field == "" || node.Value == "" {
			return nil, fmt.Errorf("query %s: field and value required", nt)
		}
	case NodeRange:
		if node.Field == "" {
			return nil, fmt.Errorf("query range: field required")
		}
		parseRangeVal := func(raw json.RawMessage) interface{} {
			if len(raw) == 0 {
				return nil
			}
			var v int64
			if err := json.Unmarshal(raw, &v); err == nil {
				return v
			}
			return nil
		}
		node.GTE = parseRangeVal(raw.GTE)
		node.LTE = parseRangeVal(raw.LTE)
		node.GT = parseRangeVal(raw.GT)
		node.LT = parseRangeVal(raw.LT)
	case NodeBool:
		for _, r := range raw.Must {
			n, err := ParseNode(r)
			if err != nil {
				return nil, err
			}
			node.Must = append(node.Must, n)
		}
		for _, r := range raw.Should {
			n, err := ParseNode(r)
			if err != nil {
				return nil, err
			}
			node.Should = append(node.Should, n)
		}
		for _, r := range raw.MustNot {
			n, err := ParseNode(r)
			if err != nil {
				return nil, err
			}
			node.MustNot = append(node.MustNot, n)
		}
		for _, r := range raw.Filter {
			n, err := ParseNode(r)
			if err != nil {
				return nil, err
			}
			node.Filter = append(node.Filter, n)
		}
	case NodePhrase:
		if node.Field == "" || len(node.Terms) == 0 {
			return nil, fmt.Errorf("query phrase: field and terms required")
		}
	default:
		return nil, fmt.Errorf("unknown query type %q", nt)
	}
	return node, nil
}

// NodeKeyword is a synonym for term in the DSL (exact match, no tokenization).
const NodeKeyword NodeType = "keyword"
