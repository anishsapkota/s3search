package query_test

import (
	"testing"

	"github.com/anishsapkota/s3-search/pkg/query"
)

func TestParseQueryString(t *testing.T) {
	cases := []struct {
		input    string
		wantType query.NodeType
	}{
		{"hello", query.NodeMatch},
		{"level:ERROR", query.NodeMatch},
		{"foo AND bar", query.NodeBool},
		{"foo OR bar", query.NodeBool},
		{"NOT foo", query.NodeBool},
		{"level:ERROR AND service:api", query.NodeBool},
	}
	for _, c := range cases {
		t.Run(c.input, func(t *testing.T) {
			n, err := query.ParseQueryString(c.input)
			if err != nil {
				t.Fatalf("parse %q: %v", c.input, err)
			}
			if n.Type != c.wantType {
				t.Errorf("type: got %q want %q", n.Type, c.wantType)
			}
		})
	}
}

func TestBM25Score(t *testing.T) {
	score := query.BM25Score(1, 10, 10, 5, 100)
	if score <= 0 {
		t.Errorf("expected positive score, got %f", score)
	}
}
