package query

import "math"

const (
	BM25K1 = 1.2
	BM25B  = 0.75
)

// BM25Score computes BM25 score for one term in one doc.
// tf = term frequency in doc, docLen = doc length (tokens), avgDocLen = average across segment.
// df = doc frequency of term (cardinality of posting list), N = total docs in segment.
func BM25Score(tf, docLen, avgDocLen float64, df, N uint64) float64 {
	if df == 0 || N == 0 {
		return 0
	}
	idf := math.Log(1 + (float64(N)-float64(df)+0.5)/(float64(df)+0.5))
	norm := tf * (BM25K1 + 1) / (tf + BM25K1*(1-BM25B+BM25B*docLen/avgDocLen))
	return idf * norm
}
