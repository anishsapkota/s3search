package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// ── Query ──────────────────────────────────────────────────────────────
	QueryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "s3search_query_duration_seconds",
		Help:    "End-to-end query duration in seconds.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"index", "status"})

	QueryTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3search_queries_total",
		Help: "Total number of search requests.",
	}, []string{"index", "status"})

	QueryHitsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3search_query_hits_total",
		Help: "Total number of search hits returned.",
	}, []string{"index"})

	// ── Ingest ─────────────────────────────────────────────────────────────
	IngestDocsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3search_ingest_docs_total",
		Help: "Total documents ingested.",
	}, []string{"index"})

	IngestBatchSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "s3search_ingest_batch_size",
		Help:    "Number of docs per ingest batch.",
		Buckets: []float64{1, 5, 10, 50, 100, 500, 1000, 5000, 10000},
	}, []string{"index"})

	// ── Flush / Segment ────────────────────────────────────────────────────
	FlushTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3search_flushes_total",
		Help: "Total segment flush operations.",
	}, []string{"index", "status"})

	FlushDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "s3search_flush_duration_seconds",
		Help:    "Duration of segment build + S3 upload.",
		Buckets: []float64{.1, .25, .5, 1, 2, 5, 10, 30},
	}, []string{"index"})

	FlushDocCount = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "s3search_flush_doc_count",
		Help:    "Documents per flushed segment.",
		Buckets: []float64{100, 500, 1000, 5000, 10000, 50000, 100000},
	}, []string{"index"})

	ActiveSegments = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "s3search_active_segments",
		Help: "Number of active (non-deleted) segments per index.",
	}, []string{"index"})

	// ── Cache ──────────────────────────────────────────────────────────────
	HotcacheLookups = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3search_hotcache_lookups_total",
		Help: "Hotcache RAM lookup results.",
	}, []string{"result"}) // "hit" | "miss"

	// ── Compaction / Deletes ───────────────────────────────────────────────
	CompactionTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3search_compactions_total",
		Help: "Total compaction runs.",
	}, []string{"index", "status"})

	CompactionMergedSegments = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "s3search_compaction_merged_segments",
		Help:    "Segments merged per compaction run.",
		Buckets: []float64{2, 3, 4, 6, 8, 12, 16},
	}, []string{"index"})

	DeletedDocsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3search_deleted_docs_total",
		Help: "Total documents deleted by query.",
	}, []string{"index"})

	// ── WAL ───────────────────────────────────────────────────────────────
	WALReplaysTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3search_wal_replays_total",
		Help: "Total WAL replay events on startup.",
	}, []string{"index"})

	WALReplayDocs = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "s3search_wal_replay_docs",
		Help:    "Documents recovered per WAL replay.",
		Buckets: []float64{0, 10, 100, 1000, 10000, 100000},
	}, []string{"index"})
)
