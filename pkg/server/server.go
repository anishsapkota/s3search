package server

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"

	"github.com/anishsapkota/s3-search/pkg/analyze"
	"github.com/anishsapkota/s3-search/pkg/index"
	"github.com/anishsapkota/s3-search/pkg/obs"
	"github.com/anishsapkota/s3-search/pkg/query"
	"github.com/anishsapkota/s3-search/pkg/schema"
	"github.com/anishsapkota/s3-search/pkg/search"
	"github.com/anishsapkota/s3-search/pkg/store"
	"github.com/anishsapkota/s3-search/web"
)

// Config holds server configuration.
type Config struct {
	Addr      string
	AuthToken string
	WalDir    string
	FlushCfg  index.FlushConfig
}

// Server is the HTTP search/indexer server.
type Server struct {
	cfg      Config
	bs       store.BlobStore
	searcher *search.Searcher

	mu      sync.RWMutex
	indexes map[string]*index.Index
	sfGroup singleflight.Group // prevents duplicate concurrent loads
}

func New(cfg Config, bs store.BlobStore) *Server {
	return &Server{
		cfg:      cfg,
		bs:       bs,
		searcher: search.New(bs, 256),
		indexes:  make(map[string]*index.Index),
	}
}

// PreloadIndexes scans S3 for known indexes (by listing schema.json objects)
// and opens them all at startup, triggering WAL replay before serving traffic.
func (s *Server) PreloadIndexes(ctx context.Context) {
	names, err := index.ListIndexes(ctx, s.bs)
	if err != nil {
		slog.Warn("preload: list indexes failed", "err", err)
		return
	}
	for _, name := range names {
		if _, err := s.openIndex(ctx, name); err != nil {
			slog.Warn("preload: open index failed", "index", name, "err", err)
		}
	}
	if len(names) > 0 {
		slog.Info("preloaded indexes", "count", len(names))
	}
}

// Handler returns the HTTP mux with CORS + UI.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// API routes.
	mux.HandleFunc("/index/",   s.routeIndex)
	mux.HandleFunc("/search/",  s.routeSearch)
	mux.Handle("/metrics",      obs.MetricsHandler())
	mux.HandleFunc("/health",   func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Serve embedded single-page UI at / and /ui (self-contained HTML, no sub-assets).
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Serve the UI for / and /ui (any suffix).
		if r.URL.Path == "/" || r.URL.Path == "/ui" || strings.HasPrefix(r.URL.Path, "/ui/") {
			data, err := web.Static.ReadFile("static/index.html")
			if err != nil {
				http.Error(w, "UI not available", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write(data)
			return
		}
		http.NotFound(w, r)
	})

	return corsMiddleware(mux)
}

// corsMiddleware adds permissive CORS headers for browser dev use.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) routeIndex(w http.ResponseWriter, r *http.Request) {
	// /index/{name}/docs  POST  → ingest
	// /index/{name}       PUT   → create
	// /index/{name}       DELETE→ drop
	path := strings.TrimPrefix(r.URL.Path, "/index/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "index name required", http.StatusBadRequest)
		return
	}
	indexName := parts[0]
	sub := ""
	if len(parts) == 2 {
		sub = parts[1]
	}

	switch {
	case r.Method == http.MethodPut && sub == "":
		s.handleCreateIndex(w, r, indexName)
	case r.Method == http.MethodPost && sub == "docs":
		s.handleIngest(w, r, indexName)
	case r.Method == http.MethodDelete && sub == "docs":
		s.handleDeleteByQuery(w, r, indexName)
	case r.Method == http.MethodPost && sub == "_compact":
		s.handleCompact(w, r, indexName)
	case r.Method == http.MethodPost && sub == "_gc":
		s.handleGC(w, r, indexName)
	case r.Method == http.MethodPost && sub == "_flush":
		s.handleFlush(w, r, indexName)
	case r.Method == http.MethodGet && sub == "":
		s.handleGetIndex(w, r, indexName)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func (s *Server) routeSearch(w http.ResponseWriter, r *http.Request) {
	// /search/{name}  POST  → search
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	indexName := strings.TrimPrefix(r.URL.Path, "/search/")
	indexName = strings.Trim(indexName, "/")
	if indexName == "" {
		http.Error(w, "index name required", http.StatusBadRequest)
		return
	}
	s.handleSearch(w, r, indexName)
}

func (s *Server) handleCreateIndex(w http.ResponseWriter, r *http.Request, name string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	sc, err := schema.Parse(body)
	if err != nil {
		http.Error(w, "invalid schema: "+err.Error(), http.StatusBadRequest)
		return
	}

	idx, err := index.Open(context.Background(), name, sc, s.bs, s.cfg.WalDir+"/"+name, s.cfg.FlushCfg)
	if err != nil {
		http.Error(w, "create index: "+err.Error(), http.StatusInternalServerError)
		return
	}

	s.mu.Lock()
	s.indexes[name] = idx
	s.mu.Unlock()

	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]string{"index": name, "status": "created"})
}

// streamBatchSize controls how many docs are accumulated before each AddBatch call.
// Keeps peak memory proportional to batch size, not total request size.
const streamBatchSize = 5_000

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request, name string) {
	s.mu.RLock()
	idx := s.indexes[name]
	s.mu.RUnlock()

	if idx == nil {
		var err error
		idx, err = s.openIndex(r.Context(), name)
		if err != nil {
			http.Error(w, "index not found: "+name, http.StatusNotFound)
			return
		}
	}

	// Stream body line-by-line — never buffer the full request body.
	// Limits peak RAM to O(streamBatchSize) regardless of request size.
	scanner := bufio.NewScanner(r.Body)
	scanner.Buffer(make([]byte, 256*1024), 16*1024*1024) // start 256KB, max 16MB per line

	batch := make([]json.RawMessage, 0, streamBatchSize)
	totalIndexed := 0

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := idx.AddBatch(r.Context(), batch); err != nil {
			return err
		}
		obs.IngestDocsTotal.WithLabelValues(name).Add(float64(len(batch)))
		obs.IngestBatchSize.WithLabelValues(name).Observe(float64(len(batch)))
		totalIndexed += len(batch)
		batch = batch[:0]
		return nil
	}

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		batch = append(batch, json.RawMessage(line))
		if len(batch) >= streamBatchSize {
			if err := flush(); err != nil {
				http.Error(w, "ingest error: "+err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}
	if err := scanner.Err(); err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := flush(); err != nil {
		http.Error(w, "ingest error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"indexed": totalIndexed})
}

func (s *Server) handleGetIndex(w http.ResponseWriter, r *http.Request, name string) {
	s.mu.RLock()
	idx := s.indexes[name]
	s.mu.RUnlock()
	if idx == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	sc, _ := schema.Marshal(idx.Schema())
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(sc)
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request, name string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}

	params, err := query.ParseRequest(body)
	if err != nil {
		http.Error(w, "parse query: "+err.Error(), http.StatusBadRequest)
		return
	}

	timer := prometheus.NewTimer(obs.QueryDuration.WithLabelValues(name, "ok"))
	ms := index.NewManifestStore(s.bs, name)
	resp, err := s.searcher.Search(r.Context(), ms, search.Request{
		Index: name,
		Query: params.Query,
		Size:  params.Size,
		From:  params.From,
	})
	if err != nil {
		timer.ObserveDuration()
		obs.QueryTotal.WithLabelValues(name, "error").Inc()
		http.Error(w, "search error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	timer.ObserveDuration()
	obs.QueryTotal.WithLabelValues(name, "ok").Inc()
	obs.QueryHitsTotal.WithLabelValues(name).Add(float64(resp.TotalHits))

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleDeleteByQuery(w http.ResponseWriter, r *http.Request, name string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	params, err := query.ParseRequest(body)
	if err != nil {
		http.Error(w, "parse query: "+err.Error(), http.StatusBadRequest)
		return
	}
	ms := index.NewManifestStore(s.bs, name)
	tok := analyze.StandardTokenizer{}
	deleted, err := index.DeleteByQuery(r.Context(), s.bs, ms, name, params.Query, tok)
	if err != nil {
		http.Error(w, "delete: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"deleted": deleted})
}

func (s *Server) handleCompact(w http.ResponseWriter, r *http.Request, name string) {
	s.mu.RLock()
	idx := s.indexes[name]
	s.mu.RUnlock()
	if idx == nil {
		http.Error(w, "index not found: "+name, http.StatusNotFound)
		return
	}
	if err := idx.NewCompactor().CompactOnce(r.Context()); err != nil {
		slog.Error("compact failed", "index", name, "err", err)
		http.Error(w, "compact: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleGC(w http.ResponseWriter, r *http.Request, name string) {
	s.mu.RLock()
	idx := s.indexes[name]
	s.mu.RUnlock()
	if idx == nil {
		http.Error(w, "index not found: "+name, http.StatusNotFound)
		return
	}
	if err := idx.NewCompactor().GC(r.Context(), 5); err != nil {
		slog.Error("gc failed", "index", name, "err", err)
		http.Error(w, "gc: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleFlush(w http.ResponseWriter, r *http.Request, name string) {
	s.mu.RLock()
	idx := s.indexes[name]
	s.mu.RUnlock()
	if idx == nil {
		http.Error(w, "index not found: "+name, http.StatusNotFound)
		return
	}
	if err := idx.FlushNow(r.Context()); err != nil {
		http.Error(w, "flush: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// openIndex loads an index (with WAL replay) exactly once per name per process lifetime.
// Concurrent callers for the same name block on the first call; subsequent callers get
// the cached result without replaying the WAL again.
// Uses background context so that a cancelled request cannot abort a load mid-flight
// and leave the map unpopulated.
func (s *Server) openIndex(ctx context.Context, name string) (*index.Index, error) {
	// Fast path: already loaded.
	s.mu.RLock()
	idx := s.indexes[name]
	s.mu.RUnlock()
	if idx != nil {
		return idx, nil
	}

	// Slow path: deduplicate concurrent loads with singleflight.
	v, err, _ := s.sfGroup.Do(name, func() (interface{}, error) {
		// Double-check after entering the flight (another goroutine may have finished first).
		s.mu.RLock()
		idx := s.indexes[name]
		s.mu.RUnlock()
		if idx != nil {
			return idx, nil
		}

		// Use background context — request context must NOT cancel an index load.
		// A cancelled load would leave s.indexes unpopulated and the next request
		// would trigger another WAL replay.
		loadCtx := context.Background()
		sc, err := index.LoadSchema(loadCtx, s.bs, name)
		if err != nil {
			return nil, err
		}
		idx, err = index.Open(loadCtx, name, sc, s.bs, s.cfg.WalDir+"/"+name, s.cfg.FlushCfg)
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		s.indexes[name] = idx
		s.mu.Unlock()
		return idx, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*index.Index), nil
}

// ListenAndServe starts the HTTP server.
func (s *Server) ListenAndServe(ctx context.Context) error {
	srv := &http.Server{
		Addr:    s.cfg.Addr,
		Handler: s.Handler(),
	}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	slog.Info("server starting", "addr", s.cfg.Addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
