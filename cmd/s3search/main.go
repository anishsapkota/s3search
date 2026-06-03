package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/anishsapkota/s3-search/pkg/index"
	"github.com/anishsapkota/s3-search/pkg/obs"
	"github.com/anishsapkota/s3-search/pkg/query"
	"github.com/anishsapkota/s3-search/pkg/schema"
	"github.com/anishsapkota/s3-search/pkg/search"
	"github.com/anishsapkota/s3-search/pkg/server"
	"github.com/anishsapkota/s3-search/pkg/store"
)

var cfgFile string

func main() {
	root := &cobra.Command{
		Use:   "s3search",
		Short: "S3-backed full-text search engine",
	}
	root.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (JSON)")

	root.AddCommand(serverCmd())
	root.AddCommand(indexCmd())
	root.AddCommand(searchCmd())

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func serverCmd() *cobra.Command {
	var role string
	cmd := &cobra.Command{
		Use:   "server",
		Short: "Start the s3search server",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := LoadConfig(cfgFile)
			if err != nil {
				return err
			}
			if role != "" {
				cfg.Role = role
			}
			obs.SetupLogging(cfg.LogLevel)

			bs, err := store.NewMinioStore(cfg.ToMinioConfig())
			if err != nil {
				return fmt.Errorf("store: %w", err)
			}

			srvCfg := server.Config{
				Addr:      cfg.Addr,
				AuthToken: cfg.AuthToken,
				WalDir:    cfg.WalDir,
				FlushCfg:  cfg.ToFlushConfig(),
			}
			srv := server.New(srvCfg, bs)

			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			// Pre-load all known indexes at startup so WAL replay happens before
			// serving traffic, not on the first user request.
			srv.PreloadIndexes(ctx)

			return srv.ListenAndServe(ctx)
		},
	}
	cmd.Flags().StringVar(&role, "role", "", "role: all|indexer|searcher")
	return cmd
}

func indexCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "index", Short: "Index management"}

	create := &cobra.Command{
		Use:   "create",
		Short: "Create an index",
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")
			schemaFile, _ := cmd.Flags().GetString("schema")
			if name == "" || schemaFile == "" {
				return fmt.Errorf("--name and --schema required")
			}
			schemaData, err := os.ReadFile(schemaFile)
			if err != nil {
				return err
			}
			sc, err := schema.Parse(schemaData)
			if err != nil {
				return err
			}
			cfg, err := LoadConfig(cfgFile)
			if err != nil {
				return err
			}
			bs, err := store.NewMinioStore(cfg.ToMinioConfig())
			if err != nil {
				return err
			}
			ctx := context.Background()
			idx, err := index.Open(ctx, name, sc, bs, cfg.WalDir+"/"+name, cfg.ToFlushConfig())
			if err != nil {
				return err
			}
			defer idx.Close(ctx)
			fmt.Printf("index %q created\n", name)
			return nil
		},
	}
	create.Flags().String("name", "", "index name")
	create.Flags().String("schema", "", "schema JSON file")

	ingest := &cobra.Command{
		Use:   "ingest",
		Short: "Ingest NDJSON file into index",
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")
			file, _ := cmd.Flags().GetString("file")
			if name == "" || file == "" {
				return fmt.Errorf("--name and --file required")
			}
			data, err := os.ReadFile(file)
			if err != nil {
				return err
			}
			cfg, err := LoadConfig(cfgFile)
			if err != nil {
				return err
			}
			bs, err := store.NewMinioStore(cfg.ToMinioConfig())
			if err != nil {
				return err
			}
			ctx := context.Background()
			sc, err := index.LoadSchema(ctx, bs, name)
			if err != nil {
				return fmt.Errorf("load schema: %w", err)
			}
			idx, err := index.Open(ctx, name, sc, bs, cfg.WalDir+"/"+name, cfg.ToFlushConfig())
			if err != nil {
				return err
			}
			defer idx.Close(ctx)

			var docs []json.RawMessage
			for _, line := range splitLines(string(data)) {
				if line != "" {
					docs = append(docs, json.RawMessage(line))
				}
			}
			if err := idx.AddBatch(ctx, docs); err != nil {
				return err
			}
			if err := idx.FlushNow(ctx); err != nil {
				return err
			}
			fmt.Printf("indexed %d docs into %q\n", len(docs), name)
			return nil
		},
	}
	ingest.Flags().String("name", "", "index name")
	ingest.Flags().String("file", "", "NDJSON file path")

	cmd.AddCommand(create, ingest)
	return cmd
}

func searchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "search",
		Short: "Search an index",
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("index")
			queryStr, _ := cmd.Flags().GetString("query")
			size, _ := cmd.Flags().GetInt("size")
			from, _ := cmd.Flags().GetInt("from")
			if name == "" || queryStr == "" {
				return fmt.Errorf("--index and --query required")
			}
			if size <= 0 {
				size = 10
			}

			q, err := query.ParseQueryString(queryStr)
			if err != nil {
				return err
			}
			cfg, err := LoadConfig(cfgFile)
			if err != nil {
				return err
			}
			bs, err := store.NewMinioStore(cfg.ToMinioConfig())
			if err != nil {
				return err
			}

			ms := index.NewManifestStore(bs, name)
			searcher := search.New(bs, 64)
			resp, err := searcher.Search(context.Background(), ms, search.Request{
				Index: name,
				Query: q,
				Size:  size,
				From:  from,
			})
			if err != nil {
				return err
			}

			out, _ := json.MarshalIndent(resp, "", "  ")
			fmt.Println(string(out))
			return nil
		},
	}
	cmd.Flags().String("index", "", "index name")
	cmd.Flags().String("query", "", "query string")
	cmd.Flags().Int("size", 10, "number of results")
	cmd.Flags().Int("from", 0, "result offset for pagination")
	return cmd
}

func splitLines(s string) []string {
	var out []string
	for _, line := range splitNewlines(s) {
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}

func splitNewlines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
