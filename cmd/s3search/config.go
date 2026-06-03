package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/anishsapkota/s3-search/pkg/index"
	"github.com/anishsapkota/s3-search/pkg/store"
)

// Config is the top-level application configuration.
type Config struct {
	Addr      string      `json:"addr"`
	Role      string      `json:"role"` // all | indexer | searcher
	AuthToken string      `json:"auth_token"`
	LogLevel  string      `json:"log_level"`
	WalDir    string      `json:"wal_dir"`
	Minio     MinioConf   `json:"minio"`
	Flush     FlushConf   `json:"flush"`
}

type MinioConf struct {
	Endpoint  string `json:"endpoint"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Bucket    string `json:"bucket"`
	UseSSL    bool   `json:"use_ssl"`
}

type FlushConf struct {
	MaxDocs  uint32 `json:"max_docs"`
	MaxBytes int    `json:"max_bytes"`
	MaxAge   string `json:"max_age"` // duration string
}

func DefaultConfig() Config {
	return Config{
		Addr:     ":7700",
		Role:     "all",
		LogLevel: "info",
		WalDir:   "/tmp/s3search/wal",
		Minio: MinioConf{
			Endpoint:  "localhost:9000",
			AccessKey: "minioadmin",
			SecretKey: "minioadmin",
			Bucket:    "s3search",
		},
		Flush: FlushConf{
			MaxDocs:  100000,
			MaxBytes: 128 * 1024 * 1024,
			MaxAge:   "60s",
		},
	}
}

func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return cfg, fmt.Errorf("load config %s: %w", path, err)
		}
		if err := json.Unmarshal(data, &cfg); err != nil {
			return cfg, fmt.Errorf("parse config: %w", err)
		}
	}
	// Environment variables override file/defaults.
	overrideString := func(dst *string, env string) {
		if v := os.Getenv(env); v != "" {
			*dst = v
		}
	}
	overrideBool := func(dst *bool, env string) {
		if v := os.Getenv(env); v == "true" || v == "1" {
			*dst = true
		}
	}
	overrideString(&cfg.Addr,              "S3SEARCH_ADDR")
	overrideString(&cfg.Role,              "S3SEARCH_ROLE")
	overrideString(&cfg.AuthToken,         "S3SEARCH_AUTH_TOKEN")
	overrideString(&cfg.LogLevel,          "S3SEARCH_LOG_LEVEL")
	overrideString(&cfg.WalDir,            "S3SEARCH_WAL_DIR")
	overrideString(&cfg.Minio.Endpoint,    "S3SEARCH_MINIO_ENDPOINT")
	overrideString(&cfg.Minio.AccessKey,   "S3SEARCH_MINIO_ACCESS_KEY")
	overrideString(&cfg.Minio.SecretKey,   "S3SEARCH_MINIO_SECRET_KEY")
	overrideString(&cfg.Minio.Bucket,      "S3SEARCH_MINIO_BUCKET")
	overrideBool(&cfg.Minio.UseSSL,        "S3SEARCH_MINIO_USE_SSL")
	return cfg, nil
}

func (cfg Config) ToMinioConfig() store.MinioConfig {
	return store.MinioConfig{
		Endpoint:  cfg.Minio.Endpoint,
		AccessKey: cfg.Minio.AccessKey,
		SecretKey: cfg.Minio.SecretKey,
		Bucket:    cfg.Minio.Bucket,
		UseSSL:    cfg.Minio.UseSSL,
	}
}

func (cfg Config) ToFlushConfig() index.FlushConfig {
	fc := index.DefaultFlushConfig
	if cfg.Flush.MaxDocs > 0 {
		fc.MaxDocs = cfg.Flush.MaxDocs
	}
	if cfg.Flush.MaxBytes > 0 {
		fc.MaxBytes = cfg.Flush.MaxBytes
	}
	if cfg.Flush.MaxAge != "" {
		if d, err := time.ParseDuration(cfg.Flush.MaxAge); err == nil {
			fc.MaxAge = d
		}
	}
	return fc
}
