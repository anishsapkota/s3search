package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// MinioConfig holds connection params for MinIO / S3.
type MinioConfig struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	UseSSL    bool
}

type minioStore struct {
	client *minio.Client
	bucket string
}

// NewMinioStore creates a BlobStore backed by a MinIO-compatible endpoint.
func NewMinioStore(cfg MinioConfig) (BlobStore, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("minio: %w", err)
	}
	return &minioStore{client: client, bucket: cfg.Bucket}, nil
}

func (m *minioStore) Get(ctx context.Context, key string) ([]byte, error) {
	obj, err := m.client.GetObject(ctx, m.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, mapErr(key, err)
	}
	defer obj.Close()
	return io.ReadAll(obj)
}

func (m *minioStore) GetRange(ctx context.Context, key string, off, length int64) ([]byte, error) {
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(off, off+length-1); err != nil {
		return nil, err
	}
	obj, err := m.client.GetObject(ctx, m.bucket, key, opts)
	if err != nil {
		return nil, mapErr(key, err)
	}
	defer obj.Close()
	return io.ReadAll(obj)
}

func (m *minioStore) Put(ctx context.Context, key string, body []byte, opts PutOpts) error {
	popts := minio.PutObjectOptions{ContentType: "application/octet-stream"}
	if opts.ContentType != "" {
		popts.ContentType = opts.ContentType
	}
	if opts.IfNoneMatch == "*" {
		// MinIO supports If-None-Match: * — use UserMetadata as header passthrough
		// via minio-go: set via PutObjectOptions.UserMetadata is not the same.
		// Use raw PutObject with options; minio-go v7 supports DisableContentSha256
		// but no direct If-None-Match header. Workaround: check existence first with
		// HEAD then PUT (not atomic). For strong conditional PUT we use minio.PutObjectOptions
		// with UserTags — NOT available. Use StatObject + PutObject with serialized
		// single-writer discipline.
		// TODO: replace with minio-go conditional PUT when API lands.
		_, err := m.client.StatObject(ctx, m.bucket, key, minio.StatObjectOptions{})
		if err == nil {
			return ErrPreconditionFailed{Key: key}
		}
	}
	_, err := m.client.PutObject(ctx, m.bucket, key, bytes.NewReader(body), int64(len(body)), popts)
	if err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return nil
}

func (m *minioStore) Delete(ctx context.Context, key string) error {
	return m.client.RemoveObject(ctx, m.bucket, key, minio.RemoveObjectOptions{})
}

func (m *minioStore) List(ctx context.Context, prefix string) ([]string, error) {
	ch := m.client.ListObjects(ctx, m.bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})
	var keys []string
	for obj := range ch {
		if obj.Err != nil {
			return nil, obj.Err
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

func (m *minioStore) Head(ctx context.Context, key string) (int64, error) {
	info, err := m.client.StatObject(ctx, m.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return 0, mapErr(key, err)
	}
	return info.Size, nil
}

func mapErr(key string, err error) error {
	var resp minio.ErrorResponse
	if errors.As(err, &resp) {
		if resp.StatusCode == http.StatusNotFound || resp.Code == "NoSuchKey" {
			return ErrNotFound{Key: key}
		}
	}
	return err
}
