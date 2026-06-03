package store

import "context"

// PutOpts controls conditional PUT behavior.
type PutOpts struct {
	// IfNoneMatch when set to "*" causes PUT to fail if object already exists.
	IfNoneMatch string
	ContentType string
}

// BlobStore is the storage abstraction over S3-compatible object stores.
type BlobStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	GetRange(ctx context.Context, key string, off, length int64) ([]byte, error)
	Put(ctx context.Context, key string, body []byte, opts PutOpts) error
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
	Head(ctx context.Context, key string) (size int64, err error)
}

// ErrPreconditionFailed is returned by Put when If-None-Match condition fails.
type ErrPreconditionFailed struct{ Key string }

func (e ErrPreconditionFailed) Error() string {
	return "precondition failed: object already exists: " + e.Key
}

// ErrNotFound is returned when an object does not exist.
type ErrNotFound struct{ Key string }

func (e ErrNotFound) Error() string { return "object not found: " + e.Key }
