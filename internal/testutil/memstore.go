package testutil

import (
	"context"
	"strings"
	"sync"

	"github.com/anishsapkota/s3-search/pkg/store"
)

// MemStore is an in-memory BlobStore for testing.
type MemStore struct {
	mu      sync.RWMutex
	objects map[string][]byte
	// RangeLog records every GetRange call: (key, off, len).
	RangeLog []RangeCall
}

type RangeCall struct {
	Key    string
	Off    int64
	Length int64
}

func NewMemStore() *MemStore {
	return &MemStore{objects: make(map[string][]byte)}
}

func (m *MemStore) Get(ctx context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.objects[key]
	if !ok {
		return nil, store.ErrNotFound{Key: key}
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp, nil
}

func (m *MemStore) GetRange(ctx context.Context, key string, off, length int64) ([]byte, error) {
	m.mu.Lock()
	m.RangeLog = append(m.RangeLog, RangeCall{key, off, length})
	m.mu.Unlock()

	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.objects[key]
	if !ok {
		return nil, store.ErrNotFound{Key: key}
	}
	end := off + length
	if end > int64(len(b)) {
		end = int64(len(b))
	}
	cp := make([]byte, end-off)
	copy(cp, b[off:end])
	return cp, nil
}

func (m *MemStore) Put(ctx context.Context, key string, body []byte, opts store.PutOpts) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if opts.IfNoneMatch == "*" {
		if _, ok := m.objects[key]; ok {
			return store.ErrPreconditionFailed{Key: key}
		}
	}
	cp := make([]byte, len(body))
	copy(cp, body)
	m.objects[key] = cp
	return nil
}

func (m *MemStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

func (m *MemStore) List(ctx context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (m *MemStore) Head(ctx context.Context, key string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.objects[key]
	if !ok {
		return 0, store.ErrNotFound{Key: key}
	}
	return int64(len(b)), nil
}
