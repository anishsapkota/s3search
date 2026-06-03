package index

import (
	"context"
	"strings"

	"github.com/anishsapkota/s3-search/pkg/store"
)

// ListIndexes scans S3 for all indexes by looking for schema.json objects.
// Returns the index names found.
func ListIndexes(ctx context.Context, bs store.BlobStore) ([]string, error) {
	// Schema objects are stored at {indexName}/schema.json.
	// List with empty prefix to find all top-level "directories".
	keys, err := bs.List(ctx, "")
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	for _, k := range keys {
		if strings.HasSuffix(k, "/schema.json") {
			name := strings.TrimSuffix(k, "/schema.json")
			// Must be a direct child (no nested slashes).
			if !strings.Contains(name, "/") && name != "" {
				seen[name] = true
			}
		}
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	return names, nil
}
