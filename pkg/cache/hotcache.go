package cache

import (
	"container/list"
	"sync"

	"github.com/anishsapkota/s3-search/pkg/obs"
	"github.com/anishsapkota/s3-search/pkg/segment"
)

// HotcacheEntry wraps a parsed hotcache.
type HotcacheEntry struct {
	Key string
	HC  *segment.Hotcache
}

// HotcacheLRU is a thread-safe in-memory LRU cache for segment hotcaches.
type HotcacheLRU struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*list.Element
	order    *list.List
}

func NewHotcacheLRU(capacity int) *HotcacheLRU {
	if capacity <= 0 {
		capacity = 128
	}
	return &HotcacheLRU{
		capacity: capacity,
		items:    make(map[string]*list.Element),
		order:    list.New(),
	}
}

func (h *HotcacheLRU) Get(key string) (*segment.Hotcache, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	el, ok := h.items[key]
	if !ok {
		obs.HotcacheLookups.WithLabelValues("miss").Inc()
		return nil, false
	}
	h.order.MoveToFront(el)
	obs.HotcacheLookups.WithLabelValues("hit").Inc()
	return el.Value.(*HotcacheEntry).HC, true
}

func (h *HotcacheLRU) Put(key string, hc *segment.Hotcache) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if el, ok := h.items[key]; ok {
		h.order.MoveToFront(el)
		el.Value.(*HotcacheEntry).HC = hc
		return
	}
	entry := &HotcacheEntry{Key: key, HC: hc}
	el := h.order.PushFront(entry)
	h.items[key] = el
	for h.order.Len() > h.capacity {
		back := h.order.Back()
		if back == nil {
			break
		}
		h.order.Remove(back)
		delete(h.items, back.Value.(*HotcacheEntry).Key)
	}
}
