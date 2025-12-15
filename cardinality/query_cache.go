// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package cardinality

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

type QueryCache struct {
	mu       sync.RWMutex
	items    map[string]*list.Element
	order    *list.List
	maxItems int
	ttl      time.Duration
}

type cacheEntry struct {
	key       string
	value     any
	expiresAt time.Time
}

func NewQueryCache(maxItems int, ttl time.Duration) *QueryCache {
	return &QueryCache{
		items:    make(map[string]*list.Element),
		order:    list.New(),
		maxItems: maxItems,
		ttl:      ttl,
	}
}

func (c *QueryCache) Get(key string) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, exists := c.items[key]
	if !exists {
		return nil, false
	}

	entry := elem.Value.(*cacheEntry)
	if time.Now().After(entry.expiresAt) {
		c.removeElement(elem)
		return nil, false
	}

	c.order.MoveToFront(elem)
	return entry.value, true
}

func (c *QueryCache) Set(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, exists := c.items[key]; exists {
		entry := elem.Value.(*cacheEntry)
		entry.value = value
		entry.expiresAt = time.Now().Add(c.ttl)
		c.order.MoveToFront(elem)
		return
	}

	for c.order.Len() >= c.maxItems {
		oldest := c.order.Back()
		if oldest != nil {
			c.removeElement(oldest)
		}
	}

	entry := &cacheEntry{
		key:       key,
		value:     value,
		expiresAt: time.Now().Add(c.ttl),
	}
	elem := c.order.PushFront(entry)
	c.items[key] = elem
}

func (c *QueryCache) removeElement(elem *list.Element) {
	entry := elem.Value.(*cacheEntry)
	delete(c.items, entry.key)
	c.order.Remove(elem)
}

func (c *QueryCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.order.Len()
}

func (c *QueryCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[string]*list.Element)
	c.order.Init()
}

func LabelsCardinalityKey(start, end time.Time, limit int, metricName string) string {
	return fmt.Sprintf("labels:%s:%s:%d:%s",
		start.Format("2006-01-02"),
		end.Format("2006-01-02"),
		limit,
		metricName,
	)
}

func LabelsCardinalityPerDayKey(start, end time.Time, limit int, metricName string) string {
	return fmt.Sprintf("labels-per-day:%s:%s:%d:%s",
		start.Format("2006-01-02"),
		end.Format("2006-01-02"),
		limit,
		metricName,
	)
}

func CardinalityKey(start, end time.Time, limit int) string {
	return fmt.Sprintf("cardinality:%s:%s:%d",
		start.Format("2006-01-02"),
		end.Format("2006-01-02"),
		limit,
	)
}

func MetricHistoryKey(metricName string, start, end time.Time) string {
	return fmt.Sprintf("metric-history:%s:%s:%s",
		metricName,
		start.Format("2006-01-02"),
		end.Format("2006-01-02"),
	)
}
