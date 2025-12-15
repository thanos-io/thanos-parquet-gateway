// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package cardinality

import (
	"testing"
	"time"
)

func TestQueryCache_GetSet(t *testing.T) {
	cache := NewQueryCache(10, time.Minute)

	// Test miss
	_, ok := cache.Get("missing")
	if ok {
		t.Error("expected cache miss for non-existent key")
	}

	// Test set and get
	cache.Set("key1", "value1")
	val, ok := cache.Get("key1")
	if !ok {
		t.Error("expected cache hit")
	}
	if val != "value1" {
		t.Errorf("expected value1, got %v", val)
	}

	// Test overwrite
	cache.Set("key1", "value2")
	val, ok = cache.Get("key1")
	if !ok {
		t.Error("expected cache hit after overwrite")
	}
	if val != "value2" {
		t.Errorf("expected value2, got %v", val)
	}
}

func TestQueryCache_Expiration(t *testing.T) {
	cache := NewQueryCache(10, 50*time.Millisecond)

	cache.Set("key1", "value1")

	// Should be present immediately
	_, ok := cache.Get("key1")
	if !ok {
		t.Error("expected cache hit before expiration")
	}

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	// Should be expired now
	_, ok = cache.Get("key1")
	if ok {
		t.Error("expected cache miss after expiration")
	}
}

func TestQueryCache_LRUEviction(t *testing.T) {
	cache := NewQueryCache(3, time.Hour)

	// Fill cache
	cache.Set("key1", "value1")
	cache.Set("key2", "value2")
	cache.Set("key3", "value3")

	if cache.Len() != 3 {
		t.Errorf("expected len 3, got %d", cache.Len())
	}

	// Access key1 to make it recently used
	cache.Get("key1")

	// Add new item - should evict key2 (least recently used)
	cache.Set("key4", "value4")

	if cache.Len() != 3 {
		t.Errorf("expected len 3 after eviction, got %d", cache.Len())
	}

	// key1 should still exist (was accessed)
	_, ok := cache.Get("key1")
	if !ok {
		t.Error("expected key1 to exist after eviction")
	}

	// key2 should be evicted (oldest, not accessed)
	_, ok = cache.Get("key2")
	if ok {
		t.Error("expected key2 to be evicted")
	}

	// key3 and key4 should exist
	_, ok = cache.Get("key3")
	if !ok {
		t.Error("expected key3 to exist")
	}
	_, ok = cache.Get("key4")
	if !ok {
		t.Error("expected key4 to exist")
	}
}

func TestQueryCache_Clear(t *testing.T) {
	cache := NewQueryCache(10, time.Hour)

	cache.Set("key1", "value1")
	cache.Set("key2", "value2")

	if cache.Len() != 2 {
		t.Errorf("expected len 2, got %d", cache.Len())
	}

	cache.Clear()

	if cache.Len() != 0 {
		t.Errorf("expected len 0 after clear, got %d", cache.Len())
	}

	_, ok := cache.Get("key1")
	if ok {
		t.Error("expected cache miss after clear")
	}
}

func TestQueryCache_KeyBuilders(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		key      string
		expected string
	}{
		{
			name:     "LabelsCardinalityKey without metric",
			key:      LabelsCardinalityKey(start, end, 10, ""),
			expected: "labels:2024-01-01:2024-01-31:10:",
		},
		{
			name:     "LabelsCardinalityKey with metric",
			key:      LabelsCardinalityKey(start, end, 10, "http_requests"),
			expected: "labels:2024-01-01:2024-01-31:10:http_requests",
		},
		{
			name:     "LabelsCardinalityPerDayKey",
			key:      LabelsCardinalityPerDayKey(start, end, 20, ""),
			expected: "labels-per-day:2024-01-01:2024-01-31:20:",
		},
		{
			name:     "CardinalityKey",
			key:      CardinalityKey(start, end, 10),
			expected: "cardinality:2024-01-01:2024-01-31:10",
		},
		{
			name:     "MetricHistoryKey",
			key:      MetricHistoryKey("cpu_usage", start, end),
			expected: "metric-history:cpu_usage:2024-01-01:2024-01-31",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.key != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, tt.key)
			}
		})
	}
}

func TestQueryCache_ConcurrentAccess(t *testing.T) {
	cache := NewQueryCache(100, time.Hour)
	done := make(chan bool)

	// Concurrent writers
	for i := range 10 {
		go func(id int) {
			for range 100 {
				cache.Set("key", id*100)
			}
			done <- true
		}(i)
	}

	// Concurrent readers
	for range 10 {
		go func() {
			for range 100 {
				cache.Get("key")
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for range 20 {
		<-done
	}

	// Cache should still be functional
	cache.Set("final", "value")
	val, ok := cache.Get("final")
	if !ok || val != "value" {
		t.Error("cache not functional after concurrent access")
	}
}
