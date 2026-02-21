package htnstratum

import (
	"sync"
	"testing"
	"time"

	"github.com/Hoosat-Oy/HTND/app/appmessage"
	"go.uber.org/zap"
)

func newTestHtnApi(ttl time.Duration) *HtnApi {
	return &HtnApi{
		address:       "test-address",
		blockWaitTime: 100 * time.Millisecond,
		logger:        zap.NewNop().Sugar(),
		gbtCache:      make(map[string]*gbtCacheEntry),
		gbtCacheTTL:   ttl,
	}
}

func TestGbtCache_DisabledWhenTTLZero(t *testing.T) {
	api := newTestHtnApi(0)
	if api.gbtCacheTTL != 0 {
		t.Errorf("expected gbtCacheTTL 0, got %v", api.gbtCacheTTL)
	}
	if len(api.gbtCache) != 0 {
		t.Error("cache map should be empty on init")
	}
}

func TestGbtCache_InitialisedWhenTTLSet(t *testing.T) {
	api := newTestHtnApi(150 * time.Millisecond)
	if api.gbtCacheTTL != 150*time.Millisecond {
		t.Errorf("expected 150ms TTL, got %v", api.gbtCacheTTL)
	}
	if api.gbtCache == nil {
		t.Error("cache map must not be nil")
	}
}

func TestGbtCache_HitBeforeExpiry(t *testing.T) {
	api := newTestHtnApi(1 * time.Second)
	tmpl := &appmessage.GetBlockTemplateResponseMessage{IsSynced: true}

	api.gbtCacheMu.Lock()
	api.gbtCache["addr1"] = &gbtCacheEntry{template: tmpl, fetchedAt: time.Now()}
	api.gbtCacheMu.Unlock()

	api.gbtCacheMu.Lock()
	entry, ok := api.gbtCache["addr1"]
	hit := ok && time.Since(entry.fetchedAt) < api.gbtCacheTTL
	api.gbtCacheMu.Unlock()

	if !hit {
		t.Error("expected cache hit before TTL expiry")
	}
	if entry.template != tmpl {
		t.Error("cached template pointer mismatch")
	}
}

func TestGbtCache_MissAfterExpiry(t *testing.T) {
	api := newTestHtnApi(1 * time.Millisecond)
	tmpl := &appmessage.GetBlockTemplateResponseMessage{IsSynced: true}

	api.gbtCacheMu.Lock()
	api.gbtCache["addr1"] = &gbtCacheEntry{template: tmpl, fetchedAt: time.Now().Add(-10 * time.Millisecond)}
	api.gbtCacheMu.Unlock()

	api.gbtCacheMu.Lock()
	entry, ok := api.gbtCache["addr1"]
	hit := ok && time.Since(entry.fetchedAt) < api.gbtCacheTTL
	api.gbtCacheMu.Unlock()

	if hit {
		t.Error("expected cache miss after TTL expiry")
	}
}

func TestGbtCache_SeparateEntriesPerAddress(t *testing.T) {
	api := newTestHtnApi(1 * time.Second)
	tmpl1 := &appmessage.GetBlockTemplateResponseMessage{IsSynced: true}
	tmpl2 := &appmessage.GetBlockTemplateResponseMessage{IsSynced: false}

	now := time.Now()
	api.gbtCacheMu.Lock()
	api.gbtCache["miner_addr"] = &gbtCacheEntry{template: tmpl1, fetchedAt: now}
	api.gbtCache["bridge_addr"] = &gbtCacheEntry{template: tmpl2, fetchedAt: now}
	api.gbtCacheMu.Unlock()

	api.gbtCacheMu.Lock()
	e1, ok1 := api.gbtCache["miner_addr"]
	e2, ok2 := api.gbtCache["bridge_addr"]
	api.gbtCacheMu.Unlock()

	if !ok1 || !ok2 {
		t.Fatal("both addresses should have cache entries")
	}
	if e1.template == e2.template {
		t.Error("different addresses must not share the same cached template")
	}
	if e1.template != tmpl1 || e2.template != tmpl2 {
		t.Error("cached template pointers do not match expected values")
	}
}

func TestGbtCache_ConcurrentAccess(t *testing.T) {
	// Verify the mutex prevents data races under concurrent reads and writes.
	api := newTestHtnApi(500 * time.Millisecond)
	tmpl := &appmessage.GetBlockTemplateResponseMessage{IsSynced: true}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			api.gbtCacheMu.Lock()
			entry, ok := api.gbtCache["addr"]
			if !ok || time.Since(entry.fetchedAt) >= api.gbtCacheTTL {
				api.gbtCache["addr"] = &gbtCacheEntry{template: tmpl, fetchedAt: time.Now()}
			}
			api.gbtCacheMu.Unlock()
		}()
	}
	wg.Wait()

	api.gbtCacheMu.Lock()
	_, ok := api.gbtCache["addr"]
	api.gbtCacheMu.Unlock()
	if !ok {
		t.Error("cache entry should exist after concurrent writes")
	}
}

func TestGbtCache_CleanupRemovesExpiredEntries(t *testing.T) {
	api := newTestHtnApi(1 * time.Millisecond)
	tmpl := &appmessage.GetBlockTemplateResponseMessage{IsSynced: true}

	api.gbtCacheMu.Lock()
	api.gbtCache["addr1"] = &gbtCacheEntry{template: tmpl, fetchedAt: time.Now().Add(-10 * time.Millisecond)}
	api.gbtCache["addr2"] = &gbtCacheEntry{template: tmpl, fetchedAt: time.Now()}
	api.gbtCacheMu.Unlock()

	// Simulate what the cleanup goroutine does.
	api.gbtCacheMu.Lock()
	for addr, entry := range api.gbtCache {
		if time.Since(entry.fetchedAt) >= api.gbtCacheTTL {
			delete(api.gbtCache, addr)
		}
	}
	api.gbtCacheMu.Unlock()

	api.gbtCacheMu.Lock()
	_, expiredGone := api.gbtCache["addr1"]
	_, freshPresent := api.gbtCache["addr2"]
	api.gbtCacheMu.Unlock()

	if expiredGone {
		t.Error("expired entry should have been removed by cleanup")
	}
	if !freshPresent {
		t.Error("fresh entry should still be present after cleanup")
	}
}
