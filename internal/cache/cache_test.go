package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/namburisnehitha/cluster-pulse/internal/cache"
)

func setupRedis(t *testing.T) (*cache.Redis, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	t.Cleanup(func() { mr.Close() })

	c, err := cache.NewRedis(context.Background(), mr.Addr(), "")
	if err != nil {
		t.Fatalf("failed to create redis client: %v", err)
	}
	return c, mr
}

func TestSet(t *testing.T) {

	// Situation 1: set a new key — should return no error
	c, _ := setupRedis(t)
	err := c.Set(context.Background(), "key1", "value1", time.Minute)
	if err != nil {
		t.Errorf("set new key: got %v, want nil", err)
	}

	// Situation 2: overwrite existing key — should return no error, value should change
	c, _ = setupRedis(t)
	err = c.Set(context.Background(), "key1", "value1", time.Minute)
	if err != nil {
		t.Fatalf("overwrite setup: got %v, want nil", err)
	}
	err = c.Set(context.Background(), "key1", "new-value", time.Minute)
	if err != nil {
		t.Errorf("overwrite: got %v, want nil", err)
	}
	val, err := c.Get(context.Background(), "key1")
	if err != nil {
		t.Fatalf("overwrite get: got %v, want nil", err)
	}
	if val != "new-value" {
		t.Errorf("overwrite: got %s, want new-value", val)
	}

	// Situation 3: set empty string value — should store and retrieve correctly
	c, _ = setupRedis(t)
	err = c.Set(context.Background(), "empty-key", "", time.Minute)
	if err != nil {
		t.Errorf("empty string: got %v, want nil", err)
	}
	val, err = c.Get(context.Background(), "empty-key")
	if err != nil {
		t.Fatalf("empty string get: got %v, want nil", err)
	}
	if val != "" {
		t.Errorf("empty string: got %q, want empty string", val)
	}

	// Situation 4: set with TTL — key should expire after TTL elapses
	c, mr := setupRedis(t)
	err = c.Set(context.Background(), "ttl-key", "value", time.Second)
	if err != nil {
		t.Fatalf("ttl setup: got %v, want nil", err)
	}
	mr.FastForward(2 * time.Second)
	_, err = c.Get(context.Background(), "ttl-key")
	if err == nil {
		t.Errorf("ttl expired: got nil, want error — key should have expired")
	}

	// Situation 5: overwrite with shorter TTL — new TTL should win, not original
	c, mr = setupRedis(t)
	err = c.Set(context.Background(), "key1", "value1", 10*time.Second)
	if err != nil {
		t.Fatalf("long ttl setup: got %v, want nil", err)
	}
	err = c.Set(context.Background(), "key1", "value2", time.Second)
	if err != nil {
		t.Fatalf("short ttl overwrite: got %v, want nil", err)
	}
	mr.FastForward(2 * time.Second)
	_, err = c.Get(context.Background(), "key1")
	if err == nil {
		t.Errorf("short ttl overwrite: got nil, want error — shorter TTL should have expired key")
	}
}

func TestGet(t *testing.T) {

	// Situation 1: get existing key — returns correct value, no error
	c, _ := setupRedis(t)
	err := c.Set(context.Background(), "key1", "value1", time.Minute)
	if err != nil {
		t.Fatalf("setup: got %v, want nil", err)
	}
	val, err := c.Get(context.Background(), "key1")
	if err != nil {
		t.Errorf("existing key: got %v, want nil", err)
	}
	if val != "value1" {
		t.Errorf("existing key: got %s, want value1", val)
	}

	// Situation 2: get non-existent key — should return error
	c, _ = setupRedis(t)
	val, err = c.Get(context.Background(), "missing")
	if err == nil {
		t.Errorf("missing key: got nil, want error")
	}
	if val != "" {
		t.Errorf("missing key: got %s, want empty string", val)
	}

	// Situation 3: get expired key — should return error after TTL elapses
	c, mr := setupRedis(t)
	err = c.Set(context.Background(), "expiring", "value", time.Second)
	if err != nil {
		t.Fatalf("expiring setup: got %v, want nil", err)
	}
	mr.FastForward(2 * time.Second)
	val, err = c.Get(context.Background(), "expiring")
	if err == nil {
		t.Errorf("expired key: got nil, want error")
	}
	if val != "" {
		t.Errorf("expired key: got %s, want empty string", val)
	}

	// Situation 4: get after overwrite — should return new value not old
	c, _ = setupRedis(t)
	err = c.Set(context.Background(), "key1", "old-value", time.Minute)
	if err != nil {
		t.Fatalf("overwrite setup: got %v, want nil", err)
	}
	err = c.Set(context.Background(), "key1", "new-value", time.Minute)
	if err != nil {
		t.Fatalf("overwrite: got %v, want nil", err)
	}
	val, err = c.Get(context.Background(), "key1")
	if err != nil {
		t.Errorf("after overwrite: got %v, want nil", err)
	}
	if val != "new-value" {
		t.Errorf("after overwrite: got %s, want new-value", val)
	}

	// Situation 5: get empty string value — empty string is a valid value, not an error
	c, _ = setupRedis(t)
	err = c.Set(context.Background(), "empty-key", "", time.Minute)
	if err != nil {
		t.Fatalf("empty string setup: got %v, want nil", err)
	}
	val, err = c.Get(context.Background(), "empty-key")
	if err != nil {
		t.Errorf("empty string: got %v, want nil", err)
	}
	if val != "" {
		t.Errorf("empty string: got %q, want empty string", val)
	}

	// Situation 6: get key just before expiry — should still return value
	c, mr = setupRedis(t)
	err = c.Set(context.Background(), "almost-expired", "value", 5*time.Second)
	if err != nil {
		t.Fatalf("almost expired setup: got %v, want nil", err)
	}
	mr.FastForward(4 * time.Second)
	val, err = c.Get(context.Background(), "almost-expired")
	if err != nil {
		t.Errorf("just before expiry: got %v, want nil — key should still be valid", err)
	}
	if val != "value" {
		t.Errorf("just before expiry: got %s, want value", val)
	}
}

func TestExists(t *testing.T) {

	// Situation 1: key exists — should return true, no error
	c, _ := setupRedis(t)
	err := c.Set(context.Background(), "key1", "value1", time.Minute)
	if err != nil {
		t.Fatalf("setup: got %v, want nil", err)
	}
	exists, err := c.Exists(context.Background(), "key1")
	if err != nil {
		t.Errorf("exists: got %v, want nil", err)
	}
	if !exists {
		t.Errorf("exists: got false, want true")
	}

	// Situation 2: key does not exist — should return false, no error
	c, _ = setupRedis(t)
	exists, err = c.Exists(context.Background(), "missing")
	if err != nil {
		t.Errorf("missing: got %v, want nil", err)
	}
	if exists {
		t.Errorf("missing: got true, want false")
	}

	// Situation 3: key exists then expires — should return false after TTL elapses
	c, mr := setupRedis(t)
	err = c.Set(context.Background(), "expiring", "value", time.Second)
	if err != nil {
		t.Fatalf("expiring setup: got %v, want nil", err)
	}
	exists, err = c.Exists(context.Background(), "expiring")
	if err != nil {
		t.Fatalf("expiring before: got %v, want nil", err)
	}
	if !exists {
		t.Errorf("expiring before: got false, want true")
	}
	mr.FastForward(2 * time.Second)
	exists, err = c.Exists(context.Background(), "expiring")
	if err != nil {
		t.Errorf("expiring after: got %v, want nil", err)
	}
	if exists {
		t.Errorf("expiring after: got true, want false")
	}

	// Situation 4: exists after overwrite — key still exists with new value
	c, _ = setupRedis(t)
	err = c.Set(context.Background(), "key1", "old-value", time.Minute)
	if err != nil {
		t.Fatalf("overwrite setup: got %v, want nil", err)
	}
	err = c.Set(context.Background(), "key1", "new-value", time.Minute)
	if err != nil {
		t.Fatalf("overwrite: got %v, want nil", err)
	}
	exists, err = c.Exists(context.Background(), "key1")
	if err != nil {
		t.Errorf("after overwrite: got %v, want nil", err)
	}
	if !exists {
		t.Errorf("after overwrite: got false, want true")
	}

	// Situation 5: multiple keys — exists check is key-specific, no prefix bleeding
	c, _ = setupRedis(t)
	err = c.Set(context.Background(), "analyzed:default/pod-1", "1", time.Minute)
	if err != nil {
		t.Fatalf("prefix setup: got %v, want nil", err)
	}
	exists, err = c.Exists(context.Background(), "analyzed:default/pod-10")
	if err != nil {
		t.Errorf("prefix bleed: got %v, want nil", err)
	}
	if exists {
		t.Errorf("prefix bleed: got true, want false — pod-10 should not match pod-1")
	}
}
