package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

type Cache interface {
	Get(context.Context, *JSONRPCRequest) (*JSONRPCResponse, bool)
	Set(context.Context, *JSONRPCRequest, *JSONRPCResponse)
}

var _ Cache = (*NoopCache)(nil)

type NoopCache struct{}

func (n NoopCache) Get(context.Context, *JSONRPCRequest) (*JSONRPCResponse, bool) { return nil, false }

func (n NoopCache) Set(context.Context, *JSONRPCRequest, *JSONRPCResponse) {}

// MemcachedCache provides caching functionality using memcached
type MemcachedCache struct {
	client     *memcache.Client
	cacheables map[string]MethodCache
}

// NewMemcachedCache creates a new cache instance
func NewMemcachedCache(cacheables map[string]MethodCache, memcachedServers ...string) *MemcachedCache {
	c := &MemcachedCache{
		client:     memcache.New(memcachedServers...),
		cacheables: cacheables,
	}
	c.client.Timeout = 100 * time.Millisecond
	c.client.MaxIdleConns = 10
	return c
}

// IsCacheable checks if a method is cacheable and returns its config
func (c *MemcachedCache) IsCacheable(method string) (MethodCache, bool) {
	cfg, ok := c.cacheables[method]
	return cfg, ok
}

// buildKey generates a cache key from the request
func (c *MemcachedCache) buildKey(req *JSONRPCRequest) string {
	h := sha256.New()
	h.Write([]byte(req.Method + ":"))
	if len(req.Params) > 0 {
		var params interface{}
		if json.Unmarshal(req.Params, &params) == nil {
			normalized, _ := json.Marshal(params)
			h.Write(normalized)
		} else {
			h.Write(req.Params)
		}
	}
	return "waterway:" + hex.EncodeToString(h.Sum(nil))[:32]
}

// Get retrieves a cached response
func (c *MemcachedCache) Get(_ context.Context, req *JSONRPCRequest) (*JSONRPCResponse, bool) {
	cfg, ok := c.IsCacheable(req.Method)
	if !ok {
		return nil, false
	}
	item, err := c.client.Get(c.buildKey(req))
	if err != nil {
		return nil, false
	}
	var cached JSONRPCResponse
	if json.Unmarshal(item.Value, &cached) != nil {
		return nil, false
	}
	if !cfg.KeyIncludesID {
		cached.ID = req.ID
	}
	return &cached, true
}

// Set stores a response in the cache
func (c *MemcachedCache) Set(_ context.Context, req *JSONRPCRequest, resp *JSONRPCResponse) {
	if resp.Error != nil {
		return
	}
	cfg, ok := c.IsCacheable(req.Method)
	if !ok {
		return
	}
	ttl := cfg.TTL
	if c.hasVolatileBlockTag(req) {
		ttl = min(ttl, 1*time.Second)
	}
	toCache := *resp
	if !cfg.KeyIncludesID {
		toCache.ID = nil
	}
	data, _ := json.Marshal(toCache)
	if err := c.client.Set(&memcache.Item{
		Key:        c.buildKey(req),
		Value:      data,
		Expiration: int32(ttl.Seconds()),
	}); err != nil {
		logger.Debug("cache set failed", "method", req.Method, "error", err)
	}
}

// hasVolatileBlockTag checks if the request contains volatile block tags
func (c *MemcachedCache) hasVolatileBlockTag(req *JSONRPCRequest) bool {
	if len(req.Params) == 0 {
		return false
	}
	p := strings.ToLower(string(req.Params))
	volatileTags := []string{`"latest"`, `"pending"`, `"earliest"`, `"safe"`, `"finalized"`}
	for _, tag := range volatileTags {
		if strings.Contains(p, tag) {
			return true
		}
	}
	return false
}

// Ping checks if the cache is healthy
func (c *MemcachedCache) Ping() error {
	return c.client.Set(&memcache.Item{
		Key:        "waterway:ping",
		Value:      []byte("pong"),
		Expiration: 1,
	})
}

// Flush clears all cache entries (if supported)
func (c *MemcachedCache) Flush() error {
	return c.client.FlushAll()
}
