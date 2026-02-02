# :potable_water:  Waterway

A high-performance JSON-RPC 2.0 proxy for Sei blockchain nodes with WebSocket pooling, automatic HTTP fallback, and response caching.

## Features

- **Dual transport support** — Routes requests via WebSocket or HTTP based on method compatibility
- **WebSocket connection pooling** — Maintains a pool of persistent connections for low-latency requests
- **Automatic fallback** — Falls back to HTTP when WebSocket calls fail or return incompatible errors
- **Response caching** — Memcached integration for cacheable RPC methods
- **Batch requests** — Handles JSON-RPC batch requests with configurable concurrency limits
- **Method filtering** — Allowlist/blocklist support for controlling exposed RPC methods
- **Metrics & health checks** — Prometheus metrics endpoint and health status reporting
- **TLS support** — Optional HTTPS/WSS termination

## Quick Start

```go
waterway, err := NewWaterway(ctx,
    WithListenAddr(":8545"),
    WithSeiWSEndpoint("ws://localhost:26657/websocket"),
    WithSeiHTTPEndpoint("http://localhost:26657"),
)
if err != nil {
    log.Fatal(err)
}
waterway.Start(ctx)
```

## Endpoints

| Path | Description |
|------|-------------|
| `/` | JSON-RPC endpoint (HTTP POST or WebSocket upgrade) |
| `/health` | Health check with pool status |
| `/metrics` | Prometheus metrics |

## Configuration Options

Configure via `Option` functions passed to `NewWaterway()`:

- `WithListenAddr` — Server bind address
- `WithSeiWSEndpoint` — Upstream WebSocket URL
- `WithSeiHTTPEndpoint` — Upstream HTTP URL
- `WithAllowedMethods` / `WithBlockedMethods` — Method filtering
- `WithHTTPOnlyMethods` — Force specific methods to use HTTP
- `WithCacheableMethods` — Enable caching for specific methods
- `WithMemcachedServers` — Memcached server addresses
- `WithMaxWSConnections` — WebSocket pool size
- `WithTLS` — TLS certificate and key paths

## How It Works

1. Incoming requests are validated against JSON-RPC 2.0 spec and method filters
2. Cacheable methods check Memcached first
3. Subscription methods (`eth_subscribe`/`eth_unsubscribe`) always use WebSocket
4. Other methods try WebSocket first, falling back to HTTP on failure
5. Methods that consistently fail over WebSocket are automatically routed to HTTP
