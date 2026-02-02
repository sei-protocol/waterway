package main

import "time"

// Option is a function that configures the application
type Option func(*options) error

type options struct {
	listenAddr      string
	tlsCert         string
	tlsKey          string
	seiWSEndpoint   string
	seiHTTPEndpoint string

	allowedOrigins  []string
	allowedMethods  map[string]bool
	blockedMethods  map[string]bool
	maxRequestSize  int64
	rateLimitPerIP  float64
	rateLimitBurst  int
	httpOnlyMethods map[string]bool

	maxWSConnections   int
	wsTimeout          time.Duration
	httpTimeout        time.Duration
	wsPingInterval     time.Duration
	wsMaxMessageSize   int64
	maxConcurrentBatch int
	connectionMaxAge   time.Duration

	memcachedServers []string
	cacheEnabled     bool
	cacheTTL         time.Duration
	cacheableMethods map[string]MethodCache
}

func newOptions(opt ...Option) (*options, error) {
	opts := &options{
		listenAddr:      "localhost:8555",
		seiWSEndpoint:   "ws://localhost:8546",
		seiHTTPEndpoint: "http://localhost:8545",
		blockedMethods: map[string]bool{
			"admin_addPeer": true, "admin_removePeer": true, "admin_startRPC": true,
			"admin_stopRPC": true, "admin_startWS": true, "admin_stopWS": true,
			"miner_start": true, "miner_stop": true, "personal_unlockAccount": true,
			"personal_sendTransaction": true, "personal_sign": true, "debug_setHead": true,
			"debug_gcStats": true, "debug_memStats": true, "debug_cpuProfile": true,
			"debug_startCPUProfile": true, "debug_stopCPUProfile": true,
			"debug_writeBlockProfile": true, "debug_writeMemProfile": true,
			"debug_setBlockProfileRate": true, "debug_setGCPercent": true,
			"debug_setMutexProfileFraction": true,
		},
		maxRequestSize: 1 << 20,
		rateLimitPerIP: 100,
		rateLimitBurst: 200,
		httpOnlyMethods: map[string]bool{
			"debug_traceTransaction": true, "debug_traceCall": true,
			"debug_traceBlockByNumber": true, "debug_traceBlockByHash": true,
			"trace_block": true, "trace_transaction": true, "trace_call": true,
			"trace_replayTransaction": true, "trace_replayBlockTransactions": true,
			"eth_getLogs": true,
		},
		maxWSConnections:   20,
		wsTimeout:          30 * time.Second,
		httpTimeout:        60 * time.Second,
		wsPingInterval:     30 * time.Second,
		wsMaxMessageSize:   4 << 20,
		maxConcurrentBatch: 50,
		connectionMaxAge:   5 * time.Minute,
		cacheEnabled:       false,
		cacheTTL:           5 * time.Second,
		cacheableMethods: map[string]MethodCache{
			"eth_getBlockByHash": {TTL: 24 * time.Hour}, "eth_getBlockByNumber": {TTL: 5 * time.Second},
			"eth_getBlockTransactionCountByHash": {TTL: 24 * time.Hour}, "eth_getBlockTransactionCountByNumber": {TTL: 5 * time.Second},
			"eth_getTransactionByHash": {TTL: 24 * time.Hour}, "eth_getTransactionReceipt": {TTL: 24 * time.Hour},
			"eth_getCode": {TTL: 1 * time.Hour}, "eth_chainId": {TTL: 1 * time.Hour},
			"net_version": {TTL: 1 * time.Hour}, "web3_clientVersion": {TTL: 1 * time.Hour},
			"eth_blockNumber": {TTL: 1 * time.Second}, "eth_gasPrice": {TTL: 3 * time.Second},
			"eth_feeHistory": {TTL: 3 * time.Second}, "eth_getBalance": {TTL: 2 * time.Second},
			"eth_getStorageAt": {TTL: 2 * time.Second}, "eth_getTransactionCount": {TTL: 2 * time.Second},
			"eth_call": {TTL: 2 * time.Second}, "eth_estimateGas": {TTL: 2 * time.Second},
		},
	}

	for _, apply := range opt {
		if err := apply(opts); err != nil {
			return nil, err
		}
	}
	return opts, nil
}

// WithListenAddr sets the address to listen on
func WithListenAddr(addr string) Option {
	return func(o *options) error {
		o.listenAddr = addr
		return nil
	}
}

// WithTLS sets TLS certificate and key paths
func WithTLS(certPath, keyPath string) Option {
	return func(o *options) error {
		o.tlsCert = certPath
		o.tlsKey = keyPath
		return nil
	}
}

// WithSeiWSEndpoint sets the Sei WebSocket endpoint
func WithSeiWSEndpoint(endpoint string) Option {
	return func(o *options) error {
		o.seiWSEndpoint = endpoint
		return nil
	}
}

// WithSeiHTTPEndpoint sets the Sei HTTP endpoint
func WithSeiHTTPEndpoint(endpoint string) Option {
	return func(o *options) error {
		o.seiHTTPEndpoint = endpoint
		return nil
	}
}

// WithAllowedOrigins sets the allowed CORS origins
func WithAllowedOrigins(origins []string) Option {
	return func(o *options) error {
		o.allowedOrigins = origins
		return nil
	}
}

// WithAllowedMethods sets the allowed RPC methods (whitelist)
func WithAllowedMethods(methods []string) Option {
	return func(o *options) error {
		o.allowedMethods = make(map[string]bool)
		for _, m := range methods {
			o.allowedMethods[m] = true
		}
		return nil
	}
}

// WithBlockedMethods adds methods to the blocklist
func WithBlockedMethods(methods []string) Option {
	return func(o *options) error {
		if o.blockedMethods == nil {
			o.blockedMethods = make(map[string]bool)
		}
		for _, m := range methods {
			o.blockedMethods[m] = true
		}
		return nil
	}
}

// WithMaxRequestSize sets the maximum request body size
func WithMaxRequestSize(size int64) Option {
	return func(o *options) error {
		o.maxRequestSize = size
		return nil
	}
}

// WithRateLimit sets the per-IP rate limit
func WithRateLimit(rps float64, burst int) Option {
	return func(o *options) error {
		o.rateLimitPerIP = rps
		o.rateLimitBurst = burst
		return nil
	}
}

// WithHTTPOnlyMethods sets methods that should only use HTTP
func WithHTTPOnlyMethods(methods []string) Option {
	return func(o *options) error {
		o.httpOnlyMethods = make(map[string]bool)
		for _, m := range methods {
			o.httpOnlyMethods[m] = true
		}
		return nil
	}
}

// WithMaxWSConnections sets the maximum WebSocket pool size
func WithMaxWSConnections(max int) Option {
	return func(o *options) error {
		o.maxWSConnections = max
		return nil
	}
}

// WithWSTimeout sets the WebSocket operation timeout
func WithWSTimeout(timeout time.Duration) Option {
	return func(o *options) error {
		o.wsTimeout = timeout
		return nil
	}
}

// WithHTTPTimeout sets the HTTP client timeout
func WithHTTPTimeout(timeout time.Duration) Option {
	return func(o *options) error {
		o.httpTimeout = timeout
		return nil
	}
}

// WithWSPingInterval sets the WebSocket ping interval
func WithWSPingInterval(interval time.Duration) Option {
	return func(o *options) error {
		o.wsPingInterval = interval
		return nil
	}
}

// WithWSMaxMessageSize sets the maximum WebSocket message size
func WithWSMaxMessageSize(size int64) Option {
	return func(o *options) error {
		o.wsMaxMessageSize = size
		return nil
	}
}

// WithMaxConcurrentBatch sets the maximum concurrent batch requests
func WithMaxConcurrentBatch(max int) Option {
	return func(o *options) error {
		o.maxConcurrentBatch = max
		return nil
	}
}

// WithConnectionMaxAge sets the maximum connection age
func WithConnectionMaxAge(age time.Duration) Option {
	return func(o *options) error {
		o.connectionMaxAge = age
		return nil
	}
}

// WithMemcachedServers sets the memcached server addresses
func WithMemcachedServers(servers []string) Option {
	return func(o *options) error {
		o.memcachedServers = servers
		return nil
	}
}

// WithCacheEnabled enables or disables caching
func WithCacheEnabled(enabled bool) Option {
	return func(o *options) error {
		o.cacheEnabled = enabled
		return nil
	}
}

// WithCacheTTL sets the default cache TTL
func WithCacheTTL(ttl time.Duration) Option {
	return func(o *options) error {
		o.cacheTTL = ttl
		return nil
	}
}

// WithCacheableMethod adds a cacheable method configuration
func WithCacheableMethod(method string, ttl time.Duration, keyIncludesID bool) Option {
	return func(o *options) error {
		if o.cacheableMethods == nil {
			o.cacheableMethods = make(map[string]MethodCache)
		}
		o.cacheableMethods[method] = MethodCache{TTL: ttl, KeyIncludesID: keyIncludesID}
		return nil
	}
}
