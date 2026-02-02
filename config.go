package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the YAML configuration file structure
type Config struct {
	Server    ServerConfig    `yaml:"server"`
	Backend   BackendConfig   `yaml:"backend"`
	Security  SecurityConfig  `yaml:"security"`
	WebSocket WebSocketConfig `yaml:"websocket"`
	HTTP      HTTPConfig      `yaml:"http"`
	Cache     CacheConfig     `yaml:"cache"`
	Metrics   MetricsConfig   `yaml:"metrics"`
}

type ServerConfig struct {
	ListenAddr string `yaml:"listen_addr"`
	TLSCert    string `yaml:"tls_cert"`
	TLSKey     string `yaml:"tls_key"`
}

type BackendConfig struct {
	SeiWSEndpoint   string `yaml:"sei_ws_endpoint"`
	SeiHTTPEndpoint string `yaml:"sei_http_endpoint"`
}

type SecurityConfig struct {
	AllowedOrigins []string `yaml:"allowed_origins"`
	AllowedMethods []string `yaml:"allowed_methods"`
	BlockedMethods []string `yaml:"blocked_methods"`
	MaxRequestSize int64    `yaml:"max_request_size"`
	RateLimitPerIP float64  `yaml:"rate_limit_per_ip"`
	RateLimitBurst int      `yaml:"rate_limit_burst"`
}

type WebSocketConfig struct {
	MaxConnections   int           `yaml:"max_connections"`
	Timeout          time.Duration `yaml:"timeout"`
	PingInterval     time.Duration `yaml:"ping_interval"`
	MaxMessageSize   int64         `yaml:"max_message_size"`
	ConnectionMaxAge time.Duration `yaml:"connection_max_age"`
}

type HTTPConfig struct {
	Timeout            time.Duration `yaml:"timeout"`
	HTTPOnlyMethods    []string      `yaml:"http_only_methods"`
	MaxConcurrentBatch int           `yaml:"max_concurrent_batch"`
}

type CacheConfig struct {
	Enabled          bool                   `yaml:"enabled"`
	MemcachedServers []string               `yaml:"memcached_servers"`
	DefaultTTL       time.Duration          `yaml:"default_ttl"`
	Methods          map[string]MethodCache `yaml:"methods"`
}

type MethodCache struct {
	TTL           time.Duration `yaml:"ttl"`
	KeyIncludesID bool          `yaml:"key_includes_id"`
}

type MetricsConfig struct {
	OTLPEndpoint      string `yaml:"otlp_endpoint"`
	PrometheusEnabled bool   `yaml:"prometheus_enabled"`
	Prefix            string `yaml:"prefix"`
}

// LoadConfig reads a YAML config file and returns Option functions
func LoadConfig(path string) ([]Option, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	return cfg.ToOptions(), nil
}

// ToOptions converts the Config to a slice of Option functions
func (c *Config) ToOptions() []Option {
	var opts []Option

	// Server options
	if c.Server.ListenAddr != "" {
		opts = append(opts, WithListenAddr(c.Server.ListenAddr))
	}
	if c.Server.TLSCert != "" && c.Server.TLSKey != "" {
		opts = append(opts, WithTLS(c.Server.TLSCert, c.Server.TLSKey))
	}

	// Backend options
	if c.Backend.SeiWSEndpoint != "" {
		opts = append(opts, WithSeiWSEndpoint(c.Backend.SeiWSEndpoint))
	}
	if c.Backend.SeiHTTPEndpoint != "" {
		opts = append(opts, WithSeiHTTPEndpoint(c.Backend.SeiHTTPEndpoint))
	}

	// Security options
	if len(c.Security.AllowedOrigins) > 0 {
		opts = append(opts, WithAllowedOrigins(c.Security.AllowedOrigins))
	}
	if len(c.Security.AllowedMethods) > 0 {
		opts = append(opts, WithAllowedMethods(c.Security.AllowedMethods))
	}
	if len(c.Security.BlockedMethods) > 0 {
		opts = append(opts, WithBlockedMethods(c.Security.BlockedMethods))
	}
	if c.Security.MaxRequestSize > 0 {
		opts = append(opts, WithMaxRequestSize(c.Security.MaxRequestSize))
	}
	if c.Security.RateLimitPerIP > 0 || c.Security.RateLimitBurst > 0 {
		opts = append(opts, WithRateLimit(c.Security.RateLimitPerIP, c.Security.RateLimitBurst))
	}

	// WebSocket options
	if c.WebSocket.MaxConnections > 0 {
		opts = append(opts, WithMaxWSConnections(c.WebSocket.MaxConnections))
	}
	if c.WebSocket.Timeout > 0 {
		opts = append(opts, WithWSTimeout(c.WebSocket.Timeout))
	}
	if c.WebSocket.PingInterval > 0 {
		opts = append(opts, WithWSPingInterval(c.WebSocket.PingInterval))
	}
	if c.WebSocket.MaxMessageSize > 0 {
		opts = append(opts, WithWSMaxMessageSize(c.WebSocket.MaxMessageSize))
	}
	if c.WebSocket.ConnectionMaxAge > 0 {
		opts = append(opts, WithConnectionMaxAge(c.WebSocket.ConnectionMaxAge))
	}

	// HTTP options
	if c.HTTP.Timeout > 0 {
		opts = append(opts, WithHTTPTimeout(c.HTTP.Timeout))
	}
	if len(c.HTTP.HTTPOnlyMethods) > 0 {
		opts = append(opts, WithHTTPOnlyMethods(c.HTTP.HTTPOnlyMethods))
	}
	if c.HTTP.MaxConcurrentBatch > 0 {
		opts = append(opts, WithMaxConcurrentBatch(c.HTTP.MaxConcurrentBatch))
	}

	// MemcachedCache options
	opts = append(opts, WithCacheEnabled(c.Cache.Enabled))
	if len(c.Cache.MemcachedServers) > 0 {
		opts = append(opts, WithMemcachedServers(c.Cache.MemcachedServers))
	}
	if c.Cache.DefaultTTL > 0 {
		opts = append(opts, WithCacheTTL(c.Cache.DefaultTTL))
	}
	for method, cfg := range c.Cache.Methods {
		opts = append(opts, WithCacheableMethod(method, cfg.TTL, cfg.KeyIncludesID))
	}

	return opts
}
