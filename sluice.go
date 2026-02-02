package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

var logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))

// JSONRPCRequest represents a JSON-RPC 2.0 request
type JSONRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      json.RawMessage `json:"id"`
}

// JSONRPCResponse represents a JSON-RPC 2.0 response
type JSONRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *JSONRPCError   `json:"error,omitempty"`
	ID      json.RawMessage `json:"id"`
}

// JSONRPCError represents a JSON-RPC 2.0 error
type JSONRPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// JSON-RPC error codes
const (
	ErrCodeParse          = -32700
	ErrCodeInvalidRequest = -32600
	ErrCodeMethodNotFound = -32601
	ErrCodeInternal       = -32603
	ErrCodeRateLimited    = -32005
	ErrCodeForbidden      = -32006
)

func newErrorResponse(id json.RawMessage, code int, message string) *JSONRPCResponse {
	return &JSONRPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error:   &JSONRPCError{Code: code, Message: message},
	}
}

// Waterway is the main proxy server
type Waterway struct {
	*options

	wsPool          *WSPool
	httpClient      *http.Client
	upgrader        websocket.Upgrader
	cache           *MemcachedCache
	wsFailedMethods sync.Map
}

// NewWaterway creates a new Waterway proxy instance
func NewWaterway(ctx context.Context, opt ...Option) (*Waterway, error) {
	opts, err := newOptions(opt...)
	if err != nil {
		return nil, err
	}

	s := &Waterway{
		options: opts,
		cache:   NewMemcachedCache(opts.cacheableMethods, opts.memcachedServers...),
		httpClient: &http.Client{
			Timeout: opts.httpTimeout,
			Transport: &http.Transport{
				DialContext:         (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
				TLSHandshakeTimeout: 10 * time.Second,
				TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS12},
			},
		},
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			CheckOrigin:     makeOriginChecker(opts.allowedOrigins),
		},
	}
	s.wsPool = NewWSPool(
		opts.seiWSEndpoint,
		opts.maxWSConnections,
		opts.wsTimeout,
		opts.connectionMaxAge,
		opts.wsPingInterval,
		opts.wsMaxMessageSize,
	)
	return s, nil
}

func makeOriginChecker(allowed []string) func(*http.Request) bool {
	if len(allowed) == 0 {
		return func(r *http.Request) bool { return true }
	}
	set := make(map[string]bool)
	for _, o := range allowed {
		set[strings.ToLower(o)] = true
	}
	return func(r *http.Request) bool { return set[strings.ToLower(r.Header.Get("Origin"))] }
}

func (s *Waterway) validateRequest(req *JSONRPCRequest) *JSONRPCError {
	if req.JSONRPC != "2.0" {
		return &JSONRPCError{Code: ErrCodeInvalidRequest, Message: "Invalid JSON-RPC version"}
	}
	if req.Method == "" {
		return &JSONRPCError{Code: ErrCodeInvalidRequest, Message: "Method required"}
	}
	if s.blockedMethods[req.Method] {
		metrics.requestsBlocked.Add(context.Background(), 1, otelmetric.WithAttributes(
			attribute.String("method", req.Method),
		))
		return &JSONRPCError{Code: ErrCodeForbidden, Message: "Method not allowed"}
	}
	if s.allowedMethods != nil && !s.allowedMethods[req.Method] {
		metrics.requestsBlocked.Add(context.Background(), 1, otelmetric.WithAttributes(
			attribute.String("method", req.Method),
		))
		return &JSONRPCError{Code: ErrCodeForbidden, Message: "Method not allowed"}
	}
	return nil
}

func (s *Waterway) callHTTP(ctx context.Context, req *JSONRPCRequest) (*JSONRPCResponse, error) {
	defer func(start time.Time) {
		metrics.upstreamDuration.Record(ctx, time.Since(start).Seconds(), otelmetric.WithAttributes(
			attribute.String("transport", "http"),
		))
	}(time.Now())

	data, _ := json.Marshal(req)
	httpReq, _ := http.NewRequestWithContext(ctx, "POST", s.seiHTTPEndpoint, bytes.NewReader(data))
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, s.maxRequestSize*10)) // Allow up to 10x request size as response
	metrics.responseSize.Record(ctx, int64(len(body)))

	var rpcResp JSONRPCResponse
	if json.Unmarshal(body, &rpcResp) != nil {
		return nil, errors.New("invalid JSON response")
	}
	return &rpcResp, nil
}

func (s *Waterway) callWS(ctx context.Context, req *JSONRPCRequest) (*JSONRPCResponse, error) {
	defer func(start time.Time) {
		metrics.upstreamDuration.Record(ctx, time.Since(start).Seconds(), otelmetric.WithAttributes(
			attribute.String("transport", "ws"),
		))
	}(time.Now())

	conn, err := s.wsPool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer s.wsPool.Put(ctx, conn)
	return conn.Call(ctx, req, s.wsTimeout)
}

func (s *Waterway) shouldUseHTTP(method string) bool {
	if s.httpOnlyMethods[method] {
		return true
	}
	_, failed := s.wsFailedMethods.Load(method)
	return failed
}

func (s *Waterway) call(ctx context.Context, req *JSONRPCRequest) (*JSONRPCResponse, error) {
	transport, status := "ws", "success"
	defer func(start time.Time) { recordRequest(ctx, req.Method, transport, status, time.Since(start)) }(time.Now())

	if err := s.validateRequest(req); err != nil {
		status = "blocked"
		return &JSONRPCResponse{JSONRPC: "2.0", ID: req.ID, Error: err}, nil
	}

	if cached, hit := s.cache.Get(ctx, req); hit {
		metrics.requestsCacheHit.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("method", req.Method),
		))
		transport = "cache"
		return cached, nil
	}

	metrics.requestsCacheMiss.Add(ctx, 1, otelmetric.WithAttributes(
		attribute.String("method", req.Method),
	))

	if req.Method == "eth_subscribe" || req.Method == "eth_unsubscribe" {
		resp, err := s.callWS(ctx, req)
		if err != nil {
			status = "error"
		}
		return resp, err
	}

	if s.shouldUseHTTP(req.Method) {
		transport = "http"
		resp, err := s.callHTTP(ctx, req)
		if err != nil {
			status = "error"
			return nil, err
		}
		s.cache.Set(ctx, req, resp)
		return resp, nil
	}

	resp, err := s.callWS(ctx, req)
	if err != nil {
		metrics.fallbackTotal.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("method", req.Method),
		))
		transport = "http_fallback"
		resp, err = s.callHTTP(ctx, req)
		if err != nil {
			status = "error"
			return nil, err
		}
		s.cache.Set(ctx, req, resp)
		return resp, nil
	}

	if resp.Error != nil && isWSIncompatibleError(resp.Error) {
		s.wsFailedMethods.Store(req.Method, true)
		metrics.fallbackTotal.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("method", req.Method),
		))
		transport = "http_fallback"
		resp, err = s.callHTTP(ctx, req)
		if err != nil {
			status = "error"
			return nil, err
		}
	}
	s.cache.Set(ctx, req, resp)
	return resp, nil
}

func isWSIncompatibleError(err *JSONRPCError) bool {
	if err.Code == ErrCodeMethodNotFound {
		return true
	}
	msg := strings.ToLower(err.Message)
	for _, p := range []string{"not supported", "not available", "use http"} {
		if strings.Contains(msg, p) {
			return true
		}
	}
	return false
}

func (s *Waterway) handleHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		s.setCORS(w)
		return
	}
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, _ := io.ReadAll(io.LimitReader(r.Body, s.maxRequestSize))
	metrics.requestSize.Record(r.Context(), int64(len(body)))
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		http.Error(w, "Empty body", http.StatusBadRequest)
		return
	}

	s.setCORS(w)
	w.Header().Set("Content-Type", "application/json")

	if body[0] == '[' {
		var batch []JSONRPCRequest
		if json.Unmarshal(body, &batch) != nil || len(batch) == 0 {
			json.NewEncoder(w).Encode(newErrorResponse(nil, ErrCodeParse, "Parse error"))
			return
		}
		if len(batch) > s.maxConcurrentBatch {
			json.NewEncoder(w).Encode(newErrorResponse(nil, ErrCodeInvalidRequest, "Batch too large"))
			return
		}
		responses := make([]*JSONRPCResponse, len(batch))
		var wg sync.WaitGroup
		sem := make(chan struct{}, s.maxConcurrentBatch)
		for i, req := range batch {
			wg.Add(1)
			sem <- struct{}{}
			go func(idx int, r JSONRPCRequest) {
				defer func() { <-sem; wg.Done() }()
				resp, err := s.call(context.Background(), &r)
				if err != nil {
					responses[idx] = newErrorResponse(r.ID, ErrCodeInternal, "Internal error")
				} else {
					responses[idx] = resp
				}
			}(i, req)
		}
		wg.Wait()
		_ = json.NewEncoder(w).Encode(responses)
	} else {
		var req JSONRPCRequest
		if json.Unmarshal(body, &req) != nil {
			_ = json.NewEncoder(w).Encode(newErrorResponse(nil, ErrCodeParse, "Parse error"))
			return
		}
		resp, err := s.call(r.Context(), &req)
		if err != nil {
			resp = newErrorResponse(req.ID, ErrCodeInternal, "Internal error")
		}
		_ = json.NewEncoder(w).Encode(resp)
	}
}

func (s *Waterway) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()

	metrics.activeConnections.Add(r.Context(), 1)
	defer metrics.activeConnections.Add(r.Context(), -1)

	conn.SetReadLimit(s.wsMaxMessageSize)
	_ = conn.SetReadDeadline(time.Now().Add(s.wsTimeout))
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(s.wsTimeout))
		return nil
	})

	pingTicker := time.NewTicker(s.wsPingInterval)
	defer pingTicker.Stop()
	done := make(chan struct{})
	writeMu := &sync.Mutex{}

	go func() {
		for {
			select {
			case <-done:
				return
			case <-pingTicker.C:
				writeMu.Lock()
				_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				_ = conn.WriteMessage(websocket.PingMessage, nil)
				writeMu.Unlock()
			}
		}
	}()
	defer close(done)

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(s.wsTimeout))
		metrics.requestSize.Record(context.Background(), int64(len(msg)))
		msg = bytes.TrimSpace(msg)
		if len(msg) == 0 {
			continue
		}

		var responses []byte
		if msg[0] == '[' {
			var batch []JSONRPCRequest
			if json.Unmarshal(msg, &batch) != nil {
				responses, _ = json.Marshal(newErrorResponse(nil, ErrCodeParse, "Parse error"))
			} else if len(batch) > s.maxConcurrentBatch {
				responses, _ = json.Marshal(newErrorResponse(nil, ErrCodeInvalidRequest, "Batch too large"))
			} else {
				resps := make([]*JSONRPCResponse, len(batch))
				var wg sync.WaitGroup
				for i, req := range batch {
					wg.Add(1)
					go func(idx int, r JSONRPCRequest) {
						defer wg.Done()
						resp, err := s.call(context.Background(), &r)
						if err != nil {
							resps[idx] = newErrorResponse(r.ID, ErrCodeInternal, "Internal error")
						} else {
							resps[idx] = resp
						}
					}(i, req)
				}
				wg.Wait()
				responses, _ = json.Marshal(resps)
			}
		} else {
			var req JSONRPCRequest
			if json.Unmarshal(msg, &req) != nil {
				responses, _ = json.Marshal(newErrorResponse(nil, ErrCodeParse, "Parse error"))
			} else {
				resp, err := s.call(context.Background(), &req)
				if err != nil {
					resp = newErrorResponse(req.ID, ErrCodeInternal, "Internal error")
				}
				responses, _ = json.Marshal(resp)
			}
		}

		metrics.responseSize.Record(context.Background(), int64(len(responses)))
		writeMu.Lock()
		_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		_ = conn.WriteMessage(websocket.TextMessage, responses)
		writeMu.Unlock()
	}
}

func (s *Waterway) setCORS(w http.ResponseWriter) {
	if len(s.allowedOrigins) == 0 {
		w.Header().Set("Access-Control-Allow-Origin", "*")
	} else {
		w.Header().Set("Access-Control-Allow-Origin", strings.Join(s.allowedOrigins, ", "))
	}
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
}

func (s *Waterway) handleHealth(w http.ResponseWriter, r *http.Request) {
	status := map[string]interface{}{
		"status":    "ok",
		"ws_pool":   s.wsPool.IsHealthy(),
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	if !s.wsPool.IsHealthy() {
		status["status"] = "degraded"
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// Start starts the proxy server
func (s *Waterway) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if websocket.IsWebSocketUpgrade(r) {
			s.handleWS(w, r)
		} else {
			s.handleHTTP(w, r)
		}
	})
	mux.HandleFunc("/health", s.handleHealth)
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:              s.listenAddr,
		Handler:           mux,
		ReadTimeout:       30 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 16,
	}
	go func() {
		<-ctx.Done()
		logger.Info("shutting down...")
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		s.wsPool.Close()
		_ = server.Shutdown(ctx)
	}()

	logger.Info("starting waterway",
		"addr", s.listenAddr,
		"ws", s.seiWSEndpoint,
		"http", s.seiHTTPEndpoint,
	)

	if s.tlsCert != "" && s.tlsKey != "" {
		return server.ListenAndServeTLS(s.tlsCert, s.tlsKey)
	}
	return server.ListenAndServe()
}

func extractID(id json.RawMessage) uint64 {
	if id == nil {
		return 0
	}
	var num float64
	if json.Unmarshal(id, &num) == nil {
		return uint64(num)
	}
	var str string
	if json.Unmarshal(id, &str) == nil {
		if i, err := strconv.ParseUint(str, 10, 64); err == nil {
			return i
		}
	}
	return 0
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
