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
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

var (
	logLevel = &slog.LevelVar{}
	logger   = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
)

// Waterway is the main proxy server
type Waterway struct {
	*options

	wsPool          *WSPool
	httpClient      *http.Client
	upgrader        websocket.Upgrader
	wsFailedMethods sync.Map
	server          http.Server
}

// NewWaterway creates a new Waterway proxy instance
func NewWaterway(opt ...Option) (*Waterway, error) {
	opts, err := newOptions(opt...)
	if err != nil {
		return nil, err
	}

	s := &Waterway{
		options: opts,
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
	}
	s.upgrader = websocket.Upgrader{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		CheckOrigin:     s.checkOrigin,
	}
	s.wsPool = NewWSPool(
		opts.seiWSEndpoint,
		opts.maxWSConnections,
		opts.wsReadTimeout,
		opts.connectionMaxAge,
		opts.wsPingInterval,
		opts.wsMaxMessageSize,
	)
	s.server.Addr = s.listenAddr
	s.server.Handler = s.ServeMux()
	s.server.ReadTimeout = 30 * time.Second
	s.server.ReadHeaderTimeout = 10 * time.Second
	s.server.WriteTimeout = 60 * time.Second
	s.server.IdleTimeout = 120 * time.Second
	s.server.RegisterOnShutdown(s.wsPool.Close)
	return s, nil
}

// Start starts the proxy server
func (s *Waterway) Start(ctx context.Context) error {
	s.server.BaseContext = func(net.Listener) context.Context { return ctx }
	go func() {
		logger.Info("Starting waterway", "addr", s.listenAddr, "ws", s.seiWSEndpoint, "http", s.seiHTTPEndpoint)
		var err error
		if s.tlsCert != "" && s.tlsKey != "" {
			err = s.server.ListenAndServeTLS(s.tlsCert, s.tlsKey)
		} else {
			err = s.server.ListenAndServe()
		}
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("Error serving HTTP", "err", err)
		}
	}()
	return nil
}

func (s *Waterway) ServeMux() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /", s.handleWS)
	mux.HandleFunc("OPTIONS /", s.handleHttpOptions)
	mux.HandleFunc("POST /", s.handleHTTPPost)

	mux.HandleFunc("GET /health", s.handleHealth)
	mux.Handle("GET /metrics", promhttp.Handler())
	return mux
}

func (s *Waterway) Shutdown(ctx context.Context) error {
	logger.Info("Shutting down...")
	return s.server.Shutdown(ctx)
}

func (s *Waterway) checkOrigin(r *http.Request) bool {
	if len(s.allowedOrigins) == 0 {
		return true
	}
	return s.allowedOrigins[strings.ToLower(r.Header.Get("Origin"))]
}

func (s *Waterway) validateRequest(req *JSONRPCRequest, requestedOverHttp bool) *JSONRPCError {
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
	wsOnly := req.Method == "eth_subscribe" || req.Method == "eth_unsubscribe"
	if wsOnly && requestedOverHttp {
		return &JSONRPCError{Code: ErrCodeInvalidRequest, Message: "Method requires WebSocket connection"}
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

	body, _ := io.ReadAll(io.LimitReader(resp.Body, s.maxRequestSize*10))
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
	return conn.Call(ctx, req, s.wsReadTimeout)
}

func (s *Waterway) shouldUseHTTP(method string) bool {
	if s.httpOnlyMethods[method] {
		return true
	}
	_, failed := s.wsFailedMethods.Load(method)
	return failed
}

func (s *Waterway) call(ctx context.Context, req *JSONRPCRequest, requestedOverHttp bool) (*JSONRPCResponse, error) {
	log := logger.With("method", req.Method, "id", req.ID)

	transport, status := "ws", "success"
	defer func(start time.Time) {
		recordRequest(ctx, req.Method, transport, status, time.Since(start))
	}(time.Now())

	if err := s.validateRequest(req, requestedOverHttp); err != nil {
		status = "invalid"
		log.Debug("Invalid request", "err", err)
		return &JSONRPCResponse{JSONRPC: "2.0", ID: req.ID, Error: err}, nil
	}

	if cached, hit := s.cache.Get(ctx, req); hit {
		transport = "cache"
		metrics.requestsCacheHit.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("method", req.Method),
		))
		return cached, nil
	}

	metrics.requestsCacheMiss.Add(ctx, 1, otelmetric.WithAttributes(
		attribute.String("method", req.Method),
	))

	// Handle subscriptions (WS-only, not cacheable)
	if req.Method == "eth_subscribe" || req.Method == "eth_unsubscribe" {
		resp, err := s.callWS(ctx, req)
		if err != nil {
			status = "error"
			log.Debug("Subscription call failed", "err", err)
		}
		return resp, err
	}

	// Route based on method compatibility
	if s.shouldUseHTTP(req.Method) {
		transport = "http"
		resp, err := s.callHTTP(ctx, req)
		if err != nil {
			status = "error"
			log.Debug("Call failed", "err", err)
			return nil, err
		}
		if resp.Error == nil {
			s.cache.Set(ctx, req, resp)
		}
		return resp, nil
	}

	fallbackToHttp := func() (*JSONRPCResponse, error) {
		transport = "http_fallback"

		metrics.fallbackTotal.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("method", req.Method),
		))

		resp, err := s.callHTTP(ctx, req)
		if err != nil {
			status = "error"
			log.Debug("HTTP fallback failed", "err", err)
			return nil, err
		}
		if resp.Error == nil {
			s.cache.Set(ctx, req, resp)
		}
		return resp, nil
	}

	// Try WS first, fall back to HTTP on failure
	resp, err := s.callWS(ctx, req)
	if err != nil {
		log.Debug("WS call failed, falling back to HTTP", "err", err)
		return fallbackToHttp()
	}

	if resp.Error != nil && isWSIncompatibleError(resp.Error) {
		log.Debug("WS incompatible response, falling back to HTTP",
			"error_code", resp.Error.Code,
			"error_message", resp.Error.Message)
		s.wsFailedMethods.Store(req.Method, true)
		return fallbackToHttp()
	}

	if resp.Error == nil {
		s.cache.Set(ctx, req, resp)
	}
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

func (s *Waterway) handleHttpOptions(w http.ResponseWriter, _ *http.Request) {
	s.setCORS(w)
}

func (s *Waterway) handleHTTPPost(w http.ResponseWriter, r *http.Request) {
	s.setCORS(w)
	w.Header().Set("Content-Type", "application/json")

	body, err := io.ReadAll(io.LimitReader(r.Body, s.maxRequestSize))
	if err != nil {
		logger.Debug("Failed to read request body", "err", err)
		writeJSONRPCError(w, nil, ErrCodeParse, "Failed to read request")
		return
	}

	metrics.requestSize.Record(r.Context(), int64(len(body)))

	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		writeJSONRPCError(w, nil, ErrCodeInvalidRequest, "Empty request body")
		return
	}

	var response []byte

	if body[0] == '[' {
		response = s.handleBatchHTTP(r.Context(), body)
	} else {
		response = s.handleSingleHTTP(r.Context(), body)
	}

	metrics.responseSize.Record(r.Context(), int64(len(response)))

	_, _ = w.Write(response)
}

func (s *Waterway) handleSingleHTTP(ctx context.Context, body []byte) []byte {
	var req JSONRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return mustMarshal(newErrorResponse(nil, ErrCodeParse, "Parse error"))
	}

	resp := s.callSafe(ctx, &req, true)
	return mustMarshal(resp)
}

func (s *Waterway) handleBatchHTTP(ctx context.Context, body []byte) []byte {
	var batch []JSONRPCRequest
	if err := json.Unmarshal(body, &batch); err != nil {
		return mustMarshal(newErrorResponse(nil, ErrCodeParse, "Parse error"))
	}

	if len(batch) == 0 {
		return mustMarshal(newErrorResponse(nil, ErrCodeInvalidRequest, "Empty batch"))
	}

	if len(batch) > s.maxConcurrentBatch {
		return mustMarshal(newErrorResponse(nil, ErrCodeInvalidRequest, "Batch too large"))
	}

	responses := make([]*JSONRPCResponse, len(batch))

	if len(batch) <= 4 {
		for i := range batch {
			responses[i] = s.callSafe(ctx, &batch[i], true)
		}
		return mustMarshal(responses)
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, min(len(batch), 16))

	for i := range batch {
		wg.Add(1)
		sem <- struct{}{}

		go func(idx int) {
			defer func() {
				<-sem
				wg.Done()
			}()
			responses[idx] = s.callSafe(ctx, &batch[idx], true)
		}(i)
	}

	wg.Wait()
	return mustMarshal(responses)
}

func (s *Waterway) callSafe(ctx context.Context, req *JSONRPCRequest, requestedOverHttp bool) *JSONRPCResponse {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("Panic in RPC call",
				"method", req.Method,
				"panic", r,
				"stack", string(debug.Stack()))
		}
	}()

	resp, err := s.call(ctx, req, requestedOverHttp)
	if err != nil {
		return newErrorResponse(req.ID, ErrCodeInternal, "Internal error")
	}
	return resp
}

func writeJSONRPCError(w http.ResponseWriter, id json.RawMessage, code int, message string) {
	response := mustMarshal(newErrorResponse(id, code, message))
	_, _ = w.Write(response)
}

func mustMarshal(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		logger.Error("Failed to marshal JSON response", "err", err)
		return []byte(`{"jsonrpc":"2.0","error":{"code":-32603,"message":"Internal error"}}`)
	}
	return data
}

func (s *Waterway) handleWS(w http.ResponseWriter, r *http.Request) {
	clientConn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Error("Failed to upgrade websocket connection", "err", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	metrics.activeConnections.Add(ctx, 1)
	defer metrics.activeConnections.Add(ctx, -1)
	defer clientConn.Close()

	// Connect to upstream
	dialer := &websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		TLSClientConfig:  &tls.Config{MinVersion: tls.VersionTLS12},
	}
	upstreamConn, _, err := dialer.DialContext(ctx, s.seiWSEndpoint, nil)
	if err != nil {
		logger.Error("Failed to connect to upstream", "err", err)
		return
	}
	defer upstreamConn.Close()

	clientConn.SetReadLimit(s.wsMaxMessageSize)
	upstreamConn.SetReadLimit(s.wsMaxMessageSize)

	// Setup pong handlers to reset read deadlines
	clientConn.SetPongHandler(func(string) error {
		return clientConn.SetReadDeadline(time.Now().Add(s.wsReadTimeout))
	})
	upstreamConn.SetPongHandler(func(string) error {
		return upstreamConn.SetReadDeadline(time.Now().Add(s.wsReadTimeout))
	})

	// Set initial read deadlines
	_ = clientConn.SetReadDeadline(time.Now().Add(s.wsReadTimeout))
	_ = upstreamConn.SetReadDeadline(time.Now().Add(s.wsReadTimeout))

	var wg sync.WaitGroup
	wg.Add(3)

	// Ping both connections periodically
	go func() {
		defer wg.Done()
		s.wsPingLoop(ctx, clientConn, upstreamConn)
	}()

	// Client -> Upstream (with validation)
	go func() {
		defer wg.Done()
		defer cancel()
		s.proxyClientToUpstream(ctx, clientConn, upstreamConn)
	}()

	// Upstream -> Client (pass-through)
	go func() {
		defer wg.Done()
		defer cancel()
		s.proxyUpstreamToClient(ctx, upstreamConn, clientConn)
	}()

	wg.Wait()
}

func (s *Waterway) wsPingLoop(ctx context.Context, clientConn, upstreamConn *websocket.Conn) {
	ticker := time.NewTicker(s.wsPingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			deadline := time.Now().Add(s.wsWriteTimeout)

			if err := clientConn.WriteControl(websocket.PingMessage, nil, deadline); err != nil {
				logger.Debug("Failed to ping client", "err", err)
				return
			}

			if err := upstreamConn.WriteControl(websocket.PingMessage, nil, deadline); err != nil {
				logger.Debug("Failed to ping upstream", "err", err)
				return
			}
		}
	}
}

func (s *Waterway) proxyClientToUpstream(ctx context.Context, client, upstream *websocket.Conn) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgType, msg, err := client.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				logger.Debug("Client read error", "err", err)
			}
			return
		}

		_ = client.SetReadDeadline(time.Now().Add(s.wsReadTimeout))

		metrics.requestSize.Record(ctx, int64(len(msg)))

		// Validate request (block forbidden methods)
		if msgType == websocket.TextMessage {
			if blocked, errResp := s.validateWSRequest(msg); blocked {
				if errResp != nil {
					_ = client.SetWriteDeadline(time.Now().Add(s.wsWriteTimeout))
					_ = client.WriteMessage(websocket.TextMessage, errResp)
				}
				continue
			}
		}

		// Forward to upstream
		if err := upstream.SetWriteDeadline(time.Now().Add(s.wsWriteTimeout)); err != nil {
			return
		}
		if err := upstream.WriteMessage(msgType, msg); err != nil {
			logger.Debug("Upstream write error", "err", err)
			return
		}
	}
}

func (s *Waterway) proxyUpstreamToClient(ctx context.Context, upstream, client *websocket.Conn) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgType, msg, err := upstream.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				logger.Debug("Upstream read error", "err", err)
			}
			return
		}

		_ = upstream.SetReadDeadline(time.Now().Add(s.wsReadTimeout))

		metrics.responseSize.Record(ctx, int64(len(msg)))

		// Forward to client
		if err := client.SetWriteDeadline(time.Now().Add(s.wsWriteTimeout)); err != nil {
			return
		}
		if err := client.WriteMessage(msgType, msg); err != nil {
			logger.Debug("Client write error", "err", err)
			return
		}
	}
}

func (s *Waterway) validateWSRequest(msg []byte) (blocked bool, errorResponse []byte) {
	msg = bytes.TrimSpace(msg)
	if len(msg) == 0 {
		return false, nil
	}

	// Handle batch requests
	if msg[0] == '[' {
		var batch []JSONRPCRequest
		if json.Unmarshal(msg, &batch) != nil {
			return false, nil // Let upstream handle parse errors
		}

		for _, req := range batch {
			if s.blockedMethods[req.Method] {
				metrics.requestsBlocked.Add(context.Background(), 1, otelmetric.WithAttributes(
					attribute.String("method", req.Method),
				))
				return true, mustMarshal(newErrorResponse(nil, ErrCodeForbidden, "Batch contains blocked method: "+req.Method))
			}
			if s.allowedMethods != nil && !s.allowedMethods[req.Method] {
				metrics.requestsBlocked.Add(context.Background(), 1, otelmetric.WithAttributes(
					attribute.String("method", req.Method),
				))
				return true, mustMarshal(newErrorResponse(nil, ErrCodeForbidden, "Batch contains disallowed method: "+req.Method))
			}
		}
		return false, nil
	}

	// Handle single request
	var req JSONRPCRequest
	if json.Unmarshal(msg, &req) != nil {
		return false, nil // Let upstream handle parse errors
	}

	if s.blockedMethods[req.Method] {
		metrics.requestsBlocked.Add(context.Background(), 1, otelmetric.WithAttributes(
			attribute.String("method", req.Method),
		))
		return true, mustMarshal(newErrorResponse(req.ID, ErrCodeForbidden, "Method not allowed"))
	}

	if s.allowedMethods != nil && !s.allowedMethods[req.Method] {
		metrics.requestsBlocked.Add(context.Background(), 1, otelmetric.WithAttributes(
			attribute.String("method", req.Method),
		))
		return true, mustMarshal(newErrorResponse(req.ID, ErrCodeForbidden, "Method not allowed"))
	}

	return false, nil
}

func (s *Waterway) setCORS(w http.ResponseWriter) {
	if len(s.allowedOrigins) == 0 {
		w.Header().Set("Access-Control-Allow-Origin", "*")
	} else {
		origins := make([]string, 0, len(s.allowedOrigins))
		for origin := range s.allowedOrigins {
			origins = append(origins, origin)
		}
		w.Header().Set("Access-Control-Allow-Origin", strings.Join(origins, ", "))
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
	_ = json.NewEncoder(w).Encode(status)
}
