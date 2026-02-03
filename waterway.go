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

	// TODO: separate the port for the endpoints below?
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

	logger := logger.With("method", req.Method, "id", req.ID)

	transport, status := "ws", "success"
	defer func(start time.Time) {
		recordRequest(ctx, req.Method, transport, status, time.Since(start))
	}(time.Now())

	if err := s.validateRequest(req, requestedOverHttp); err != nil {
		status = "invalid"
		logger.Debug("Invalid request", "err", err)
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
			logger.Debug("Subscription call failed", "err", err)
		}
		return resp, err
	}

	// Route based on method compatibility
	if s.shouldUseHTTP(req.Method) {
		transport = "http"
		resp, err := s.callHTTP(ctx, req)
		if err != nil {
			status = "error"
			logger.Debug("Call failed", "err", err)
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
			logger.Debug("HTTP fallback failed", "err", err)
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
		logger.Debug("WS call failed, falling back to HTTP", "err", err)
		return fallbackToHttp()
	}

	if resp.Error != nil && isWSIncompatibleError(resp.Error) { // Check for WS-incompatible response errors
		logger.Debug("WS incompatible response, falling back to HTTP",
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

func (s *Waterway) handleHttpOptions(w http.ResponseWriter, _ *http.Request) { s.setCORS(w) }

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

	// Small batches: process sequentially
	if len(batch) <= 4 {
		for i := range batch {
			responses[i] = s.callSafe(ctx, &batch[i], true)
		}
		return mustMarshal(responses)
	}

	// Larger batches: bounded parallelism
	var wg sync.WaitGroup
	sem := make(chan struct{}, min(len(batch), 16)) // Cap actual concurrency

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

// callSafe wraps call with panic recovery for safe use in goroutines.
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

// writeJSONRPCError writes a JSON-RPC error response.
func writeJSONRPCError(w http.ResponseWriter, id json.RawMessage, code int, message string) {
	response := mustMarshal(newErrorResponse(id, code, message))
	_, _ = w.Write(response)
}

// mustMarshal marshals v to JSON, panicking on error (should never happen with valid structs).
func mustMarshal(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		// This should never happen with our response types
		logger.Error("Failed to marshal JSON response", "err", err)
		return []byte(`{"jsonrpc":"2.0","error":{"code":-32603,"message":"Internal error"}}`)
	}
	return data
}

func (s *Waterway) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Error("Failed to upgrade websocket connection", "err", err)
		return
	}

	// Create a context that we control (r.Context() is unreliable post-upgrade)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	metrics.activeConnections.Add(ctx, 1)
	defer metrics.activeConnections.Add(ctx, -1)
	defer conn.Close()

	conn.SetReadLimit(s.wsMaxMessageSize)
	if err := conn.SetReadDeadline(time.Now().Add(s.wsReadTimeout)); err != nil {
		logger.Error("Failed to set read deadline", "err", err)
		return
	}

	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(s.wsReadTimeout))
	})

	conn.SetCloseHandler(func(code int, text string) error {
		cancel()
		return nil
	})

	writeCh := make(chan []byte, 16) // Buffer some writes to reduce contention
	errCh := make(chan error, 1)

	notifyError := func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}

	go func() {
		pingTicker := time.NewTicker(s.wsPingInterval)
		defer pingTicker.Stop()
		for ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-writeCh:
				if !ok {
					return
				}
				if err := conn.SetWriteDeadline(time.Now().Add(s.wsWriteTimeout)); err != nil {
					notifyError(err)
					return
				}
				if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
					notifyError(err)
					return
				}
			case <-pingTicker.C:
				if err := conn.SetWriteDeadline(time.Now().Add(s.wsWriteTimeout)); err != nil {
					notifyError(err)
					return
				}
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					notifyError(err)
					return
				}
			}
		}
	}()

	// Read loop
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case err := <-errCh:
			logger.Debug("WebSocket write error", "err", err)
			return
		default:
		}

		_, msg, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNormalClosure) {
				logger.Debug("WebSocket read error", "err", err)
			}
			return
		}

		if err := conn.SetReadDeadline(time.Now().Add(s.wsReadTimeout)); err != nil {
			return
		}

		metrics.requestSize.Record(ctx, int64(len(msg)))

		msg = bytes.TrimSpace(msg)
		if len(msg) == 0 {
			continue
		}

		responses := s.processWSMessage(ctx, msg)
		metrics.responseSize.Record(ctx, int64(len(responses)))

		select {
		case <-ctx.Done():
			return
		case writeCh <- responses:
		case err := <-errCh:
			logger.Debug("WebSocket write error", "err", err)
			return
		}
	}
}

func (s *Waterway) processWSMessage(ctx context.Context, msg []byte) []byte {
	if msg[0] == '[' {
		return s.processWSBatch(ctx, msg)
	}
	return s.processWSSingle(ctx, msg)
}

func (s *Waterway) processWSSingle(ctx context.Context, msg []byte) []byte {
	var req JSONRPCRequest
	if err := json.Unmarshal(msg, &req); err != nil {
		resp, _ := json.Marshal(newErrorResponse(nil, ErrCodeParse, "Parse error"))
		return resp
	}

	resp, err := s.call(ctx, &req, false)
	if err != nil {
		resp = newErrorResponse(req.ID, ErrCodeInternal, "Internal error")
	}

	result, _ := json.Marshal(resp)
	return result
}

func (s *Waterway) processWSBatch(ctx context.Context, msg []byte) []byte {
	var batch []JSONRPCRequest
	if err := json.Unmarshal(msg, &batch); err != nil {
		resp, _ := json.Marshal(newErrorResponse(nil, ErrCodeParse, "Parse error"))
		return resp
	}

	if len(batch) == 0 {
		resp, _ := json.Marshal(newErrorResponse(nil, ErrCodeInvalidRequest, "Empty batch"))
		return resp
	}

	if len(batch) > s.maxConcurrentBatch {
		resp, _ := json.Marshal(newErrorResponse(nil, ErrCodeInvalidRequest, "Batch too large"))
		return resp
	}

	resps := make([]*JSONRPCResponse, len(batch))

	// For small batches, process sequentially to avoid goroutine overhead
	if len(batch) <= 4 {
		for i, req := range batch {
			resp, err := s.call(ctx, &req, false)
			if err != nil {
				resps[i] = newErrorResponse(req.ID, ErrCodeInternal, "Internal error")
			} else {
				resps[i] = resp
			}
		}
	} else {
		// For larger batches, use bounded parallelism
		sem := make(chan struct{}, min(len(batch), 16))
		var wg sync.WaitGroup

		for i, req := range batch {
			wg.Add(1)
			sem <- struct{}{}

			go func(idx int, r JSONRPCRequest) {
				defer func() {
					<-sem
					wg.Done()
				}()

				resp, err := s.call(ctx, &r, false)
				if err != nil {
					resps[idx] = newErrorResponse(r.ID, ErrCodeInternal, "Internal error")
				} else {
					resps[idx] = resp
				}
			}(i, req)
		}
		wg.Wait()
	}

	result, _ := json.Marshal(resps)
	return result
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
