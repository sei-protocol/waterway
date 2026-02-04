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

	mux.HandleFunc("GET /", s.handleRoot)
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

func (s *Waterway) shouldUseHTTP(method string) bool {
	if s.httpOnlyMethods[method] {
		return true
	}
	_, failed := s.wsFailedMethods.Load(method)
	return failed
}

// call routes requests through caching and HTTP fallback logic.
// For WS clients, subscription methods are handled separately via direct upstream forwarding.
func (s *Waterway) call(ctx context.Context, req *JSONRPCRequest, requestedOverHttp bool, upstreamWS *websocket.Conn) (*JSONRPCResponse, error) {
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

	// Check cache first
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

	// Route based on method compatibility
	if s.shouldUseHTTP(req.Method) {
		transport = "http"
		resp, err := s.callHTTP(ctx, req)
		if err != nil {
			status = "error"
			log.Debug("HTTP call failed", "err", err)
			return nil, err
		}
		if resp.Error == nil {
			s.cache.Set(ctx, req, resp)
		}
		return resp, nil
	}

	// For WS clients with an upstream connection, use it directly
	if upstreamWS != nil {
		resp, err := s.callUpstreamWS(ctx, upstreamWS, req)
		if err != nil {
			log.Debug("Upstream WS call failed, falling back to HTTP", "err", err)
			return s.callWithHTTPFallback(ctx, req, log, &transport, &status)
		}

		if resp.Error != nil && isWSIncompatibleError(resp.Error) {
			log.Debug("WS incompatible response, falling back to HTTP",
				"error_code", resp.Error.Code,
				"error_message", resp.Error.Message)
			s.wsFailedMethods.Store(req.Method, true)
			return s.callWithHTTPFallback(ctx, req, log, &transport, &status)
		}

		if resp.Error == nil {
			s.cache.Set(ctx, req, resp)
		}
		return resp, nil
	}

	// For HTTP clients or when no upstream WS, use the pool
	resp, err := s.callWSPool(ctx, req)
	if err != nil {
		log.Debug("WS pool call failed, falling back to HTTP", "err", err)
		return s.callWithHTTPFallback(ctx, req, log, &transport, &status)
	}

	if resp.Error != nil && isWSIncompatibleError(resp.Error) {
		log.Debug("WS incompatible response, falling back to HTTP",
			"error_code", resp.Error.Code,
			"error_message", resp.Error.Message)
		s.wsFailedMethods.Store(req.Method, true)
		return s.callWithHTTPFallback(ctx, req, log, &transport, &status)
	}

	if resp.Error == nil {
		s.cache.Set(ctx, req, resp)
	}
	return resp, nil
}

func (s *Waterway) callWSPool(ctx context.Context, req *JSONRPCRequest) (*JSONRPCResponse, error) {
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

func (s *Waterway) callUpstreamWS(ctx context.Context, upstream *websocket.Conn, req *JSONRPCRequest) (*JSONRPCResponse, error) {
	defer func(start time.Time) {
		metrics.upstreamDuration.Record(ctx, time.Since(start).Seconds(), otelmetric.WithAttributes(
			attribute.String("transport", "ws_direct"),
		))
	}(time.Now())

	// Generate internal request ID for correlation
	internalID := time.Now().UnixNano()

	data, _ := json.Marshal(struct {
		JSONRPC string          `json:"jsonrpc"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params,omitempty"`
		ID      int64           `json:"id"`
	}{"2.0", req.Method, req.Params, internalID})

	if err := upstream.SetWriteDeadline(time.Now().Add(s.wsWriteTimeout)); err != nil {
		return nil, err
	}
	if err := upstream.WriteMessage(websocket.TextMessage, data); err != nil {
		return nil, err
	}

	// Read response (with timeout)
	if err := upstream.SetReadDeadline(time.Now().Add(s.wsReadTimeout)); err != nil {
		return nil, err
	}

	for {
		_, msg, err := upstream.ReadMessage()
		if err != nil {
			return nil, err
		}

		var resp JSONRPCResponse
		if json.Unmarshal(msg, &resp) != nil {
			continue
		}

		// Check if this is the response to our request
		respID, _ := resp.ID.MarshalJSON()
		expectedID, _ := json.Marshal(internalID)
		if bytes.Equal(respID, expectedID) {
			// Restore original request ID
			resp.ID = req.ID
			return &resp, nil
		}

		// This might be a subscription notification - we can't handle it here
		// It will be lost, but that's okay for request/response calls
	}
}

func (s *Waterway) callWithHTTPFallback(ctx context.Context, req *JSONRPCRequest, log *slog.Logger, transport, status *string) (*JSONRPCResponse, error) {
	*transport = "http_fallback"

	metrics.fallbackTotal.Add(ctx, 1, otelmetric.WithAttributes(
		attribute.String("method", req.Method),
	))

	resp, err := s.callHTTP(ctx, req)
	if err != nil {
		*status = "error"
		log.Debug("HTTP fallback failed", "err", err)
		return nil, err
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

// ============================================================================
// HTTP Handlers
// ============================================================================

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

	resp := s.callSafeHTTP(ctx, &req)
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
			responses[i] = s.callSafeHTTP(ctx, &batch[i])
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
			responses[idx] = s.callSafeHTTP(ctx, &batch[idx])
		}(i)
	}

	wg.Wait()
	return mustMarshal(responses)
}

func (s *Waterway) callSafeHTTP(ctx context.Context, req *JSONRPCRequest) *JSONRPCResponse {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("Panic in RPC call",
				"method", req.Method,
				"panic", r,
				"stack", string(debug.Stack()))
		}
	}()

	resp, err := s.call(ctx, req, true, nil)
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

// ============================================================================
// WebSocket Handler
// ============================================================================

func (s *Waterway) handleRoot(w http.ResponseWriter, r *http.Request) {
	// Check if this is a WebSocket upgrade request
	if websocket.IsWebSocketUpgrade(r) {
		s.handleWS(w, r)
		return
	}

	// Regular HTTP GET - return service info
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"service": "waterway",
		"status":  "ok",
		"endpoints": map[string]string{
			"websocket": "GET / (with Upgrade header)",
			"http_rpc":  "POST /",
			"health":    "GET /health",
			"metrics":   "GET /metrics",
		},
	})
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

	// Connect to upstream for subscriptions
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

	// Setup pong handlers
	clientConn.SetPongHandler(func(string) error {
		return clientConn.SetReadDeadline(time.Now().Add(s.wsReadTimeout))
	})
	upstreamConn.SetPongHandler(func(string) error {
		return upstreamConn.SetReadDeadline(time.Now().Add(s.wsReadTimeout))
	})

	_ = clientConn.SetReadDeadline(time.Now().Add(s.wsReadTimeout))
	_ = upstreamConn.SetReadDeadline(time.Now().Add(s.wsReadTimeout))

	// Channel for sending responses to client
	clientWriteCh := make(chan []byte, 64)
	errCh := make(chan error, 2)

	var wg sync.WaitGroup
	wg.Add(4)

	// Ping loop
	go func() {
		defer wg.Done()
		s.wsPingLoop(ctx, clientConn, upstreamConn)
	}()

	// Client writer - sends responses to client
	go func() {
		defer wg.Done()
		s.wsClientWriter(ctx, clientConn, clientWriteCh, errCh)
	}()

	// Upstream reader - forwards subscription notifications to client
	go func() {
		defer wg.Done()
		defer cancel()
		s.wsUpstreamReader(ctx, upstreamConn, clientWriteCh, errCh)
	}()

	// Client reader - reads requests, routes them appropriately
	go func() {
		defer wg.Done()
		defer cancel()
		s.wsClientReader(ctx, clientConn, upstreamConn, clientWriteCh, errCh)
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

func (s *Waterway) wsClientWriter(ctx context.Context, client *websocket.Conn, writeCh <-chan []byte, errCh chan<- error) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-writeCh:
			if !ok {
				return
			}
			if err := client.SetWriteDeadline(time.Now().Add(s.wsWriteTimeout)); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			if err := client.WriteMessage(websocket.TextMessage, msg); err != nil {
				logger.Debug("Client write error", "err", err)
				select {
				case errCh <- err:
				default:
				}
				return
			}
		}
	}
}

func (s *Waterway) wsUpstreamReader(ctx context.Context, upstream *websocket.Conn, clientWriteCh chan<- []byte, errCh chan<- error) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, msg, err := upstream.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				logger.Debug("Upstream read error", "err", err)
				select {
				case errCh <- err:
				default:
				}
			}
			return
		}

		_ = upstream.SetReadDeadline(time.Now().Add(s.wsReadTimeout))

		// Log all upstream messages at debug level
		logger.Debug("Upstream message received", "size", len(msg), "raw", string(msg))

		metrics.responseSize.Record(ctx, int64(len(msg)))

		// Check for problematic responses and skip them
		if reason := s.checkMalformedResponse(msg); reason != "" {
			logger.Warn("Skipping malformed upstream response",
				"reason", reason,
				"raw", string(msg),
			)
			metrics.malformedResponses.Add(ctx, 1, otelmetric.WithAttributes(
				attribute.String("reason", reason),
			))
			continue
		}

		// Check for upstream errors (rate limiting, etc.)
		if errInfo := s.checkUpstreamError(msg); errInfo != nil {
			logger.Warn("Upstream error response",
				"code", errInfo.Code,
				"message", errInfo.Message,
				"raw", string(msg),
			)
			metrics.upstreamErrors.Add(ctx, 1, otelmetric.WithAttributes(
				attribute.Int("code", errInfo.Code),
			))
			// Still forward errors to client - they need to know
		}

		// Forward to client
		select {
		case clientWriteCh <- msg:
		case <-ctx.Done():
			return
		default:
			logger.Warn("Client write channel full, dropping message", "size", len(msg))
			metrics.droppedMessages.Add(ctx, 1)
		}
	}
}

// checkMalformedResponse checks if a message is malformed and should be skipped.
// Returns empty string if OK, otherwise returns the reason for skipping.
func (s *Waterway) checkMalformedResponse(msg []byte) string {
	msg = bytes.TrimSpace(msg)

	// Empty message
	if len(msg) == 0 {
		return "empty_message"
	}

	// Empty JSON object
	if bytes.Equal(msg, []byte("{}")) {
		return "empty_json_object"
	}

	// Try to parse as JSON
	var raw map[string]json.RawMessage
	if json.Unmarshal(msg, &raw) != nil {
		return "invalid_json"
	}

	// Check for subscription notifications
	if methodRaw, hasMethod := raw["method"]; hasMethod {
		var method string
		if json.Unmarshal(methodRaw, &method) != nil {
			return "invalid_method_field"
		}

		if method == "eth_subscription" {
			return s.checkSubscriptionNotification(raw)
		}
	}

	// Check for regular responses (has "id" field)
	if _, hasID := raw["id"]; hasID {
		return s.checkRegularResponse(raw)
	}

	return ""
}

// checkSubscriptionNotification validates eth_subscription notifications
func (s *Waterway) checkSubscriptionNotification(raw map[string]json.RawMessage) string {
	paramsRaw, hasParams := raw["params"]
	if !hasParams {
		return "subscription_missing_params"
	}

	// Check for empty params
	params := bytes.TrimSpace(paramsRaw)
	if len(params) == 0 || bytes.Equal(params, []byte("null")) || bytes.Equal(params, []byte("{}")) {
		return "subscription_empty_params"
	}

	var paramsObj struct {
		Subscription string          `json:"subscription"`
		Result       json.RawMessage `json:"result"`
	}
	if json.Unmarshal(paramsRaw, &paramsObj) != nil {
		return "subscription_invalid_params"
	}

	if paramsObj.Subscription == "" {
		return "subscription_missing_id"
	}

	// Check result
	result := bytes.TrimSpace(paramsObj.Result)
	if len(result) == 0 {
		return "subscription_empty_result"
	}

	if bytes.Equal(result, []byte("null")) {
		return "subscription_null_result"
	}

	if bytes.Equal(result, []byte("{}")) {
		return "subscription_empty_object_result"
	}

	if bytes.Equal(result, []byte("[]")) {
		return "subscription_empty_array_result"
	}

	// For log subscriptions, check for required 'address' field
	if bytes.Contains(result, []byte("topics")) || bytes.Contains(result, []byte("logIndex")) {
		var logResult struct {
			Address string `json:"address"`
		}
		if json.Unmarshal(result, &logResult) == nil && logResult.Address == "" {
			return "log_missing_address"
		}
	}

	return ""
}

// checkRegularResponse validates regular JSON-RPC responses
func (s *Waterway) checkRegularResponse(raw map[string]json.RawMessage) string {
	// If it has an error field, it's an error response - let it through
	// (we handle errors separately in checkUpstreamError)
	if _, hasError := raw["error"]; hasError {
		return ""
	}

	// Check for result field
	resultRaw, hasResult := raw["result"]
	if !hasResult {
		// No result and no error - malformed response
		return "response_missing_result"
	}

	// Check for empty/null result
	result := bytes.TrimSpace(resultRaw)
	if len(result) == 0 {
		return "response_empty_result"
	}

	// Note: "null" is a valid result for some methods (e.g., eth_getTransactionReceipt for pending tx)
	// So we don't skip null results for regular responses

	// Empty object might be valid for some responses, but empty params in a response is suspicious
	// We'll allow {} for now but log it at debug level
	if bytes.Equal(result, []byte("{}")) {
		logger.Debug("Response with empty object result", "raw", string(resultRaw))
	}

	return ""
}

// upstreamErrorInfo holds information about an upstream error
type upstreamErrorInfo struct {
	Code    int
	Message string
}

// checkUpstreamError checks if a message is an error response
func (s *Waterway) checkUpstreamError(msg []byte) *upstreamErrorInfo {
	var resp struct {
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if json.Unmarshal(msg, &resp) == nil && resp.Error != nil {
		return &upstreamErrorInfo{
			Code:    resp.Error.Code,
			Message: resp.Error.Message,
		}
	}

	return nil
}

func (s *Waterway) wsClientReader(ctx context.Context, client, upstream *websocket.Conn, clientWriteCh chan<- []byte, errCh <-chan error) {
	upstreamMu := &sync.Mutex{} // Protect upstream writes

	for {
		select {
		case <-ctx.Done():
			return
		case err := <-errCh:
			logger.Debug("Error from other goroutine", "err", err)
			return
		default:
		}

		_, msg, err := client.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				logger.Debug("Client read error", "err", err)
			}
			return
		}

		_ = client.SetReadDeadline(time.Now().Add(s.wsReadTimeout))

		metrics.requestSize.Record(ctx, int64(len(msg)))

		msg = bytes.TrimSpace(msg)
		if len(msg) == 0 {
			continue
		}

		// Log client requests at debug level
		logger.Debug("Client request received", "raw", string(msg))

		// Route the request
		response := s.routeWSRequest(ctx, msg, upstream, upstreamMu)
		if response != nil {
			select {
			case clientWriteCh <- response:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (s *Waterway) routeWSRequest(ctx context.Context, msg []byte, upstream *websocket.Conn, upstreamMu *sync.Mutex) []byte {
	// Handle batches
	if msg[0] == '[' {
		return s.routeWSBatch(ctx, msg, upstream, upstreamMu)
	}

	var req JSONRPCRequest
	if err := json.Unmarshal(msg, &req); err != nil {
		return mustMarshal(newErrorResponse(nil, ErrCodeParse, "Parse error"))
	}

	// Check blocked methods
	if s.blockedMethods[req.Method] {
		metrics.requestsBlocked.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("method", req.Method),
		))
		return mustMarshal(newErrorResponse(req.ID, ErrCodeForbidden, "Method not allowed"))
	}
	if s.allowedMethods != nil && !s.allowedMethods[req.Method] {
		metrics.requestsBlocked.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("method", req.Method),
		))
		return mustMarshal(newErrorResponse(req.ID, ErrCodeForbidden, "Method not allowed"))
	}

	// Subscriptions: forward directly to upstream, response comes via upstream reader
	if req.Method == "eth_subscribe" || req.Method == "eth_unsubscribe" {
		logger.Debug("Forwarding subscription request to upstream", "method", req.Method, "id", req.ID)

		upstreamMu.Lock()
		err := upstream.SetWriteDeadline(time.Now().Add(s.wsWriteTimeout))
		if err == nil {
			err = upstream.WriteMessage(websocket.TextMessage, msg)
		}
		upstreamMu.Unlock()

		if err != nil {
			logger.Debug("Failed to forward subscription to upstream", "err", err)
			return mustMarshal(newErrorResponse(req.ID, ErrCodeInternal, "Upstream error"))
		}

		// Response will come through wsUpstreamReader
		return nil
	}

	// Regular requests: use call() with HTTP fallback, caching, etc.
	resp, err := s.call(ctx, &req, false, nil)
	if err != nil {
		return mustMarshal(newErrorResponse(req.ID, ErrCodeInternal, "Internal error"))
	}
	return mustMarshal(resp)
}

func (s *Waterway) routeWSBatch(ctx context.Context, msg []byte, upstream *websocket.Conn, upstreamMu *sync.Mutex) []byte {
	var batch []JSONRPCRequest
	if err := json.Unmarshal(msg, &batch); err != nil {
		return mustMarshal(newErrorResponse(nil, ErrCodeParse, "Parse error"))
	}

	if len(batch) == 0 {
		return mustMarshal(newErrorResponse(nil, ErrCodeInvalidRequest, "Empty batch"))
	}

	if len(batch) > s.maxConcurrentBatch {
		return mustMarshal(newErrorResponse(nil, ErrCodeInvalidRequest, "Batch too large"))
	}

	// Check for blocked methods
	for _, req := range batch {
		if s.blockedMethods[req.Method] {
			metrics.requestsBlocked.Add(ctx, 1, otelmetric.WithAttributes(
				attribute.String("method", req.Method),
			))
			return mustMarshal(newErrorResponse(nil, ErrCodeForbidden, "Batch contains blocked method: "+req.Method))
		}
		if s.allowedMethods != nil && !s.allowedMethods[req.Method] {
			metrics.requestsBlocked.Add(ctx, 1, otelmetric.WithAttributes(
				attribute.String("method", req.Method),
			))
			return mustMarshal(newErrorResponse(nil, ErrCodeForbidden, "Batch contains disallowed method: "+req.Method))
		}
	}

	// Check if batch contains subscriptions
	hasSubscription := false
	for _, req := range batch {
		if req.Method == "eth_subscribe" || req.Method == "eth_unsubscribe" {
			hasSubscription = true
			break
		}
	}

	// If batch contains subscriptions, forward entire batch to upstream
	if hasSubscription {
		logger.Debug("Forwarding batch with subscriptions to upstream", "size", len(batch))

		upstreamMu.Lock()
		err := upstream.SetWriteDeadline(time.Now().Add(s.wsWriteTimeout))
		if err == nil {
			err = upstream.WriteMessage(websocket.TextMessage, msg)
		}
		upstreamMu.Unlock()

		if err != nil {
			logger.Debug("Failed to forward batch to upstream", "err", err)
			return mustMarshal(newErrorResponse(nil, ErrCodeInternal, "Upstream error"))
		}

		// Response will come through wsUpstreamReader
		return nil
	}

	// No subscriptions: process through call() with HTTP fallback
	responses := make([]*JSONRPCResponse, len(batch))

	if len(batch) <= 4 {
		for i := range batch {
			resp, err := s.call(ctx, &batch[i], false, nil)
			if err != nil {
				responses[i] = newErrorResponse(batch[i].ID, ErrCodeInternal, "Internal error")
			} else {
				responses[i] = resp
			}
		}
		return mustMarshal(responses)
	}

	// Parallel processing for larger batches
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

			resp, err := s.call(ctx, &batch[idx], false, nil)
			if err != nil {
				responses[idx] = newErrorResponse(batch[idx].ID, ErrCodeInternal, "Internal error")
			} else {
				responses[idx] = resp
			}
		}(i)
	}

	wg.Wait()
	return mustMarshal(responses)
}

// ============================================================================
// Utility
// ============================================================================

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
