package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// WSPool manages a pool of WebSocket connections
type WSPool struct {
	endpoint     string
	connections  chan *WSConn
	maxConns     int
	timeout      time.Duration
	maxAge       time.Duration
	pingInterval time.Duration
	maxMsgSize   int64
	healthy      atomic.Bool
	dialer       *websocket.Dialer
}

// WSConn represents a pooled WebSocket connection
type WSConn struct {
	conn      *websocket.Conn
	mu        sync.Mutex
	requestID atomic.Uint64
	pending   map[uint64]chan *JSONRPCResponse
	pendingMu sync.RWMutex
	createdAt time.Time
	closed    atomic.Bool
	closeCh   chan struct{}
}

// NewWSPool creates a new WebSocket connection pool
func NewWSPool(endpoint string, maxConns int, timeout, maxAge, pingInterval time.Duration, maxMsgSize int64) *WSPool {
	p := &WSPool{
		endpoint:     endpoint,
		connections:  make(chan *WSConn, maxConns),
		maxConns:     maxConns,
		timeout:      timeout,
		maxAge:       maxAge,
		pingInterval: pingInterval,
		maxMsgSize:   maxMsgSize,
		dialer: &websocket.Dialer{
			HandshakeTimeout: 10 * time.Second,
			TLSClientConfig:  &tls.Config{MinVersion: tls.VersionTLS12},
		},
	}
	p.healthy.Store(true)
	return p
}

// Get retrieves a connection from the pool
func (p *WSPool) Get(ctx context.Context) (*WSConn, error) {
	for ctx.Err() == nil {
		select {
		case conn := <-p.connections:
			metrics.wsPoolSize.Add(ctx, -1)
			if conn.isValid(p.maxAge) {
				return conn, nil
			}
			conn.Close()
		default:
			return p.newConnection(ctx)
		}
	}
	return nil, ctx.Err()
}

// Put returns a connection to the pool
func (p *WSPool) Put(ctx context.Context, conn *WSConn) {
	if conn == nil || conn.closed.Load() || !conn.isValid(p.maxAge) {
		if conn != nil {
			conn.Close()
		}
		return
	}
	select {
	case p.connections <- conn:
		metrics.wsPoolSize.Add(ctx, 1)
	default:
		conn.Close()
	}
}

func (p *WSPool) newConnection(ctx context.Context) (*WSConn, error) {
	ws, _, err := p.dialer.DialContext(ctx, p.endpoint, nil)
	if err != nil {
		p.healthy.Store(false)
		return nil, err
	}
	p.healthy.Store(true)
	ws.SetReadLimit(p.maxMsgSize)
	conn := &WSConn{
		conn:      ws,
		pending:   make(map[uint64]chan *JSONRPCResponse),
		createdAt: time.Now(),
		closeCh:   make(chan struct{}),
	}
	go conn.readLoop()
	go conn.pingLoop(p.pingInterval)
	return conn, nil
}

// IsHealthy returns whether the pool is healthy
func (p *WSPool) IsHealthy() bool { return p.healthy.Load() }

// Close closes all connections in the pool
func (p *WSPool) Close() {
	close(p.connections)
	for conn := range p.connections {
		conn.Close()
	}
}

func (c *WSConn) isValid(maxAge time.Duration) bool {
	return !c.closed.Load() && time.Since(c.createdAt) < maxAge
}

// Close closes the WebSocket connection
func (c *WSConn) Close() {
	if c.closed.Swap(true) {
		return
	}
	close(c.closeCh)
	c.pendingMu.Lock()
	for _, ch := range c.pending {
		close(ch)
	}
	c.pending = nil
	c.pendingMu.Unlock()
	_ = c.conn.Close()
}

func (c *WSConn) readLoop() {
	defer c.Close()
	for {
		select {
		case <-c.closeCh:
			return
		default:
		}
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		var resp JSONRPCResponse
		if json.Unmarshal(msg, &resp) != nil {
			continue
		}
		id := extractID(resp.ID)
		if id == 0 {
			continue
		}
		c.pendingMu.RLock()
		ch, ok := c.pending[id]
		c.pendingMu.RUnlock()
		if ok {
			select {
			case ch <- &resp:
			default:
			}
			c.pendingMu.Lock()
			delete(c.pending, id)
			c.pendingMu.Unlock()
		}
	}
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

func (c *WSConn) pingLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.closeCh:
			return
		case <-ticker.C:
			c.mu.Lock()
			err := c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
			c.mu.Unlock()
			if err != nil {
				c.Close()
				return
			}
		}
	}
}

// Call sends a JSON-RPC request and waits for a response
func (c *WSConn) Call(ctx context.Context, req *JSONRPCRequest, timeout time.Duration) (*JSONRPCResponse, error) {
	if c.closed.Load() {
		return nil, errors.New("connection closed")
	}
	id := c.requestID.Add(1)
	data, _ := json.Marshal(struct {
		JSONRPC string          `json:"jsonrpc"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params,omitempty"`
		ID      uint64          `json:"id"`
	}{"2.0", req.Method, req.Params, id})

	respCh := make(chan *JSONRPCResponse, 1)
	c.pendingMu.Lock()
	c.pending[id] = respCh
	c.pendingMu.Unlock()

	c.mu.Lock()
	_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	err := c.conn.WriteMessage(websocket.TextMessage, data)
	c.mu.Unlock()
	if err != nil {
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case resp := <-respCh:
		if resp == nil {
			return nil, errors.New("connection closed")
		}
		resp.ID = req.ID
		return resp, nil
	case <-ctx.Done():
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return nil, ctx.Err()
	case <-c.closeCh:
		return nil, errors.New("connection closed")
	}
}
