// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build internal_testing

package nats

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// genMockServer is a minimal NATS server driven over plain TCP. Each accepted
// connection is one "generation": it answers the initial CONNECT/PING with a
// PONG, counts client PINGs, and can be told to hang up. The test decides when
// the next Accept happens, which makes reconnect timing deterministic.
type genMockServer struct {
	listener net.Listener

	mu       sync.Mutex
	sessions map[*genMockSession]struct{}
	closed   chan struct{}
}

type genMockSession struct {
	conn   net.Conn
	pings  atomic.Int64
	closed atomic.Bool
}

func newGenMockServer(t *testing.T) *genMockServer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &genMockServer{
		listener: l,
		sessions: make(map[*genMockSession]struct{}),
		closed:   make(chan struct{}),
	}
	t.Cleanup(func() {
		s.closeAll()
		_ = l.Close()
	})
	return s
}

func (s *genMockServer) url() string {
	return fmt.Sprintf("nats://127.0.0.1:%d", s.listener.Addr().(*net.TCPAddr).Port)
}

// acceptAsync accepts the next connection in the background and reports it on
// the returned channel, so a test can hang up the current session without
// blocking on Accept first. A nil result means the listener was closed.
func (s *genMockServer) acceptAsync() <-chan *genMockSession {
	ch := make(chan *genMockSession, 1)
	go func() {
		c, err := s.listener.Accept()
		if err != nil {
			ch <- nil
			return
		}
		sess := &genMockSession{conn: c}
		s.mu.Lock()
		s.sessions[sess] = struct{}{}
		s.mu.Unlock()
		go sess.serve(s)
		ch <- sess
	}()
	return ch
}

func (s *genMockServer) remove(sess *genMockSession) {
	s.mu.Lock()
	delete(s.sessions, sess)
	s.mu.Unlock()
}

func (s *genMockServer) closeAll() {
	s.mu.Lock()
	for sess := range s.sessions {
		sess.hangUp()
	}
	s.mu.Unlock()
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
}

func (sess *genMockSession) serve(s *genMockServer) {
	defer func() {
		sess.hangUp()
		s.remove(sess)
	}()
	br := bufio.NewReaderSize(sess.conn, 4096)
	// Initial INFO. Declare a real max payload and header support so publishes
	// are not rejected client-side.
	if _, err := sess.conn.Write([]byte("INFO {\"server_id\":\"gen-mock\",\"max_payload\":1048576,\"headers\":true}\r\n")); err != nil {
		return
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return
		}
		switch {
		case line == pingProto:
			sess.pings.Add(1)
			if _, err := sess.conn.Write([]byte(pongProto)); err != nil {
				return
			}
		case len(line) >= 4 && (line[:4] == "PUB " || line[:5] == "HPUB "):
			// PUB  <subject> <reply> <nbytes>\r\n
			// HPUB <subject> <reply> <hdrbytes> <totalbytes>\r\n
			// The last field is the number of payload bytes that follow;
			// consume them so the next protocol line is read correctly.
			fields := splitProtoFields(line)
			nbytes := 0
			if len(fields) > 0 {
				fmt.Sscanf(fields[len(fields)-1], "%d", &nbytes)
			}
			if _, err := io.ReadFull(br, make([]byte, nbytes)); err != nil {
				return
			}
		default:
			// CONNECT, SUB/UNSUB and server-originated PONG lines: ignored.
		}
	}
}

// splitProtoFields splits "PUB subj reply 123\r\n" into its space-separated
// fields without the trailing CRLF.
func splitProtoFields(line string) []string {
	for len(line) > 0 && (line[len(line)-1] == '\n' || line[len(line)-1] == '\r') {
		line = line[:len(line)-1]
	}
	return strings.Split(line, " ")
}

func (sess *genMockSession) hangUp() {
	if sess.closed.CompareAndSwap(false, true) {
		_ = sess.conn.Close()
	}
}

func (sess *genMockSession) pingCount() int64 {
	return sess.pings.Load()
}

// waitForPing blocks until this session observes at least n client PINGs or
// the timeout fires.
func (sess *genMockSession) waitForPing(t *testing.T, n int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if sess.pingCount() >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected at least %d pings, got %d", n, sess.pingCount())
}

// connectGenClient dials the mock server with short, deterministic timing.
func connectGenClient(t *testing.T, url string, opts ...Option) *Conn {
	t.Helper()
	defaults := []Option{
		ReconnectWait(20 * time.Millisecond),
		ReconnectJitter(0, 0),
		MaxReconnects(-1),
		PingInterval(50 * time.Millisecond),
		MaxPingsOutstanding(100),
		Timeout(time.Second),
	}
	nc, err := Connect(url, append(defaults, opts...)...)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// TestGenerationBumpsOnReconnect verifies requirement 1's first half: every
// physical connection gets a new, strictly increasing generation, and the
// ping timer is recreated for the new generation rather than reused.
func TestGenerationBumpsOnReconnect(t *testing.T) {
	srv := newGenMockServer(t)
	// Arm Accept before connecting: the client dials while the test goroutine
	// would otherwise be blocked inside Accept.
	firstCh := srv.acceptAsync()
	nc := connectGenClient(t, srv.url())
	var first *genMockSession
	select {
	case first = <-firstCh:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not get the initial connection")
	}

	if g := nc.ConnGeneration(); g != 1 {
		t.Fatalf("initial generation = %d, want 1", g)
	}
	if !nc.ConnPingTimerActive() {
		t.Fatal("ping timer should be active on a connected client")
	}
	// The initial handshake PING plus timer-driven PINGs land on session 1.
	first.waitForPing(t, 1, time.Second)

	// Arm the next Accept before hanging up so the reconnect dial is served.
	nextCh := srv.acceptAsync()
	// Kill the first socket; the reconnect loop dials a second session.
	first.hangUp()
	var second *genMockSession
	select {
	case second = <-nextCh:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not get a second connection")
	}
	if second == nil {
		t.Fatal("second session failed to accept")
	}

	// Wait for the client to report connected again, then check the fence.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if nc.IsConnected() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !nc.IsConnected() {
		t.Fatal("client did not reconnect")
	}
	if g := nc.ConnGeneration(); g != 2 {
		t.Fatalf("generation after reconnect = %d, want 2", g)
	}
	if !nc.ConnPingTimerActive() {
		t.Fatal("ping timer should be recreated for the new generation")
	}
	second.waitForPing(t, 1, time.Second)
}

// TestStalePingTimerFencedAfterReconnect proves requirement 2: a ping timer
// callback that fires while the connection is between generations cannot act
// on the new socket. We force many rapid reconnects with a very short ping
// interval; with the fence, the new session must observe pings starting from
// the handshake (pout reset to zero) and must never be torn down by a stale
// ErrStaleConnection from an old timer.
func TestStalePingTimerFencedAfterReconnect(t *testing.T) {
	srv := newGenMockServer(t)
	firstCh := srv.acceptAsync()
	nc := connectGenClient(t, srv.url(), PingInterval(15*time.Millisecond))
	var first *genMockSession
	select {
	case first = <-firstCh:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not get the initial connection")
	}

	const cycles = 5
	var prev *genMockSession = first
	for i := 0; i < cycles; i++ {
		// Let timers from the current generation fire a few times before we
		// pull the socket, so stale callbacks are genuinely in flight.
		prev.waitForPing(t, 2, time.Second)
		nextCh := srv.acceptAsync()
		prev.hangUp()

		var next *genMockSession
		select {
		case next = <-nextCh:
		case <-time.After(3 * time.Second):
			t.Fatalf("cycle %d: no new session", i)
		}
		if next == nil {
			t.Fatalf("cycle %d: new session failed to accept", i)
		}
		// Reconnected on a fresh generation.
		want := uint64(i + 2)
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if nc.IsConnected() && nc.ConnGeneration() == want {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if g := nc.ConnGeneration(); g != want {
			t.Fatalf("cycle %d: generation = %d, want %d", i, g, want)
		}
		if !nc.IsConnected() {
			t.Fatalf("cycle %d: client not connected after reconnect", i)
		}
		prev = next
	}

	// Final session must still be alive and receiving current-generation
	// pings after settling: a stale timer would have forced an extra
	// disconnect/reconnect and closed this socket.
	finalG := nc.ConnGeneration()
	time.Sleep(200 * time.Millisecond)
	if nc.ConnGeneration() != finalG {
		t.Fatalf("generation changed from %d to %d while idle (stale timer fired)",
			finalG, nc.ConnGeneration())
	}
	if !nc.IsConnected() {
		t.Fatal("connection was dropped by a stale ping timer")
	}
	prev.waitForPing(t, 1, time.Second)
}

// TestInflightRequestGeneration covers requirement 1 (request ownership) and
// requirement 3 (no silent replay): a request registered on generation N
// keeps that identity across a natural reconnect, and its response is still
// delivered when the server can route it; timeout cleans both maps.
func TestInflightRequestGeneration(t *testing.T) {
	srv := newGenMockServer(t)
	sessCh := srv.acceptAsync()
	nc := connectGenClient(t, srv.url())
	var sess *genMockSession
	select {
	case sess = <-sessCh:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not get the initial connection")
	}

	// Drive a request through the public API with a long timeout, then find
	// its internal token via the generation map.
	type requestResult struct {
		msg *Msg
		err error
	}
	resCh := make(chan requestResult, 1)
	go func() {
		m, err := nc.Request("gen.req", []byte("hi"), 3*time.Second)
		resCh <- requestResult{m, err}
	}()

	// Wait until the request is registered on generation 1.
	var token string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		nc.mu.Lock()
		for k, g := range nc.respGens {
			if g == 1 {
				token = k
			}
		}
		nc.mu.Unlock()
		if token != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if token == "" {
		t.Fatal("request was not registered on generation 1")
	}
	if g, ok := nc.InflightRequestGeneration(token); !ok || g != 1 {
		t.Fatalf("inflight generation = %d/%v, want 1/true", g, ok)
	}

	// Natural reconnect: the request is NOT failed and NOT replayed; its
	// generation ownership stays pinned to the connection that sent it.
	nextCh := srv.acceptAsync()
	sess.hangUp()
	var second *genMockSession
	select {
	case second = <-nextCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no second session")
	}
	if second == nil {
		t.Fatal("second session failed to accept")
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if nc.IsConnected() && nc.ConnGeneration() == 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if g, ok := nc.InflightRequestGeneration(token); !ok || g != 1 {
		t.Fatalf("after reconnect inflight generation = %d/%v, want 1/true (pinned, not migrated)", g, ok)
	}

	// Simulate the server still being able to route the response after the
	// reconnect: deliver it locally through the response mux. The request
	// completes normally even though it was registered on generation 1.
	nc.mu.Lock()
	inbox := nc.respSubPrefix + token
	nc.mu.Unlock()
	respMsg := NewMsg(inbox)
	respMsg.Data = []byte("reply")
	nc.respHandler(respMsg)
	var res requestResult
	select {
	case res = <-resCh:
	case <-time.After(2 * time.Second):
		t.Fatal("request did not complete after response")
	}
	if res.err != nil {
		t.Fatalf("request returned error: %v", res.err)
	}
	if string(res.msg.Data) != "reply" {
		t.Fatalf("unexpected response %q", res.msg.Data)
	}
	if _, ok := nc.InflightRequestGeneration(token); ok {
		t.Fatal("generation entry not cleaned up after delivery")
	}
	if n := nc.NumInflightRequests(); n != 0 {
		t.Fatalf("respMap not empty after delivery: %d", n)
	}
}

// TestInflightRequestTimeoutCleansGeneration verifies the two in-flight maps
// stay in sync when a request times out.
func TestInflightRequestTimeoutCleansGeneration(t *testing.T) {
	srv := newGenMockServer(t)
	sessCh := srv.acceptAsync()
	nc := connectGenClient(t, srv.url())
	select {
	case <-sessCh:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not get the initial connection")
	}

	_, err := nc.Request("gen.timeout", nil, 30*time.Millisecond)
	if err != ErrTimeout {
		t.Fatalf("err = %v, want ErrTimeout", err)
	}
	if n := nc.NumInflightRequests(); n != 0 {
		t.Fatalf("respMap leaked %d entries after timeout", n)
	}
	nc.mu.Lock()
	n := len(nc.respGens)
	nc.mu.Unlock()
	if n != 0 {
		t.Fatalf("respGens leaked %d entries after timeout", n)
	}
}

// TestForceReconnectFailsInflightRequests locks requirement 3's explicit
// interrupt contract at the generation layer: ForceReconnect ends old
// generation requests with ErrDisconnected and leaves no in-flight state.
func TestForceReconnectFailsInflightRequests(t *testing.T) {
	srv := newGenMockServer(t)
	sessCh := srv.acceptAsync()
	nc := connectGenClient(t, srv.url(), ReconnectWait(500*time.Millisecond))
	select {
	case <-sessCh:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not get the initial connection")
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := nc.Request("gen.force", nil, 3*time.Second)
		errCh <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if nc.NumInflightRequests() == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := nc.ForceReconnect(); err != nil {
		t.Fatalf("force reconnect: %v", err)
	}
	select {
	case err := <-errCh:
		if err != ErrDisconnected {
			t.Fatalf("err = %v, want ErrDisconnected", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request was not failed by ForceReconnect")
	}
	if n := nc.NumInflightRequests(); n != 0 {
		t.Fatalf("in-flight state leaked after ForceReconnect: %d", n)
	}
}
