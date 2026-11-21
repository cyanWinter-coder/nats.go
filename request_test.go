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

package nats

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// mockNATSServer is a minimal mock NATS server for request/reply tests.
// It captures incoming requests (PUB/HPUB with a reply subject) and lets
// the test drive the responses.
type mockNATSServer struct {
	ln  net.Listener
	url string

	mu   sync.Mutex
	conn net.Conn
	sid  string

	onRequest func(payload []byte, reply string)
}

func newMockNATSServer(t *testing.T) *mockNATSServer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Could not listen on an ephemeral port: %v", err)
	}
	s := &mockNATSServer{
		ln:  l,
		url: fmt.Sprintf("nats://127.0.0.1:%d", l.Addr().(*net.TCPAddr).Port),
	}
	go s.acceptLoop()
	return s
}

func (s *mockNATSServer) acceptLoop() {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conn = c
		s.mu.Unlock()
		go s.serve(c)
	}
}

func (s *mockNATSServer) serve(c net.Conn) {
	s.writeConn(c, "INFO {\"server_id\":\"mock\",\"max_payload\":1048576}\r\n")
	br := bufio.NewReader(c)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "PING":
			s.writeConn(c, "PONG\r\n")
		case "SUB":
			// SUB <subject> <sid> or SUB <subject> <queue> <sid>
			s.mu.Lock()
			s.sid = fields[len(fields)-1]
			s.mu.Unlock()
		case "PUB":
			// PUB <subject> [<reply>] <size>
			var reply string
			var size int
			if len(fields) == 4 {
				reply = fields[2]
				size, _ = strconv.Atoi(fields[3])
			} else {
				size, _ = strconv.Atoi(fields[2])
			}
			payload := make([]byte, size+2)
			if _, err := io.ReadFull(br, payload); err != nil {
				return
			}
			if s.onRequest != nil && reply != _EMPTY_ {
				s.onRequest(payload[:size], reply)
			}
		case "HPUB":
			// HPUB <subject> [<reply>] <hdr-size> <total-size>
			var reply string
			var hsz, tsz int
			if len(fields) == 5 {
				reply = fields[2]
				hsz, _ = strconv.Atoi(fields[3])
				tsz, _ = strconv.Atoi(fields[4])
			} else {
				hsz, _ = strconv.Atoi(fields[2])
				tsz, _ = strconv.Atoi(fields[3])
			}
			buf := make([]byte, tsz+2)
			if _, err := io.ReadFull(br, buf); err != nil {
				return
			}
			if s.onRequest != nil && reply != _EMPTY_ {
				s.onRequest(buf[hsz:tsz], reply)
			}
		}
	}
}

func (s *mockNATSServer) writeConn(c net.Conn, str string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c.Write([]byte(str))
}

// sendMsg delivers a MSG frame for the given (reply) subject to the
// client on the current connection.
func (s *mockNATSServer) sendMsg(subject, payload string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return
	}
	fmt.Fprintf(s.conn, "MSG %s %s %d\r\n%s\r\n", subject, s.sid, len(payload), payload)
}

// closeConn closes the current client connection, forcing a reconnect.
func (s *mockNATSServer) closeConn() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		s.conn.Close()
	}
}

func (s *mockNATSServer) close() {
	s.ln.Close()
	s.closeConn()
}

func respMapLen(nc *Conn) int {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	return len(nc.respMap)
}

func waitForRespMapLen(t *testing.T, nc *Conn, n int) {
	t.Helper()
	for range 200 {
		if respMapLen(nc) == n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("Expected %d response waiter(s), got %d", n, respMapLen(nc))
}

// Response tokens must embed the connection generation so that a reply
// from a previous connection can not match a request of the current one.
func TestRespInboxGenerationToken(t *testing.T) {
	nc := &Conn{}

	nc.mu.Lock()
	inbox1 := nc.newRespInbox()
	if gen := nc.respGen; gen != 0 {
		nc.mu.Unlock()
		t.Fatalf("Expected initial generation 0, got %d", gen)
	}
	// Simulate a reconnect.
	nc.respGen++
	inbox2 := nc.newRespInbox()
	prefix := nc.respSubPrefix
	if got := nc.respToken(inbox1); got != inbox1[len(prefix):] {
		nc.mu.Unlock()
		t.Fatalf("respToken(%q) = %q", inbox1, got)
	}
	nc.mu.Unlock()

	if !strings.HasPrefix(inbox1, prefix) || !strings.HasPrefix(inbox2, prefix) {
		t.Fatalf("Inboxes %q and %q should share prefix %q", inbox1, inbox2, prefix)
	}
	tok1, tok2 := inbox1[len(prefix):], inbox2[len(prefix):]
	if len(tok1) != respTokenLen || len(tok2) != respTokenLen {
		t.Fatalf("Expected token length %d, got %d and %d", respTokenLen, len(tok1), len(tok2))
	}
	if tok1 == tok2 {
		t.Fatalf("Tokens of different generations should differ: %q", tok1)
	}
	if tok1[:respGenSuffixLen] == tok2[:respGenSuffixLen] {
		t.Fatalf("Generation part of the token did not change: %q vs %q", tok1, tok2)
	}
}

// A reply from an old connection generation must never complete a
// request of the current generation, and a reply must be delivered
// to a given request at most once.
func TestRespHandlerGenerationIsolation(t *testing.T) {
	nc := &Conn{}

	nc.mu.Lock()
	oldInbox := nc.newRespInbox()
	oldToken := oldInbox[nc.respSubLen:]
	oldCh := make(chan *Msg, 1)
	nc.respMap[oldToken] = oldCh
	// Simulate a reconnect: the generation moves on.
	nc.respGen++
	newInbox := nc.newRespInbox()
	newToken := newInbox[nc.respSubLen:]
	newCh := make(chan *Msg, 1)
	nc.respMap[newToken] = newCh
	nc.mu.Unlock()

	// A reply for a still pending old-generation request completes
	// that request only.
	nc.respHandler(&Msg{Subject: oldInbox, Data: []byte("old")})
	select {
	case m := <-oldCh:
		if string(m.Data) != "old" {
			t.Fatalf("Unexpected response: %q", m.Data)
		}
	default:
		t.Fatal("Pending old-generation request should have completed")
	}
	// A second reply for the same inbox is dropped: one response only.
	nc.respHandler(&Msg{Subject: oldInbox, Data: []byte("old-dup")})
	select {
	case m := <-oldCh:
		t.Fatalf("Duplicate response delivered: %q", m.Data)
	default:
	}

	// Simulate the old request timing out: its waiter is removed.
	nc.removeRespWaiter(oldToken)
	// The new-generation request is now the only waiter. A stale reply
	// to the old inbox must not complete it, not even through the
	// single-entry fallback used for server-rewritten subjects.
	nc.respHandler(&Msg{Subject: oldInbox, Data: []byte("stale")})
	select {
	case m := <-newCh:
		t.Fatalf("Stale reply completed a new-generation request: %q", m.Data)
	default:
	}
	nc.mu.Lock()
	_, ok := nc.respMap[newToken]
	nc.mu.Unlock()
	if !ok {
		t.Fatal("New-generation waiter should still be registered")
	}

	// The single-entry fallback for server-rewritten subjects (no
	// matching prefix) must still work.
	nc.respHandler(&Msg{Subject: "rewritten.subject", Data: []byte("rw")})
	select {
	case m := <-newCh:
		if string(m.Data) != "rw" {
			t.Fatalf("Unexpected response: %q", m.Data)
		}
	default:
		t.Fatal("Fallback for server-rewritten subject is broken")
	}
}

// On timeout, the response waiter must be removed and a late reply
// must be discarded without affecting subsequent requests.
func TestRequestTimeoutRemovesWaiter(t *testing.T) {
	s := newMockNATSServer(t)
	defer s.close()

	type reqMsg struct {
		payload, reply string
	}
	received := make(chan reqMsg, 10)
	s.onRequest = func(payload []byte, reply string) {
		received <- reqMsg{string(payload), reply}
	}

	nc, err := Connect(s.url)
	if err != nil {
		t.Fatalf("Unexpected connect error: %v", err)
	}
	defer nc.Close()

	done := make(chan error, 1)
	go func() {
		_, err := nc.Request("foo", []byte("one"), 100*time.Millisecond)
		done <- err
	}()
	var r1 reqMsg
	select {
	case r1 = <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("Server did not receive the request")
	}
	if err := <-done; err != ErrTimeout {
		t.Fatalf("Expected %v, got %v", ErrTimeout, err)
	}
	// The waiter (and its inbox) must be gone.
	waitForRespMapLen(t, nc, 0)

	// A late reply for the timed out request must be dropped.
	s.sendMsg(r1.reply, "late")
	// Give the client a chance to (mis)process it.
	time.Sleep(100 * time.Millisecond)
	if n := respMapLen(nc); n != 0 {
		t.Fatalf("Expected no waiters, got %d", n)
	}

	// A subsequent request must work and get its own response.
	done = make(chan error, 1)
	var resp *Msg
	go func() {
		var err error
		resp, err = nc.Request("foo", []byte("two"), 2*time.Second)
		done <- err
	}()
	var r2 reqMsg
	select {
	case r2 = <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("Server did not receive the second request")
	}
	if r2.reply == r1.reply {
		t.Fatalf("Response inbox %q was reused across requests", r2.reply)
	}
	s.sendMsg(r2.reply, "two")
	if err := <-done; err != nil {
		t.Fatalf("Unexpected request error: %v", err)
	}
	if string(resp.Data) != "two" {
		t.Fatalf("Expected response %q, got %q", "two", resp.Data)
	}
}

// On context cancellation, the response waiter must be removed and a
// late reply must be discarded.
func TestRequestWithContextCancelRemovesWaiter(t *testing.T) {
	s := newMockNATSServer(t)
	defer s.close()

	received := make(chan string, 10)
	s.onRequest = func(payload []byte, reply string) {
		received <- reply
	}

	nc, err := Connect(s.url)
	if err != nil {
		t.Fatalf("Unexpected connect error: %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := nc.RequestWithContext(ctx, "foo", []byte("req"))
		done <- err
	}()
	var reply string
	select {
	case reply = <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("Server did not receive the request")
	}
	waitForRespMapLen(t, nc, 1)

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Expected %v, got %v", context.Canceled, err)
	}
	// The waiter (and its inbox) must be gone.
	waitForRespMapLen(t, nc, 0)

	// A late reply for the cancelled request must be dropped.
	s.sendMsg(reply, "late")
	time.Sleep(100 * time.Millisecond)
	if n := respMapLen(nc); n != 0 {
		t.Fatalf("Expected no waiters, got %d", n)
	}
}

// A request that fails to publish must not leave a waiter behind.
func TestRequestPublishErrorRemovesWaiter(t *testing.T) {
	s := newMockNATSServer(t)
	defer s.close()

	nc, err := Connect(s.url)
	if err != nil {
		t.Fatalf("Unexpected connect error: %v", err)
	}
	nc.Close()

	if _, err := nc.Request("foo", []byte("req"), time.Second); err != ErrConnectionClosed {
		t.Fatalf("Expected %v, got %v", ErrConnectionClosed, err)
	}
	if n := respMapLen(nc); n != 0 {
		t.Fatalf("Expected no waiters after failed request, got %d", n)
	}

	if _, err := nc.RequestWithContext(context.Background(), "foo", []byte("req")); err != ErrConnectionClosed {
		t.Fatalf("Expected %v, got %v", ErrConnectionClosed, err)
	}
	if n := respMapLen(nc); n != 0 {
		t.Fatalf("Expected no waiters after failed request, got %d", n)
	}
}

// A reconnect must bump the response generation so that new requests
// get response inboxes that replies from the old connection can not
// match. Stale replies to old inboxes must be dropped without
// affecting subsequent requests.
func TestRequestReconnectGenerationIsolation(t *testing.T) {
	s := newMockNATSServer(t)
	defer s.close()

	type reqMsg struct {
		payload, reply string
	}
	received := make(chan reqMsg, 10)
	s.onRequest = func(payload []byte, reply string) {
		received <- reqMsg{string(payload), reply}
	}

	reconnected := make(chan bool, 1)
	nc, err := Connect(s.url,
		ReconnectWait(10*time.Millisecond),
		ReconnectJitter(0, 0),
		ReconnectHandler(func(_ *Conn) { reconnected <- true }),
	)
	if err != nil {
		t.Fatalf("Unexpected connect error: %v", err)
	}
	defer nc.Close()

	// First request on the initial connection generation; it is never
	// answered and will time out.
	req1Done := make(chan error, 1)
	go func() {
		_, err := nc.Request("foo", []byte("one"), 2*time.Second)
		req1Done <- err
	}()
	var r1 reqMsg
	select {
	case r1 = <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("Server did not receive the first request")
	}

	nc.mu.Lock()
	gen0, prefix := nc.respGen, nc.respSubPrefix
	nc.mu.Unlock()

	// Force a reconnect.
	s.closeConn()
	if err := Wait(reconnected); err != nil {
		t.Fatalf("Did not reconnect: %v", err)
	}
	nc.Flush()

	nc.mu.Lock()
	gen1 := nc.respGen
	nc.mu.Unlock()
	if gen1 <= gen0 {
		t.Fatalf("Expected generation to increase after reconnect, was %d, now %d", gen0, gen1)
	}

	// The old request times out and its waiter is removed.
	if err := <-req1Done; err != ErrTimeout {
		t.Fatalf("Expected %v, got %v", ErrTimeout, err)
	}
	waitForRespMapLen(t, nc, 0)

	// A stale reply to the old inbox, received on the new connection,
	// must be dropped.
	s.sendMsg(r1.reply, "stale")
	time.Sleep(100 * time.Millisecond)
	if n := respMapLen(nc); n != 0 {
		t.Fatalf("Expected no waiters, got %d", n)
	}

	// A new request uses a new-generation inbox.
	req2Done := make(chan error, 1)
	var resp *Msg
	go func() {
		var err error
		resp, err = nc.Request("foo", []byte("two"), 2*time.Second)
		req2Done <- err
	}()
	var r2 reqMsg
	select {
	case r2 = <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("Server did not receive the second request")
	}
	tok1, tok2 := r1.reply[len(prefix):], r2.reply[len(prefix):]
	if tok1 == tok2 || tok1[:respGenSuffixLen] == tok2[:respGenSuffixLen] {
		t.Fatalf("Expected new-generation inbox after reconnect, had %q, got %q", r1.reply, r2.reply)
	}

	// Another stale reply to the old inbox while the new request is the
	// only waiter must not complete it.
	s.sendMsg(r1.reply, "stale")
	select {
	case err := <-req2Done:
		t.Fatalf("New-generation request completed by stale reply (err=%v, resp=%v)", err, resp)
	case <-time.After(200 * time.Millisecond):
	}

	// Its own reply completes it.
	s.sendMsg(r2.reply, "two")
	if err := <-req2Done; err != nil {
		t.Fatalf("Unexpected request error: %v", err)
	}
	if string(resp.Data) != "two" {
		t.Fatalf("Expected response %q, got %q", "two", resp.Data)
	}
}

// A single slow response must not block other multiplexed requests.
func TestRequestMuxConcurrentSlowResponse(t *testing.T) {
	s := newMockNATSServer(t)
	defer s.close()

	slowGate := make(chan struct{})
	s.onRequest = func(payload []byte, reply string) {
		if string(payload) == "slow" {
			go func() {
				<-slowGate
				s.sendMsg(reply, "slow-done")
			}()
			return
		}
		s.sendMsg(reply, "ok")
	}

	nc, err := Connect(s.url)
	if err != nil {
		t.Fatalf("Unexpected connect error: %v", err)
	}
	defer nc.Close()

	const numFast = 50
	fastDone := make(chan error, numFast)
	slowDone := make(chan error, 1)

	go func() {
		m, err := nc.Request("foo", []byte("slow"), 10*time.Second)
		if err == nil && string(m.Data) != "slow-done" {
			err = fmt.Errorf("unexpected slow response: %q", m.Data)
		}
		slowDone <- err
	}()
	// Make sure the slow request is in flight first.
	waitForRespMapLen(t, nc, 1)

	for i := range numFast {
		go func() {
			m, err := nc.Request("foo", []byte(fmt.Sprintf("fast-%d", i)), 10*time.Second)
			if err == nil && string(m.Data) != "ok" {
				err = fmt.Errorf("unexpected fast response: %q", m.Data)
			}
			fastDone <- err
		}()
	}

	// All fast requests must complete while the slow one is still pending.
	for range numFast {
		select {
		case err := <-fastDone:
			if err != nil {
				t.Fatalf("Fast request failed: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Fast requests blocked by a slow response")
		}
	}
	select {
	case err := <-slowDone:
		t.Fatalf("Slow request completed before its response was released: %v", err)
	default:
	}

	close(slowGate)
	select {
	case err := <-slowDone:
		if err != nil {
			t.Fatalf("Slow request failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Slow request did not complete")
	}
	waitForRespMapLen(t, nc, 0)
}

// Request, RequestMsg, RequestWithContext and RequestMsgWithContext
// must keep working on top of the multiplexed response inbox.
func TestRequestAPIsWithMuxInbox(t *testing.T) {
	s := newMockNATSServer(t)
	defer s.close()

	s.onRequest = func(payload []byte, reply string) {
		s.sendMsg(reply, "echo:"+string(payload))
	}

	nc, err := Connect(s.url)
	if err != nil {
		t.Fatalf("Unexpected connect error: %v", err)
	}
	defer nc.Close()

	check := func(m *Msg, err error, expected string) {
		t.Helper()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if string(m.Data) != expected {
			t.Fatalf("Expected %q, got %q", expected, m.Data)
		}
	}

	m, err := nc.Request("foo", []byte("a"), 2*time.Second)
	check(m, err, "echo:a")

	m, err = nc.RequestMsg(&Msg{Subject: "foo", Data: []byte("b")}, 2*time.Second)
	check(m, err, "echo:b")

	m, err = nc.RequestWithContext(context.Background(), "foo", []byte("c"))
	check(m, err, "echo:c")

	m, err = nc.RequestMsgWithContext(context.Background(), &Msg{Subject: "foo", Data: []byte("d")})
	check(m, err, "echo:d")

	waitForRespMapLen(t, nc, 0)
}
