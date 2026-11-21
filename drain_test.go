// Copyright 2012-2026 The NATS Authors
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
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// drainTestServer is a minimal in-process NATS server used by the drain
// tests. It completes the connection handshake, answers PINGs with PONGs,
// tracks SUB registrations and echoes every published message back to the
// subscriptions with a matching subject (like a real server would).
type drainTestServer struct {
	l    net.Listener
	mu   sync.Mutex
	conn net.Conn
	subs map[int]string
}

func newDrainTestServer(t *testing.T) *drainTestServer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Could not listen on an ephemeral port: %v", err)
	}
	s := &drainTestServer{l: l, subs: make(map[int]string)}
	go s.serve()
	return s
}

func (s *drainTestServer) url() string {
	return fmt.Sprintf("nats://%s", s.l.Addr().String())
}

func (s *drainTestServer) close() {
	s.l.Close()
	s.mu.Lock()
	if s.conn != nil {
		s.conn.Close()
	}
	s.mu.Unlock()
}

func (s *drainTestServer) serve() {
	conn, err := s.l.Accept()
	if err != nil {
		return
	}
	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()
	defer conn.Close()

	if _, err := conn.Write([]byte("INFO {\"server_id\":\"drain-test\",\"version\":\"2.10.0\",\"max_payload\":1048576}\r\n")); err != nil {
		return
	}
	br := bufio.NewReader(conn)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			return
		}
		switch fields[0] {
		case "PING":
			if _, err := conn.Write([]byte("PONG\r\n")); err != nil {
				return
			}
		case "CONNECT", "UNSUB":
			// Nothing to do.
		case "SUB":
			// SUB <subject> [queue] <sid>
			sid, err := strconv.Atoi(fields[len(fields)-1])
			if err != nil {
				return
			}
			s.mu.Lock()
			s.subs[sid] = fields[1]
			s.mu.Unlock()
		case "PUB":
			// PUB <subject> [reply] <size>
			size, err := strconv.Atoi(fields[len(fields)-1])
			if err != nil {
				return
			}
			payload := make([]byte, size+2)
			if _, err := io.ReadFull(br, payload); err != nil {
				return
			}
			subj := fields[1]
			s.mu.Lock()
			for sid, ssubj := range s.subs {
				if ssubj != subj {
					continue
				}
				msg := fmt.Sprintf("MSG %s %d %d\r\n", subj, sid, size)
				if _, err := conn.Write([]byte(msg)); err != nil {
					s.mu.Unlock()
					return
				}
				if _, err := conn.Write(payload); err != nil {
					s.mu.Unlock()
					return
				}
			}
			s.mu.Unlock()
		}
	}
}

// waitForCond polls cond every interval until it returns true or the
// timeout expires, in which case it fails the test.
func waitForCond(t *testing.T, timeout, interval time.Duration, cond func() bool, msg string) {
	t.Helper()
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); {
		if cond() {
			return
		}
		time.Sleep(interval)
	}
	t.Fatalf("condition not met after %v: %s", timeout, msg)
}

// Once a drain starts, new subscriptions must be rejected, while plain
// publishes and requests are still accepted during the subscription
// drain (so in-flight callbacks can respond, e.g. JetStream AckSync)
// and everything is rejected once the connection is closed.
func TestDrainRejectsNewRequestAndSubscribe(t *testing.T) {
	s := newDrainTestServer(t)
	defer s.close()

	nc, err := Connect(s.url(), DrainTimeout(500*time.Millisecond))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	entered := make(chan struct{})
	release := make(chan struct{})
	if _, err := nc.Subscribe("foo", func(_ *Msg) {
		close(entered)
		<-release
	}); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := nc.Publish("foo", nil); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("callback not entered")
	}

	if err := nc.Drain(); err != nil {
		t.Fatalf("Error on drain: %v", err)
	}
	if !nc.IsDraining() {
		t.Fatal("Expected connection to be draining")
	}

	// New subscriptions are rejected as soon as the drain starts.
	if _, err := nc.Subscribe("bar", func(_ *Msg) {}); !errors.Is(err, ErrConnectionDraining) {
		t.Fatalf("Expected %v on subscribe during drain, got %v", ErrConnectionDraining, err)
	}
	// Plain publishes are still accepted while subscriptions drain.
	if err := nc.Publish("baz", nil); err != nil {
		t.Fatalf("Expected no error on publish during subscription drain, got %v", err)
	}

	// Let the in-flight callback finish so the drain can complete.
	close(release)
	waitForCond(t, 2*time.Second, 10*time.Millisecond, nc.IsClosed, "connection not closed after drain")

	// Once closed, everything is rejected with ErrConnectionClosed.
	if _, err := nc.Request("bar", nil, time.Second); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("Expected %v on request after drain, got %v", ErrConnectionClosed, err)
	}
	if _, err := nc.Subscribe("bar", func(_ *Msg) {}); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("Expected %v on subscribe after drain, got %v", ErrConnectionClosed, err)
	}
	if err := nc.Publish("baz", nil); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("Expected %v on publish after drain, got %v", ErrConnectionClosed, err)
	}
}

// Once the drain moves to the publish phase, both new publishes and new
// requests are rejected with ErrConnectionDraining.
func TestDrainPubsPhaseRejectsPublishAndRequest(t *testing.T) {
	s := newDrainTestServer(t)
	defer s.close()

	nc, err := Connect(s.url())
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Simulate the publish drain phase: subscriptions have been drained
	// and the connection is flushing the last publishes before closing.
	nc.mu.Lock()
	nc.changeConnStatus(DRAINING_PUBS)
	nc.mu.Unlock()

	if err := nc.Publish("foo", nil); !errors.Is(err, ErrConnectionDraining) {
		t.Fatalf("Expected %v on publish during publish drain, got %v", ErrConnectionDraining, err)
	}
	if _, err := nc.Request("foo", nil, time.Second); !errors.Is(err, ErrConnectionDraining) {
		t.Fatalf("Expected %v on request during publish drain, got %v", ErrConnectionDraining, err)
	}
	if _, err := nc.Subscribe("foo", func(_ *Msg) {}); !errors.Is(err, ErrConnectionDraining) {
		t.Fatalf("Expected %v on subscribe during publish drain, got %v", ErrConnectionDraining, err)
	}
}

// Messages already queued on a subscription, and the callback currently
// being executed, must complete before the drain closes the connection.
func TestDrainCompletesInflightSubscriptionMessages(t *testing.T) {
	s := newDrainTestServer(t)
	defer s.close()

	nc, err := Connect(s.url(), DrainTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	const total = 5
	var received int32
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	if _, err := nc.Subscribe("foo", func(_ *Msg) {
		once.Do(func() { close(entered) })
		<-release
		atomic.AddInt32(&received, 1)
	}); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	for i := 0; i < total; i++ {
		if err := nc.Publish("foo", nil); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("callback not entered")
	}

	if err := nc.Drain(); err != nil {
		t.Fatalf("Error on drain: %v", err)
	}

	// While the first callback is still blocked (and 4 messages are
	// queued behind it), the drain must not close the connection.
	time.Sleep(300 * time.Millisecond)
	if nc.IsClosed() {
		t.Fatal("Connection closed while messages were still being processed")
	}

	// Release the callbacks: all queued messages must be delivered
	// before the connection moves to closed.
	close(release)
	waitForCond(t, 2*time.Second, 10*time.Millisecond, nc.IsClosed, "connection not closed after drain")
	if r := atomic.LoadInt32(&received); r != total {
		t.Fatalf("Expected %d messages to be processed during drain, got %d", total, r)
	}
}

// A request still pending when the drain deadline expires must be
// canceled explicitly instead of waiting for its own (much longer)
// timeout.
func TestDrainCancelsPendingRequest(t *testing.T) {
	s := newDrainTestServer(t)
	defer s.close()

	nc, err := Connect(s.url(),
		DrainTimeout(300*time.Millisecond),
		// The drain will time out on the blocked callback; that is
		// expected, so swallow the report.
		ErrorHandler(func(_ *Conn, _ *Subscription, _ error) {}),
	)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Hold the drain in the subscription phase with a blocked callback.
	entered := make(chan struct{})
	release := make(chan struct{})
	if _, err := nc.Subscribe("foo", func(_ *Msg) {
		close(entered)
		<-release
	}); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := nc.Publish("foo", nil); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("callback not entered")
	}

	// Issue a request that will never be answered (no responder).
	reqErr := make(chan error, 1)
	go func() {
		_, err := nc.Request("no.reply", nil, 30*time.Second)
		reqErr <- err
	}()
	// Wait for the request to be registered: the shared response
	// subscription shows up next to the "foo" subscription.
	waitForCond(t, 2*time.Second, 5*time.Millisecond, func() bool {
		return nc.NumSubscriptions() == 2
	}, "request not registered")

	if err := nc.Drain(); err != nil {
		t.Fatalf("Error on drain: %v", err)
	}

	// The pending request must be canceled by the drain, well before
	// its own 30s timeout.
	select {
	case err := <-reqErr:
		if !errors.Is(err, ErrConnectionDraining) {
			t.Fatalf("Expected %v for request canceled by drain, got %v", ErrConnectionDraining, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Pending request was not canceled by the drain")
	}

	close(release)
	waitForCond(t, 2*time.Second, 10*time.Millisecond, nc.IsClosed, "connection not closed after drain")
}

// When the drain deadline expires with messages still unprocessed, the
// timeout must be reported (ErrDrainTimeout) and the connection must
// still close safely.
func TestDrainTimeoutReportsAndCloses(t *testing.T) {
	s := newDrainTestServer(t)
	defer s.close()

	errCh := make(chan error, 10)
	nc, err := Connect(s.url(),
		DrainTimeout(200*time.Millisecond),
		ErrorHandler(func(_ *Conn, _ *Subscription, err error) { errCh <- err }),
	)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	if _, err := nc.Subscribe("foo", func(_ *Msg) {
		close(entered)
		<-release
	}); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := nc.Publish("foo", nil); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("callback not entered")
	}

	if err := nc.Drain(); err != nil {
		t.Fatalf("Error on drain: %v", err)
	}

	// The callback is still blocked when the deadline expires: the drain
	// must report that not everything completed.
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrDrainTimeout) {
			t.Fatalf("Expected %v, got %v", ErrDrainTimeout, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Drain timeout was not reported")
	}

	// ... and the connection must still end up closed.
	waitForCond(t, 2*time.Second, 10*time.Millisecond, nc.IsClosed, "connection not closed after drain timeout")
}

// A subscription drain must be bounded by the drain timeout: if the
// pending messages can not be processed in time, the subscription is
// removed anyway and the timeout is reported, instead of the drain
// waiting forever.
func TestSubscriptionDrainTimeout(t *testing.T) {
	s := newDrainTestServer(t)
	defer s.close()

	errCh := make(chan error, 10)
	nc, err := Connect(s.url(),
		DrainTimeout(200*time.Millisecond),
		ErrorHandler(func(_ *Conn, _ *Subscription, err error) { errCh <- err }),
	)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	sub, err := nc.Subscribe("foo", func(_ *Msg) {
		close(entered)
		<-release
	})
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := nc.Publish("foo", nil); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("callback not entered")
	}

	if err := sub.Drain(); err != nil {
		t.Fatalf("Error on subscription drain: %v", err)
	}

	// Even though the callback never finishes, the subscription must be
	// removed once the drain timeout expires...
	waitForCond(t, 2*time.Second, 10*time.Millisecond, func() bool {
		return nc.NumSubscriptions() == 0
	}, "subscription not removed after drain timeout")

	// ... the timeout must be reported...
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrDrainTimeout) {
			t.Fatalf("Expected %v, got %v", ErrDrainTimeout, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Drain timeout was not reported")
	}

	// ... and the subscription must leave the draining state.
	waitForCond(t, 2*time.Second, 10*time.Millisecond, func() bool {
		return !sub.IsDraining()
	}, "subscription still draining after drain timeout")

	// The connection itself stays usable.
	if !nc.IsConnected() {
		t.Fatal("Connection should still be connected after subscription drain timeout")
	}
}

// Drain, Close and repeated Drain calls racing each other must each
// clean up once and leave the connection closed (never resurrected to a
// draining state).
func TestDrainConcurrentClose(t *testing.T) {
	s := newDrainTestServer(t)
	defer s.close()

	nc, err := Connect(s.url(), DrainTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	sub, err := nc.Subscribe("foo", func(_ *Msg) {
		close(entered)
		<-release
	})
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := nc.Publish("foo", nil); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("callback not entered")
	}

	if err := nc.Drain(); err != nil {
		t.Fatalf("Error on drain: %v", err)
	}
	waitForCond(t, 2*time.Second, 5*time.Millisecond, sub.IsDraining, "subscription not draining")

	// Race Close against repeated Drain calls.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); nc.Close() }()
		go func() { defer wg.Done(); nc.Drain() }()
	}
	wg.Wait()

	if !nc.IsClosed() {
		t.Fatalf("Connection not closed after concurrent Close/Drain: %v", nc.Status())
	}
	// The drain goroutine must not flip the closed connection back to a
	// draining state.
	time.Sleep(300 * time.Millisecond)
	if nc.Status() != CLOSED {
		t.Fatalf("Connection resurrected after Close: %v", nc.Status())
	}
}

// A reconnect must not be forced on a draining connection: the drain is
// terminal, and the old drain must not tear down a new connection.
func TestDrainForceReconnectRejected(t *testing.T) {
	s := newDrainTestServer(t)
	defer s.close()

	nc, err := Connect(s.url(), DrainTimeout(300*time.Millisecond))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	entered := make(chan struct{})
	release := make(chan struct{})
	if _, err := nc.Subscribe("foo", func(_ *Msg) {
		close(entered)
		<-release
	}); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := nc.Publish("foo", nil); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("callback not entered")
	}

	if err := nc.Drain(); err != nil {
		t.Fatalf("Error on drain: %v", err)
	}
	if err := nc.ForceReconnect(); !errors.Is(err, ErrConnectionDraining) {
		t.Fatalf("Expected %v on force reconnect during drain, got %v", ErrConnectionDraining, err)
	}

	// The drain still completes and closes the connection.
	close(release)
	waitForCond(t, 2*time.Second, 10*time.Millisecond, nc.IsClosed, "connection not closed after drain")
}
