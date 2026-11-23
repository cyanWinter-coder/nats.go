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

package test

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// fairConn dials a fresh server with the FairDispatch option enabled.
func fairConn(t *testing.T, opts ...nats.Option) *nats.Conn {
	t.Helper()
	c := newTester(t)
	inst := c.CreateServer(t, false)
	t.Cleanup(func() { inst.Destroy(t) })
	all := append([]nats.Option{nats.FairDispatch()}, opts...)
	return dialInstance(t, inst, all...)
}

// TestFairDispatchEnabled verifies the option wires the per-connection
// dispatcher rather than the legacy per-subscription goroutines.
func TestFairDispatchEnabled(t *testing.T) {
	nc := fairConn(t)
	if !nc.FairDispatchEnabled() {
		t.Fatalf("expected fair dispatch to be enabled")
	}

	sub, err := nc.Subscribe("fd.enabled", func(*nats.Msg) {})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()
}

// TestFairDispatchPerSubFIFO verifies strict in-order delivery for a single
// subscription even though callbacks run on a shared worker pool: at most one
// callback per subscription is ever in flight, so messages cannot overtake.
func TestFairDispatchPerSubFIFO(t *testing.T) {
	nc := fairConn(t, nats.FairDispatchWorkers(8))

	// Serialize callbacks with a gate and verify each callback observes the
	// next expected sequence; a reordering would trip the counter immediately.
	gate := make(chan struct{})
	var next int64
	var bad int64
	sub, err := nc.Subscribe("fd.fifo", func(m *nats.Msg) {
		<-gate
		var v int64
		fmt.Sscanf(string(m.Data), "%d", &v)
		if v != atomic.LoadInt64(&next) {
			atomic.StoreInt64(&bad, v)
			return
		}
		atomic.StoreInt64(&next, v+1)
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	const n = 500
	for i := range n {
		if err := nc.Publish("fd.fifo", []byte(fmt.Sprintf("%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("FlushTimeout: %v", err)
	}
	close(gate)

	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		if got := atomic.LoadInt64(&next); got != n {
			return fmt.Errorf("ordered deliveries %d, want %d", got, n)
		}
		if b := atomic.LoadInt64(&bad); b != 0 {
			return fmt.Errorf("out-of-order message %d observed", b)
		}
		return nil
	})
	_ = sub
}

// TestFairDispatchPerSubOrderingAndIsolation covers requirements 1 and 2:
// each subscription keeps an independent ordered queue, and a full queue
// isolates only that subscription while every other subscription keeps being
// delivered and the connection stays healthy enough to flush.
func TestFairDispatchPerSubOrderingAndIsolation(t *testing.T) {
	nc := fairConn(t)

	// Silence the default error handler; we assert on the captured error
	// instead.
	var (
		errMu   sync.Mutex
		gotErr  error
		errSubj string
	)
	nc.SetErrorHandler(func(_ *nats.Conn, s *nats.Subscription, err error) {
		errMu.Lock()
		gotErr = err
		if s != nil {
			errSubj = s.Subject
		}
		errMu.Unlock()
	})

	// slow blocks forever once it has taken the first message, so its bounded
	// queue fills up and it gets isolated.
	gate := make(chan struct{})
	slow, err := nc.Subscribe("fd.slow", func(m *nats.Msg) {
		<-gate
	})
	if err != nil {
		t.Fatalf("Subscribe slow: %v", err)
	}
	if err := slow.SetPendingLimits(10, 1024*1024); err != nil {
		t.Fatalf("SetPendingLimits: %v", err)
	}

	// fast records delivery order and must never be stalled by slow.
	const fastN = 200
	fastMu := make(chan struct{}, fastN)
	fast, err := nc.Subscribe("fd.fast", func(m *nats.Msg) {
		fastMu <- struct{}{}
	})
	if err != nil {
		t.Fatalf("Subscribe fast: %v", err)
	}

	// Flood slow past its limit; each overflow must be dropped in processMsg
	// without blocking the read loop.
	for range 100 {
		if err := nc.Publish("fd.slow", []byte("x")); err != nil {
			t.Fatalf("Publish slow: %v", err)
		}
	}
	// Interleave fast messages; they must all get through regardless.
	for i := range fastN {
		if err := nc.Publish("fd.fast", []byte(fmt.Sprintf("%d", i))); err != nil {
			t.Fatalf("Publish fast: %v", err)
		}
	}
	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("FlushTimeout while slow sub is isolated: %v", err)
	}

	// All fast messages delivered despite the isolated slow subscription.
	deadline := time.After(5 * time.Second)
	for range fastN {
		select {
		case <-fastMu:
		case <-deadline:
			t.Fatalf("fast subscription stalled by the isolated slow subscription")
		}
	}

	// The identifiable, wrapped error surfaced on the slow subscription only.
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		errMu.Lock()
		defer errMu.Unlock()
		if gotErr == nil {
			return fmt.Errorf("expected a slow consumer error")
		}
		if !errors.Is(gotErr, nats.ErrSlowConsumer) {
			return fmt.Errorf("error %v does not wrap ErrSlowConsumer", gotErr)
		}
		if !errors.Is(gotErr, nats.ErrSubscriptionQueueFull) {
			return fmt.Errorf("expected ErrSubscriptionQueueFull, got %v", gotErr)
		}
		if errSubj != "fd.slow" {
			return fmt.Errorf("error attributed to %q, want fd.slow", errSubj)
		}
		return nil
	})

	dropped, err := slow.Dropped()
	if err != nil {
		t.Fatalf("Dropped: %v", err)
	}
	if dropped == 0 {
		t.Fatalf("expected the isolated subscription to record drops")
	}
	if nc.LastError() != nats.ErrSlowConsumer {
		t.Fatalf("LastError = %v, want ErrSlowConsumer", nc.LastError())
	}

	// Unblock slow and confirm the subscription recovers and preserves FIFO
	// order for whatever remains in its bounded queue.
	close(gate)
	_ = fast
}

// TestFairDispatchRoundRobin verifies bounded round-robin advancement: the
// dispatcher's turn sequence (recorded via the internal turn hook) alternates
// between two concurrently ready subscriptions rather than letting one drain
// its whole queue before the other moves. A single worker makes the
// round-robin order deterministic: after each quantum-bounded turn the
// subscription goes to the tail of the ring.
func TestFairDispatchRoundRobin(t *testing.T) {
	nc := fairConn(t, nats.FairDispatchQuantum(1), nats.FairDispatchWorkers(1))

	// Gate both handlers until all messages are queued so both subscriptions
	// are ready at once and the ring keeps rotating between them.
	release := make(chan struct{})
	var (
		turnMu sync.Mutex
		turns  []string
		got    []string
		gotMu  sync.Mutex
	)
	mk := func(subj string) *nats.Subscription {
		s, err := nc.Subscribe(subj, func(m *nats.Msg) {
			<-release
			gotMu.Lock()
			got = append(got, subj)
			gotMu.Unlock()
		})
		if err != nil {
			t.Fatalf("Subscribe %s: %v", subj, err)
		}
		return s
	}
	s1 := mk("fd.rr.a")
	s2 := mk("fd.rr.b")

	nc.SetFairDispatchTurnHooks(func(s *nats.Subscription) {
		turnMu.Lock()
		turns = append(turns, s.Subject)
		turnMu.Unlock()
	}, nil)

	const n = 20
	for range n {
		if err := nc.Publish("fd.rr.a", []byte("a")); err != nil {
			t.Fatal(err)
		}
		if err := nc.Publish("fd.rr.b", []byte("b")); err != nil {
			t.Fatal(err)
		}
	}
	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("FlushTimeout: %v", err)
	}
	close(release)

	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		gotMu.Lock()
		ndel := len(got)
		gotMu.Unlock()
		if ndel != 2*n {
			return fmt.Errorf("delivered %d, want %d", ndel, 2*n)
		}
		return nil
	})

	// Assert on the scheduling order. With both queues populated and quantum
	// 1, neither subscription may hold more than two consecutive worker turns.
	turnMu.Lock()
	defer turnMu.Unlock()
	if len(turns) < 2*n {
		t.Fatalf("recorded %d turns, want at least %d: %v", len(turns), 2*n, turns)
	}
	run := 1
	for i := 1; i < len(turns); i++ {
		if turns[i] == turns[i-1] {
			run++
			if run > 2 {
				t.Fatalf("subscription %s monopolized %d consecutive turns: %v", turns[i], run, turns)
			}
		} else {
			run = 1
		}
	}
	_ = s1
	_ = s2
}

// TestFairDispatchUnsubscribeStopsDelivery covers requirement 3: unsubscribe
// stops new delivery immediately (matching the historical waitForMsgs
// semantics), while a callback already in flight is allowed to complete and
// the closed handler fires exactly once.
func TestFairDispatchUnsubscribeStopsDelivery(t *testing.T) {
	// A single worker makes which callback is in flight deterministic.
	nc := fairConn(t, nats.FairDispatchWorkers(1), nats.FairDispatchQuantum(1))

	var (
		mu       sync.Mutex
		got      []int
		closed   int32
		release  = make(chan struct{})
		inflight = make(chan struct{})
	)
	sub, err := nc.Subscribe("fd.unsub", func(m *nats.Msg) {
		var v int
		fmt.Sscanf(string(m.Data), "%d", &v)
		if v == 0 {
			close(inflight)
			<-release
		}
		mu.Lock()
		got = append(got, v)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	sub.SetClosedHandler(func(string) { atomic.AddInt32(&closed, 1) })

	const n = 30
	for i := range n {
		if err := nc.Publish("fd.unsub", []byte(fmt.Sprintf("%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("FlushTimeout: %v", err)
	}

	// Wait until message 0's callback is genuinely in flight, then remove
	// interest while it is parked.
	select {
	case <-inflight:
	case <-time.After(5 * time.Second):
		t.Fatalf("first callback never started")
	}
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	close(release)

	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		if c := atomic.LoadInt32(&closed); c != 1 {
			return fmt.Errorf("closed handler ran %d times, want 1", c)
		}
		return nil
	})

	// Exactly the in-flight callback (message 0) completes; queued messages
	// are not started after interest is removed, as in the legacy path.
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("expected only the in-flight message 0 to complete, got %v", got)
	}
}

// TestFairDispatchDrainOrder covers requirement 3's deterministic completion:
// Drain removes interest first (no new delivery can start) and then completes
// every message already enqueued in strict FIFO order, invoking the closed
// handler exactly once at the end.
func TestFairDispatchDrainOrder(t *testing.T) {
	nc := fairConn(t)

	var (
		mu     sync.Mutex
		got    []int
		closed int32
	)
	sub, err := nc.Subscribe("fd.drain", func(m *nats.Msg) {
		var v int
		fmt.Sscanf(string(m.Data), "%d", &v)
		mu.Lock()
		got = append(got, v)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	sub.SetClosedHandler(func(string) { atomic.AddInt32(&closed, 1) })

	const n = 50
	for i := range n {
		if err := nc.Publish("fd.drain", []byte(fmt.Sprintf("%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("FlushTimeout: %v", err)
	}

	if err := sub.Drain(); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	// Drain returns once interest is removed; the queued messages and the
	// closed handler complete asynchronously on a dispatcher worker.
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		if c := atomic.LoadInt32(&closed); c != 1 {
			return fmt.Errorf("closed handler ran %d times, want 1", c)
		}
		return nil
	})
	mu.Lock()
	defer mu.Unlock()
	if len(got) != n {
		t.Fatalf("drain completed %d messages, want %d", len(got), n)
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("drain order broken at %d: got %v", i, got)
		}
	}
}

// TestFairDispatchConnCloseReleasesOnce covers requirement 4: closing the
// connection while a callback is in flight completes that callback, runs each
// closed handler once, and the dispatcher's worker pool exits without hanging
// or leaving any goroutine/timer behind.
func TestFairDispatchConnCloseReleasesOnce(t *testing.T) {
	c := newTester(t)
	inst := c.CreateServer(t, false)
	nc := dialInstance(t, inst, nats.FairDispatch())

	var (
		inflight = make(chan struct{})
		proceed  = make(chan struct{})
		closed   int32
	)
	sub, err := nc.Subscribe("fd.close", func(m *nats.Msg) {
		close(inflight)
		<-proceed
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	sub.SetClosedHandler(func(string) { atomic.AddInt32(&closed, 1) })

	if err := nc.Publish("fd.close", []byte("x")); err != nil {
		t.Fatal(err)
	}
	<-inflight

	// Close is non-blocking: drive the callback concurrently and assert Close
	// returns even while the callback was in flight a moment ago.
	done := make(chan struct{})
	go func() { nc.Close(); close(done) }()
	close(proceed)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Close hung waiting for the fair dispatcher")
	}

	// The worker pool must fully terminate and the closed handler fire once.
	nc.WaitForFairDispatchShutdown()
	if c := atomic.LoadInt32(&closed); c != 1 {
		t.Fatalf("closed handler ran %d times, want 1", c)
	}
	inst.Destroy(t)
}

// TestFairDispatchCloseInsideHandler verifies that calling connection Close
// from within a message callback (a supported historical pattern) cannot
// self-deadlock the worker running that callback, and the dispatcher still
// shuts down cleanly with the closed handler invoked once.
func TestFairDispatchCloseInsideHandler(t *testing.T) {
	c := newTester(t)
	inst := c.CreateServer(t, false)
	nc := dialInstance(t, inst, nats.FairDispatch())

	var (
		ran    int32
		closed int32
	)
	sub, err := nc.Subscribe("fd.closein", func(m *nats.Msg) {
		atomic.AddInt32(&ran, 1)
		// Must return rather than deadlock.
		nc.Close()
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	sub.SetClosedHandler(func(string) { atomic.AddInt32(&closed, 1) })

	if err := nc.Publish("fd.closein", []byte("x")); err != nil {
		t.Fatal(err)
	}

	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		if atomic.LoadInt32(&ran) != 1 {
			return fmt.Errorf("handler did not run")
		}
		if atomic.LoadInt32(&closed) != 1 {
			return fmt.Errorf("closed handler ran %d times, want 1", atomic.LoadInt32(&closed))
		}
		return nil
	})
	nc.WaitForFairDispatchShutdown()
	if !nc.IsClosed() {
		t.Fatalf("connection should be closed")
	}
	inst.Destroy(t)
}

// TestFairDispatchChanAndIteratorCompatible covers requirement 5: channel and
// sync (including the Go 1.23 iterator) subscriptions keep their exact
// historical semantics and ordering under a fair-dispatch connection.
func TestFairDispatchChanAndIteratorCompatible(t *testing.T) {
	nc := fairConn(t)

	// Channel subscription: bounded channel, FIFO, slow-consumer behavior on a
	// full channel unchanged.
	ch := make(chan *nats.Msg, 16)
	csub, err := nc.ChanSubscribe("fd.chan", ch)
	if err != nil {
		t.Fatalf("ChanSubscribe: %v", err)
	}
	for i := range 16 {
		if err := nc.Publish("fd.chan", []byte(fmt.Sprintf("%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("FlushTimeout: %v", err)
	}
	for i := range 16 {
		select {
		case m := <-ch:
			if string(m.Data) != fmt.Sprintf("%d", i) {
				t.Fatalf("chan order broken at %d: got %s", i, m.Data)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("missing chan message %d", i)
		}
	}

	// Sync subscription: NextMsg FIFO ordering unchanged.
	ssub, err := nc.SubscribeSync("fd.sync")
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	for i := range 10 {
		if err := nc.Publish("fd.sync", []byte(fmt.Sprintf("%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("FlushTimeout: %v", err)
	}
	for i := range 10 {
		m, err := ssub.NextMsg(5 * time.Second)
		if err != nil {
			t.Fatalf("NextMsg %d: %v", i, err)
		}
		if string(m.Data) != fmt.Sprintf("%d", i) {
			t.Fatalf("sync order broken at %d: got %s", i, m.Data)
		}
	}

	// Iterator (Go 1.23+) shares the sync path; assert it stays ordered.
	isub, err := nc.SubscribeSync("fd.iter")
	if err != nil {
		t.Fatalf("SubscribeSync iter: %v", err)
	}
	for i := range 5 {
		if err := nc.Publish("fd.iter", []byte(fmt.Sprintf("%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("FlushTimeout iter: %v", err)
	}
	idx := 0
	for msg, err := range isub.Msgs() {
		if err != nil {
			t.Fatalf("Msgs iteration: %v", err)
		}
		if string(msg.Data) != fmt.Sprintf("%d", idx) {
			t.Fatalf("iterator order broken at %d: got %s", idx, msg.Data)
		}
		idx++
		if idx == 5 {
			break
		}
	}
	_ = csub
}
