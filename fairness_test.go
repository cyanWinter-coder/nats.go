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

// White-box tests for subscription-level fair scheduling: a slow
// subscription must only affect its own pending queue, pending limits
// must produce identifiable errors, drain/close must deliver or discard
// deterministically, and unsubscribe racing reconnect must not duplicate
// deliveries or leak goroutines.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newBareConn returns a minimal Conn that can inject messages through
// processMsg without a network connection. Protocol output is discarded.
func newBareConn() *Conn {
	nc := &Conn{
		Opts: GetDefaultOptions(),
		subs: make(map[int64]*Subscription),
		fch:  make(chan struct{}, 1),
		ps:   &parseState{},
	}
	nc.bw = &natsWriter{w: io.Discard, limit: 32768}
	nc.ach = &asyncCallbacksHandler{}
	nc.ach.cond = sync.NewCond(&nc.ach.mu)
	go nc.ach.asyncCBDispatcher()
	return nc
}

// injectTestMsg delivers a message to the subscription with the given sid,
// mimicking what the parser does when a MSG arrives from the server.
func injectTestMsg(nc *Conn, sid int64, subject string, data []byte) {
	nc.ps.ma.sid = sid
	nc.ps.ma.subject = []byte(subject)
	nc.ps.ma.reply = nil
	nc.ps.ma.hdr = -1
	nc.ps.msgCopied = true
	nc.processMsg(data)
}

// answerTestPongs simulates the server's PONG replies (as processPong would
// be called from the read loop) until the returned function is called.
func answerTestPongs(nc *Conn) func() {
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			nc.mu.Lock()
			n := len(nc.pongs)
			nc.mu.Unlock()
			if n == 0 {
				time.Sleep(time.Millisecond)
				continue
			}
			nc.processPong()
		}
	}()
	return func() { close(stop); wg.Wait() }
}

// waitForCond polls f until it returns nil or the timeout expires.
func waitForCond(t *testing.T, timeout time.Duration, f func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := f()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestSubscriptionFairnessSlowConsumerDoesNotBlockOthers(t *testing.T) {
	nc := newBareConn()
	defer nc.ach.close()
	stopAnswering := answerTestPongs(nc)
	defer stopAnswering()

	// Slow subscription: its handler blocks until released.
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	slow, err := nc.Subscribe("slow", func(m *Msg) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := slow.SetPendingLimits(8, -1); err != nil {
		t.Fatalf("SetPendingLimits: %v", err)
	}

	fastReceived := make(chan struct{}, 200)
	fast, err := nc.Subscribe("fast", func(m *Msg) {
		fastReceived <- struct{}{}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Wait for the slow handler to hold its first message so its
	// pending queue backs up.
	injectTestMsg(nc, slow.sid, "slow", []byte("x"))
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("slow handler did not start")
	}

	// Injecting to a subscription that is over its limits must never
	// block the read loop (here: the injecting goroutine).
	const slowMsgs, fastMsgs = 49, 100
	injectDone := make(chan struct{})
	go func() {
		defer close(injectDone)
		for i := 0; i < slowMsgs; i++ {
			injectTestMsg(nc, slow.sid, "slow", []byte("x"))
		}
		for i := 0; i < fastMsgs; i++ {
			injectTestMsg(nc, fast.sid, "fast", []byte("x"))
		}
	}()
	select {
	case <-injectDone:
	case <-time.After(2 * time.Second):
		t.Fatal("processMsg blocked on a slow subscription")
	}

	// The fast subscription must receive everything despite the slow one
	// being backed up.
	for i := 0; i < fastMsgs; i++ {
		select {
		case <-fastReceived:
		case <-time.After(5 * time.Second):
			t.Fatalf("fast subscription starved by slow one after %d/%d messages", i, fastMsgs)
		}
	}

	// The ping/flush path must not be affected either.
	if err := nc.FlushTimeout(2 * time.Second); err != nil {
		t.Fatalf("flush blocked by slow subscription: %v", err)
	}

	// Every message sent to the slow subscription was either accepted
	// into its own queue or explicitly dropped and counted.
	st := slow.Stats()
	if st.Blocked == 0 {
		t.Fatal("expected blocked events on the slow subscription")
	}
	if st.Dropped == 0 {
		t.Fatal("expected dropped messages on the slow subscription")
	}
	if st.Received+int64(st.Dropped) != slowMsgs+1 {
		t.Fatalf("accounting mismatch: received %d + dropped %d != injected %d",
			st.Received, st.Dropped, slowMsgs+1)
	}
	if fst := fast.Stats(); fst.Dropped != 0 || fst.Blocked != 0 || fst.Delivered != fastMsgs {
		t.Fatalf("fast subscription affected by slow one: %+v", fst)
	}

	// Let the slow subscription drain; everything it accepted must be
	// delivered, in order, with nothing left pending.
	close(release)
	waitForCond(t, 5*time.Second, func() error {
		st := slow.Stats()
		if st.PendingMsgs != 0 || st.Delivered != st.Received {
			return fmt.Errorf("waiting for drain: %+v", st)
		}
		return nil
	})

	slow.Unsubscribe()
	fast.Unsubscribe()
}

func TestSubscriptionPendingLimitErrorIdentifiable(t *testing.T) {
	errCh := make(chan error, 10)
	nc := newBareConn()
	defer nc.ach.close()
	nc.Opts.AsyncErrorCB = func(_ *Conn, _ *Subscription, err error) {
		errCh <- err
	}

	expectErr := func(t *testing.T, want, notWant error) {
		t.Helper()
		select {
		case err := <-errCh:
			if !errors.Is(err, ErrSlowConsumer) {
				t.Fatalf("expected ErrSlowConsumer, got %v", err)
			}
			if !errors.Is(err, want) {
				t.Fatalf("expected %v, got %v", want, err)
			}
			if notWant != nil && errors.Is(err, notWant) {
				t.Fatalf("did not expect %v in %v", notWant, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("did not receive async error %v", want)
		}
		if last := nc.LastError(); last != ErrSlowConsumer {
			t.Fatalf("LastError should remain ErrSlowConsumer, got %v", last)
		}
	}

	t.Run("MessagesLimit", func(t *testing.T) {
		sub, err := nc.SubscribeSync("msgs.limit")
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()
		if err := sub.SetPendingLimits(2, -1); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 5; i++ {
			injectTestMsg(nc, sub.sid, "msgs.limit", []byte("x"))
		}
		expectErr(t, ErrSlowConsumerMsgsLimit, ErrSlowConsumerBytesLimit)
		st := sub.Stats()
		if st.Blocked != 3 || st.Dropped != 3 || st.Received != 2 {
			t.Fatalf("unexpected stats: %+v", st)
		}
	})

	t.Run("BytesLimit", func(t *testing.T) {
		sub, err := nc.SubscribeSync("bytes.limit")
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()
		// Each message accounts for len(data) = 10 pending bytes.
		if err := sub.SetPendingLimits(-1, 25); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 5; i++ {
			injectTestMsg(nc, sub.sid, "bytes.limit", []byte("0123456789"))
		}
		expectErr(t, ErrSlowConsumerBytesLimit, ErrSlowConsumerMsgsLimit)
		st := sub.Stats()
		if st.Blocked == 0 || st.Dropped == 0 || st.Received+int64(st.Dropped) != 5 {
			t.Fatalf("unexpected stats: %+v", st)
		}
	})

	t.Run("ChannelFull", func(t *testing.T) {
		ch := make(chan *Msg, 2)
		sub, err := nc.ChanSubscribe("chan.full", ch)
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()
		for i := 0; i < 5; i++ {
			injectTestMsg(nc, sub.sid, "chan.full", []byte("x"))
		}
		expectErr(t, ErrSlowConsumerMsgsLimit, ErrSlowConsumerBytesLimit)
		st := sub.Stats()
		if st.Blocked != 3 || st.Dropped != 3 || st.Received != 2 || st.Delivered != 2 {
			t.Fatalf("unexpected stats: %+v", st)
		}
		if len(ch) != 2 {
			t.Fatalf("expected 2 messages in the channel, got %d", len(ch))
		}
	})
}

func TestSubscriptionStats(t *testing.T) {
	nc := newBareConn()
	defer nc.ach.close()

	sub, err := nc.SubscribeSync("stats")
	if err != nil {
		t.Fatal(err)
	}
	const msgs = 5
	// pendingSize accounts for the message payload only.
	const wsz = 5
	for i := 0; i < msgs; i++ {
		injectTestMsg(nc, sub.sid, "stats", []byte("hello"))
	}
	st := sub.Stats()
	if st.Received != msgs || st.Delivered != 0 || st.Dropped != 0 || st.Blocked != 0 {
		t.Fatalf("unexpected stats after inject: %+v", st)
	}
	if st.PendingMsgs != msgs || st.PendingBytes != msgs*wsz {
		t.Fatalf("unexpected pending: %+v", st)
	}

	for i := 0; i < 2; i++ {
		if _, err := sub.NextMsg(time.Second); err != nil {
			t.Fatal(err)
		}
	}
	st = sub.Stats()
	if st.Delivered != 2 || st.PendingMsgs != 3 || st.PendingBytes != 3*wsz {
		t.Fatalf("unexpected stats after NextMsg: %+v", st)
	}

	// Stats remains available after the subscription is closed.
	if err := sub.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	st = sub.Stats()
	if st.Received != msgs || st.Delivered != 2 {
		t.Fatalf("unexpected stats after unsubscribe: %+v", st)
	}

	// A nil subscription returns the zero value.
	var nilSub *Subscription
	if zst := nilSub.Stats(); zst != (SubStats{}) {
		t.Fatalf("expected zero stats for nil subscription, got %+v", zst)
	}
}

func TestSubscriptionDrainDeliversPendingInOrder(t *testing.T) {
	nc := newBareConn()
	defer nc.ach.close()
	stopAnswering := answerTestPongs(nc)
	defer stopAnswering()

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var mu sync.Mutex
	var order []int
	sub, err := nc.Subscribe("drain", func(m *Msg) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		mu.Lock()
		order = append(order, int(m.Data[0]))
		mu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}
	stCh := sub.StatusChanged(SubscriptionClosed)

	const total = 20
	injectTestMsg(nc, sub.sid, "drain", []byte{0})
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}
	for i := 1; i < total; i++ {
		injectTestMsg(nc, sub.sid, "drain", []byte{byte(i)})
	}

	// Draining must deliver every pending message, in receive order,
	// before the subscription is closed.
	if err := sub.Drain(); err != nil {
		t.Fatalf("drain: %v", err)
	}
	close(release)
	select {
	case st := <-stCh:
		if st != SubscriptionClosed {
			t.Fatalf("expected closed status, got %v", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("subscription did not close after drain")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(order) != total {
		t.Fatalf("expected %d delivered messages, got %d", total, len(order))
	}
	for i := 0; i < total; i++ {
		if order[i] != i {
			t.Fatalf("messages delivered out of order at %d: %v", i, order)
		}
	}
	if st := sub.Stats(); st.Received != total || st.Delivered != total || st.Dropped != 0 {
		t.Fatalf("unexpected stats after drain: %+v", st)
	}
}

func TestSubscriptionCloseDiscardsPending(t *testing.T) {
	nc := newBareConn()

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	sub, err := nc.Subscribe("close", func(m *Msg) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	})
	if err != nil {
		t.Fatal(err)
	}
	closedHandler := make(chan string, 1)
	sub.SetClosedHandler(func(subj string) { closedHandler <- subj })

	const total = 10
	for i := 0; i < total; i++ {
		injectTestMsg(nc, sub.sid, "close", []byte("x"))
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}

	// Closing with a backlog must not deliver partial batches silently:
	// undelivered messages are discarded and explicitly accounted.
	nc.Close()
	close(release)
	select {
	case <-closedHandler:
	case <-time.After(5 * time.Second):
		t.Fatal("delivery goroutine did not exit on close")
	}

	st := sub.Stats()
	if st.Received != total {
		t.Fatalf("expected %d received, got %+v", total, st)
	}
	if st.Delivered != 1 {
		t.Fatalf("expected 1 delivered, got %+v", st)
	}
	if st.Dropped != total-1 {
		t.Fatalf("expected %d dropped on close, got %+v", total-1, st)
	}
	if st.PendingMsgs != 0 || st.PendingBytes != 0 {
		t.Fatalf("expected nothing left pending, got %+v", st)
	}
}

func TestSubscriptionUnsubscribeConcurrentResend(t *testing.T) {
	nc := newBareConn()
	defer nc.ach.close()

	const numSubs = 20
	var exitWg sync.WaitGroup
	exitWg.Add(numSubs)
	var delivered atomic.Int64
	subs := make([]*Subscription, numSubs)
	for i := range subs {
		sub, err := nc.Subscribe(fmt.Sprintf("sub.%d", i), func(m *Msg) {
			delivered.Add(1)
		})
		if err != nil {
			t.Fatal(err)
		}
		// The closed handler fires when the delivery goroutine exits.
		sub.SetClosedHandler(func(string) { exitWg.Done() })
		subs[i] = sub
	}
	for _, sub := range subs {
		injectTestMsg(nc, sub.sid, sub.Subject, []byte("x"))
	}

	// Unsubscribe everything while a reconnect resends subscriptions.
	var wg sync.WaitGroup
	for _, sub := range subs {
		wg.Add(1)
		go func(s *Subscription) {
			defer wg.Done()
			s.Unsubscribe()
		}(sub)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			nc.resendSubscriptions()
		}
	}()
	wg.Wait()

	// All delivery goroutines must exit: no leaked goroutines or timers.
	exited := make(chan struct{})
	go func() { exitWg.Wait(); close(exited) }()
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("delivery goroutines leaked after unsubscribe")
	}

	// No message may be delivered for a removed subscription, even if a
	// reconnect raced the unsubscribe and resent its sid.
	before := delivered.Load()
	for _, sub := range subs {
		injectTestMsg(nc, sub.sid, sub.Subject, []byte("x"))
	}
	if got := delivered.Load(); got != before {
		t.Fatalf("delivered %d messages after unsubscribe", got-before)
	}
	if n := nc.NumSubscriptions(); n != 0 {
		t.Fatalf("expected no subscriptions left, got %d", n)
	}
}

func TestSubscriptionConcurrentClose(t *testing.T) {
	nc := newBareConn()

	const numSubs = 10
	var exitWg sync.WaitGroup
	exitWg.Add(numSubs)
	subs := make([]*Subscription, numSubs)
	for i := range subs {
		sub, err := nc.Subscribe(fmt.Sprintf("cc.%d", i), func(m *Msg) {
			// Keep a backlog in flight so close races pending messages.
			time.Sleep(time.Millisecond)
		})
		if err != nil {
			t.Fatal(err)
		}
		sub.SetClosedHandler(func(string) { exitWg.Done() })
		subs[i] = sub
	}

	// Inject traffic, unsubscribe half of the subscriptions and close the
	// connection, all concurrently.
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			for _, sub := range subs {
				injectTestMsg(nc, sub.sid, sub.Subject, []byte("x"))
			}
		}
	}()
	go func() {
		defer wg.Done()
		for _, sub := range subs[:numSubs/2] {
			sub.Unsubscribe()
		}
	}()
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		nc.Close()
	}()
	wg.Wait()

	// No delivery goroutine may leak.
	exited := make(chan struct{})
	go func() { exitWg.Wait(); close(exited) }()
	select {
	case <-exited:
	case <-time.After(10 * time.Second):
		t.Fatal("delivery goroutines leaked after concurrent close")
	}

	// With default (huge) pending limits there are no limit drops, so
	// every received message was either delivered or explicitly dropped:
	// no half-delivered messages.
	for _, sub := range subs {
		st := sub.Stats()
		if st.Received != st.Delivered+int64(st.Dropped) {
			t.Fatalf("half-delivered messages on %q: %+v", sub.Subject, st)
		}
	}
}

func TestResendSubscriptionsSkipsClosed(t *testing.T) {
	nc := newBareConn()
	defer nc.ach.close()
	var buf bytes.Buffer
	nc.bw = &natsWriter{w: &buf, limit: 32768}

	keep, err := nc.Subscribe("keep", func(*Msg) {})
	if err != nil {
		t.Fatal(err)
	}
	defer keep.Unsubscribe()
	drop, err := nc.Subscribe("drop", func(*Msg) {})
	if err != nil {
		t.Fatal(err)
	}
	if err := drop.Unsubscribe(); err != nil {
		t.Fatal(err)
	}

	nc.resendSubscriptions()
	out := buf.String()
	if !strings.Contains(out, "SUB keep") {
		t.Fatalf("active subscription not resent: %q", out)
	}
	if strings.Contains(out, "SUB drop") {
		t.Fatalf("closed subscription was resent to the server: %q", out)
	}
}
