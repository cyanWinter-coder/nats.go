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

package jetstream

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nats-io/nats.go"
)

// newTestPublisher builds a publisher usable without a connection. Reply
// prefix and RNG are set so registerPAF can allocate reply subjects.
func newTestPublisher(maxPending int) *jetStreamClient {
	js := &jetStreamClient{
		asyncPublisherOpts: asyncPublisherOpts{maxpa: maxPending},
		asyncPublishContext: asyncPublishContext{
			replyPrefix: "test.",
			rr:          rand.New(rand.NewSource(1)),
		},
	}
	return js
}

func TestRegisterPAFRejectsWhenPublisherClosed(t *testing.T) {
	pub := newTestPublisher(1)
	js := &jetStream{publisher: pub}
	pub.closed = true

	if _, err := js.registerPAF(&pubAckFuture{jsClient: pub}, time.Second, nil); !errors.Is(err, ErrJetStreamPublisherClosed) {
		t.Fatalf("expected ErrJetStreamPublisherClosed, got %v", err)
	}
	// A refused registration must not leave anything pending.
	if pending := len(pub.acks); pending != 0 {
		t.Fatalf("expected no pending publishes, got %d", pending)
	}
}

// A caller blocked at the pending cap returns a clear error when its stall
// wait elapses, rather than queueing without bound.
func TestRegisterPAFStallWaitBoundsQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pub := newTestPublisher(1)
		js := &jetStream{publisher: pub}

		if _, err := js.registerPAF(&pubAckFuture{jsClient: pub}, time.Second, nil); err != nil {
			t.Fatalf("first registration: %v", err)
		}

		done := make(chan error, 1)
		go func() {
			_, err := js.registerPAF(&pubAckFuture{jsClient: pub}, 50*time.Millisecond, nil)
			done <- err
		}()
		// The fake clock advances to the timer deadline once every goroutine
		// is blocked; the result is then delivered on done.
		err := <-done
		if !errors.Is(err, ErrTooManyStalledMsgs) {
			t.Fatalf("expected ErrTooManyStalledMsgs, got %v", err)
		}
		if pending := len(pub.acks); pending != 1 {
			t.Fatalf("expected the queue to stay bounded at 1, got %d", pending)
		}
		if pub.stalled != 0 {
			t.Fatalf("expected no stalled callers after the timeout, got %d", pub.stalled)
		}
	})
}

// A canceled context aborts the wait for a slot without publishing anything.
func TestRegisterPAFContextCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pub := newTestPublisher(1)
		js := &jetStream{publisher: pub}

		if _, err := js.registerPAF(&pubAckFuture{jsClient: pub}, time.Second, nil); err != nil {
			t.Fatalf("first registration: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			// A long stall wait: only cancellation can end this call.
			_, err := js.registerPAF(&pubAckFuture{jsClient: pub}, time.Hour, ctx)
			done <- err
		}()
		synctest.Wait()
		cancel()
		synctest.Wait()

		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected context.Canceled, got %v", err)
			}
		default:
			t.Fatal("registerPAF did not abort on context cancellation")
		}
		if pending := len(pub.acks); pending != 1 {
			t.Fatalf("expected only the first publish to be pending, got %d", pending)
		}
	})
}

// PublishAsyncComplete must not close while a caller is still waiting for a
// slot: that caller can still publish and register a future afterwards.
func TestPublishAsyncCompleteWaitsForStalledCaller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pub := newTestPublisher(1)
		js := &jetStream{publisher: pub}

		reply, err := js.registerPAF(&pubAckFuture{jsClient: pub}, time.Second, nil)
		if err != nil {
			t.Fatalf("first registration: %v", err)
		}
		complete := js.PublishAsyncComplete()

		entered := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			close(entered)
			_, err := js.registerPAF(&pubAckFuture{jsClient: pub}, time.Hour, nil)
			done <- err
		}()
		synctest.Wait()
		<-entered

		// Resolving the first publish leaves only the stalled caller, which
		// must keep the completion channel open.
		pub.Lock()
		js.resolvePendingLocked(reply[len(pub.replyPrefix):], ErrAsyncPublishTimeout)()
		synctest.Wait()
		select {
		case <-complete:
			t.Fatal("PublishAsyncComplete closed while a caller was still waiting for a slot")
		default:
		}

		// Freeing the slot lets the waiter register; its result then closes
		// the channel.
		js.clearPAF("nothing")
		synctest.Wait()

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("waiting registration failed: %v", err)
			}
		default:
			t.Fatal("waiting registration did not proceed after a slot freed")
		}
		select {
		case <-complete:
			t.Fatal("PublishAsyncComplete closed while a registered future was still pending")
		default:
		}

		pub.RLock()
		ids := make([]string, 0, len(pub.acks))
		for id := range pub.acks {
			ids = append(ids, id)
		}
		pub.RUnlock()
		for _, id := range ids {
			pub.Lock()
			js.resolvePendingLocked(id, ErrAsyncPublishTimeout)()
		}
		select {
		case <-complete:
		default:
			t.Fatal("PublishAsyncComplete did not close after all publishes resolved")
		}
	})
}

// CleanupPublisher aborts callers waiting for a slot; they must not hang
// until the stall wait, and must report the publisher as closed.
func TestCleanupPublisherAbortsStalledCallers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pub := newTestPublisher(1)
		js := &jetStream{publisher: pub}

		if _, err := js.registerPAF(&pubAckFuture{jsClient: pub}, time.Second, nil); err != nil {
			t.Fatalf("first registration: %v", err)
		}
		complete := js.PublishAsyncComplete()

		done := make(chan error, 1)
		go func() {
			_, err := js.registerPAF(&pubAckFuture{jsClient: pub}, time.Hour, nil)
			done <- err
		}()
		synctest.Wait()

		js.CleanupPublisher()
		synctest.Wait()

		select {
		case err := <-done:
			if !errors.Is(err, ErrJetStreamPublisherClosed) {
				t.Fatalf("expected ErrJetStreamPublisherClosed, got %v", err)
			}
		default:
			t.Fatal("CleanupPublisher did not release the stalled caller")
		}
		select {
		case <-complete:
		default:
			t.Fatal("PublishAsyncComplete was not closed by CleanupPublisher")
		}
	})
}

// A late ack for a future already failed by reconnect must not revive it.
func TestReconnectThenAckResolvesOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pub := newTestPublisher(1)
		js := &jetStream{publisher: pub, opts: JetStreamOptions{replyPrefixLen: len(pub.replyPrefix) + aReplyTokensize}}
		js.opts.replyPrefixLen = len(pub.replyPrefix) // id extraction is prefix-relative

		paf := &pubAckFuture{msg: &nats.Msg{Subject: "foo"}, jsClient: pub}
		reply, err := js.registerPAF(paf, time.Second, nil)
		if err != nil {
			t.Fatalf("registration: %v", err)
		}

		js.failPendingPublishes(nats.ErrDisconnected)
		if pending := js.PublishAsyncPending(); pending != 0 {
			t.Fatalf("expected no pending publishes after reconnect, got %d", pending)
		}

		// The late ack arrives after the future has failed.
		js.handleAsyncReply(&nats.Msg{
			Subject: reply,
			Data:    []byte(`{"stream":"foo","seq":1}`),
		})

		select {
		case err := <-paf.Err():
			if !errors.Is(err, nats.ErrDisconnected) {
				t.Fatalf("expected ErrDisconnected, got %v", err)
			}
		default:
			t.Fatal("expected the reconnect error to be delivered")
		}
		select {
		case ack := <-paf.Ok():
			t.Fatalf("future also reported success after failing: %+v", ack)
		default:
		}
		if pending := js.PublishAsyncPending(); pending != 0 {
			t.Fatalf("late ack re-registered a future, pending=%d", pending)
		}
	})
}

// A no-responders retry that starts after the future was resolved must not
// publish again and must not create a second result.
func TestRetryAfterResolveIsDropped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pub := newTestPublisher(1)
		js := &jetStream{publisher: pub, opts: JetStreamOptions{replyPrefixLen: len(pub.replyPrefix)}}
		js.publisher.asyncPublisherOpts.ackTimeout = time.Hour

		paf := &pubAckFuture{msg: &nats.Msg{Subject: "foo"}, jsClient: pub, maxRetries: 5, retryWait: time.Second}
		reply, err := js.registerPAF(paf, time.Second, nil)
		if err != nil {
			t.Fatalf("registration: %v", err)
		}
		id := reply[len(pub.replyPrefix):]

		// Simulate a no-responders reply that schedules the retry.
		js.handleAsyncReply(&nats.Msg{
			Subject: reply,
			Header:  nats.Header{statusHdr: []string{statusNoResponders}},
		})
		// Resolve the future before the retry timer fires.
		pub.Lock()
		js.resolvePendingLocked(id, ErrAsyncPublishTimeout)()

		synctest.Wait()
		// A retry firing now must observe the future is gone and do nothing.
		js.retryPendingPublish(id)
		synctest.Wait()

		select {
		case err := <-paf.Err():
			if !errors.Is(err, ErrAsyncPublishTimeout) {
				t.Fatalf("expected ErrAsyncPublishTimeout, got %v", err)
			}
		default:
			t.Fatal("expected the timeout result to be delivered")
		}
		select {
		case ack := <-paf.Ok():
			t.Fatalf("retry produced a second result: %+v", ack)
		default:
		}
	})
}

// Concurrent ack, reconnect, timeout and cleanup must leave every future with
// exactly one terminal result.
func TestConcurrentTerminalOutcomesResolveOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const publishes = 200

		pub := newTestPublisher(publishes)
		js := &jetStream{publisher: pub, opts: JetStreamOptions{replyPrefixLen: len(pub.replyPrefix)}}

		futures := make([]*pubAckFuture, 0, publishes)
		replies := make([]string, 0, publishes)
		for i := 0; i < publishes; i++ {
			paf := &pubAckFuture{msg: &nats.Msg{Subject: "foo"}, jsClient: pub, maxRetries: 0}
			reply, err := js.registerPAF(paf, time.Second, nil)
			if err != nil {
				t.Fatalf("registration %d: %v", i, err)
			}
			futures = append(futures, paf)
			replies = append(replies, reply)
		}

		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			for _, reply := range replies {
				js.handleAsyncReply(&nats.Msg{
					Subject: reply,
					Data:    []byte(`{"stream":"foo","seq":1}`),
				})
			}
		}()
		go func() {
			defer wg.Done()
			for _, reply := range replies {
				id := reply[len(pub.replyPrefix):]
				pub.Lock()
				js.resolvePendingLocked(id, ErrAsyncPublishTimeout)()
			}
		}()
		go func() {
			defer wg.Done()
			js.failPendingPublishes(nats.ErrDisconnected)
		}()
		wg.Wait()
		js.CleanupPublisher()

		if pending := js.PublishAsyncPending(); pending != 0 {
			t.Fatalf("expected no pending publishes, got %d", pending)
		}
		for i, paf := range futures {
			acks, errs := 0, 0
			for {
				select {
				case <-paf.Ok():
					acks++
				case <-paf.Err():
					errs++
				default:
					if acks+errs == 0 {
						t.Fatalf("publish %d resolved with neither ack nor error", i)
					}
					if acks > 1 || errs > 1 {
						t.Fatalf("publish %d resolved more than once: acks=%d errs=%d", i, acks, errs)
					}
					goto next
				}
			}
		next:
			if acks == 1 && errs == 1 {
				t.Fatalf("publish %d reported both success and error", i)
			}
		}
	})
}

// registerPAF must never admit more than maxPending publishes, even with many
// concurrent callers, and every rejected caller sees ErrTooManyStalledMsgs.
func TestRegisterPAFConcurrentStaysBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			maxPending = 5
			callers    = 50
		)
		pub := newTestPublisher(maxPending)
		js := &jetStream{publisher: pub}

		var wg sync.WaitGroup
		errs := make(chan error, callers)
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := js.registerPAF(&pubAckFuture{jsClient: pub}, 10*time.Millisecond, nil)
				errs <- err
			}()
		}
		wg.Wait()
		close(errs)

		accepted, rejected := 0, 0
		for err := range errs {
			switch {
			case err == nil:
				accepted++
			case errors.Is(err, ErrTooManyStalledMsgs):
				rejected++
			default:
				t.Fatalf("unexpected error: %v", err)
			}
		}
		if accepted != maxPending {
			t.Fatalf("expected %d accepted publishes, got %d", maxPending, accepted)
		}
		if rejected != callers-maxPending {
			t.Fatalf("expected %d rejected publishes, got %d", callers-maxPending, rejected)
		}
		if pending := len(pub.acks); pending != maxPending {
			t.Fatalf("queue exceeded its bound: %d pending", pending)
		}
	})
}

// PublishAsyncComplete and CleanupPublisher racing each other must not
// deadlock, and no future may be left unresolved.
func TestCleanupAndCompleteConcurrent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for i := 0; i < 100; i++ {
			pub := newTestPublisher(10)
			js := &jetStream{publisher: pub}
			futures := make([]*pubAckFuture, 0, 10)
			for j := 0; j < 10; j++ {
				paf := &pubAckFuture{msg: &nats.Msg{Subject: "foo"}, jsClient: pub}
				if _, err := js.registerPAF(paf, time.Second, nil); err != nil {
					t.Fatalf("registration: %v", err)
				}
				futures = append(futures, paf)
			}

			var wg sync.WaitGroup
			wg.Add(2)
			var complete <-chan struct{}
			go func() {
				defer wg.Done()
				complete = js.PublishAsyncComplete()
			}()
			go func() {
				defer wg.Done()
				js.CleanupPublisher()
			}()
			wg.Wait()
			synctest.Wait()

			select {
			case <-complete:
			default:
				t.Fatal("PublishAsyncComplete was not closed after cleanup")
			}
			for j, paf := range futures {
				select {
				case <-paf.Err():
				default:
					t.Fatalf("future %d was not resolved by cleanup", j)
				}
			}
		}
	})
}

// A publisher is reusable after CleanupPublisher: the next publish is
// accepted and resolves normally.
func TestPublisherReusableAfterCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pub := newTestPublisher(1)
		js := &jetStream{publisher: pub}
		js.CleanupPublisher()

		// newAsyncReply resets closed when it recreates the reply state; do
		// the subscription-less equivalent here since there is no server.
		pub.Lock()
		pub.closed = false
		pub.Unlock()

		paf := &pubAckFuture{msg: &nats.Msg{Subject: "foo"}, jsClient: pub}
		if _, err := js.registerPAF(paf, time.Second, nil); err != nil {
			t.Fatalf("publish after cleanup: %v", err)
		}
	})
}

func TestErrTooManyStalledMsgsStillReported(t *testing.T) {
	if err := ErrTooManyStalledMsgs; err == nil {
		t.Fatal("ErrTooManyStalledMsgs must remain exported")
	}
	_ = fmt.Sprintf("%v", ErrTooManyStalledMsgs)
}
