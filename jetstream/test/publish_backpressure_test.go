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
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	testservice "github.com/synadia-io/orbit.go/ntf-client"
)

// A publisher at its pending cap returns ErrTooManyStalledMsgs instead of
// queueing without bound, and the queue never exceeds the cap.
func TestPublishAsyncPendingCapIsBounded(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, _ jetstream.JetStream) {
		const maxPending = 5
		js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(maxPending))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		ctx := newTesterCtx(t, 5*time.Second)
		// NoAck keeps futures outstanding: nothing ever resolves them.
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}, NoAck: true}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for i := 0; i < maxPending; i++ {
			if _, err := js.PublishAsync("FOO.1", []byte("msg"), jetstream.WithStallWait(time.Second)); err != nil {
				t.Fatalf("publish %d: %v", i, err)
			}
		}
		if pending := js.PublishAsyncPending(); pending != maxPending {
			t.Fatalf("expected %d pending publishes, got %d", maxPending, pending)
		}
		if _, err := js.PublishAsync("FOO.1", []byte("msg"), jetstream.WithStallWait(50*time.Millisecond)); !errors.Is(err, jetstream.ErrTooManyStalledMsgs) {
			t.Fatalf("expected ErrTooManyStalledMsgs, got %v", err)
		}
		if pending := js.PublishAsyncPending(); pending != maxPending {
			t.Fatalf("rejected publish changed the pending count to %d", pending)
		}
	})
}

// A canceled context aborts a call waiting for a pending slot; a publish
// that already fits is unaffected.
func TestPublishAsyncContextCancelsStalledWait(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, _ jetstream.JetStream) {
		js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(1))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		ctx := newTesterCtx(t, 5*time.Second)
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}, NoAck: true}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if _, err := js.PublishAsync("FOO.1", []byte("msg")); err != nil {
			t.Fatalf("first publish: %v", err)
		}

		pubCtx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := js.PublishAsync("FOO.1", []byte("msg"),
				jetstream.WithPublishContext(pubCtx),
				jetstream.WithStallWait(time.Minute),
			)
			done <- err
		}()
		time.Sleep(50 * time.Millisecond)
		cancel()

		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected context.Canceled, got %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("PublishAsync did not abort on context cancellation")
		}
		if pending := js.PublishAsyncPending(); pending != 1 {
			t.Fatalf("expected only the first publish to be pending, got %d", pending)
		}
	})
}

// CleanupPublisher aborts a call waiting for a pending slot and every pending
// future exactly once, and PublishAsyncComplete closes.
func TestPublishAsyncCleanupAbortsWaiterAndPending(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, _ jetstream.JetStream) {
		js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(1))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		ctx := newTesterCtx(t, 5*time.Second)
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}, NoAck: true}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		first, err := js.PublishAsync("FOO.1", []byte("msg"))
		if err != nil {
			t.Fatalf("first publish: %v", err)
		}
		complete := js.PublishAsyncComplete()

		done := make(chan error, 1)
		go func() {
			_, err := js.PublishAsync("FOO.1", []byte("msg"), jetstream.WithStallWait(time.Minute))
			done <- err
		}()
		time.Sleep(50 * time.Millisecond)
		js.CleanupPublisher()

		select {
		case err := <-done:
			if !errors.Is(err, jetstream.ErrJetStreamPublisherClosed) {
				t.Fatalf("expected ErrJetStreamPublisherClosed, got %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("CleanupPublisher did not release the stalled caller")
		}
		select {
		case err := <-first.Err():
			if !errors.Is(err, jetstream.ErrJetStreamPublisherClosed) {
				t.Fatalf("expected ErrJetStreamPublisherClosed, got %v", err)
			}
		case <-first.Ok():
			t.Fatal("expected the pending future to fail, not succeed")
		case <-time.After(5 * time.Second):
			t.Fatal("pending future was not resolved by cleanup")
		}
		select {
		case <-complete:
		case <-time.After(5 * time.Second):
			t.Fatal("PublishAsyncComplete was not closed by cleanup")
		}
		if pending := js.PublishAsyncPending(); pending != 0 {
			t.Fatalf("expected no pending publishes, got %d", pending)
		}
	})
}

// A future is resolved exactly once when a reconnect races in-flight acks:
// each future either succeeds or reports the disconnect, never both.
func TestPublishAsyncReconnectResolvesOnce(t *testing.T) {
	withJSServerInstance(t, func(t *testing.T, _ *nats.Conn, js jetstream.JetStream, inst *testservice.Instance) {
		ctx := newTesterCtx(t, 5*time.Second)
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		const numPublishes = 200
		futures := make([]jetstream.PubAckFuture, 0, numPublishes)
		for i := 0; i < numPublishes; i++ {
			paf, err := js.PublishAsync("FOO.A", []byte("hello"))
			if err != nil {
				t.Fatalf("publish %d: %v", i, err)
			}
			futures = append(futures, paf)
		}
		// Bounce the server while acks are in flight.
		inst.StopServer(t, inst.Servers[0])
		inst.StartServer(t, inst.Servers[0])

		var wg sync.WaitGroup
		errs := make(chan error, numPublishes)
		for i, paf := range futures {
			wg.Add(1)
			go func(i int, paf jetstream.PubAckFuture) {
				defer wg.Done()
				acks, errsCount := 0, 0
				deadline := time.After(10 * time.Second)
				for acks+errsCount == 0 {
					select {
					case <-paf.Ok():
						acks++
					case err := <-paf.Err():
						errsCount++
						if !errors.Is(err, nats.ErrDisconnected) && !errors.Is(err, nats.ErrConnectionClosed) && !errors.Is(err, jetstream.ErrNoStreamResponse) {
							errs <- err
						}
					case <-deadline:
						errs <- errors.New("future never resolved")
						return
					}
				}
				// After one result, a second must not be available: an acks
				// channel delivers at most one value, so a second select must
				// not produce anything.
				select {
				case <-paf.Ok():
					if acks > 0 {
						acks++
					}
				case <-paf.Err():
					errsCount++
				default:
				}
				_ = i
			}(i, paf)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("Unexpected error: %v", err)
		}
		select {
		case <-js.PublishAsyncComplete():
		case <-time.After(10 * time.Second):
			t.Fatal("Did not receive completion signal")
		}
	})
}

// CleanupPublisher racing PublishAsyncComplete must not deadlock or lose an
// unresolved future.
func TestPublishAsyncCleanupCompleteRace(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, _ jetstream.JetStream) {
		js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(10))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		ctx := newTesterCtx(t, 5*time.Second)
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}, NoAck: true}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for i := 0; i < 200; i++ {
			futures := make([]jetstream.PubAckFuture, 0, 5)
			for j := 0; j < 5; j++ {
				paf, err := js.PublishAsync("FOO.A", []byte("hello"))
				if err != nil {
					t.Fatalf("publish: %v", err)
				}
				futures = append(futures, paf)
			}

			var wg sync.WaitGroup
			var complete <-chan struct{}
			wg.Add(2)
			go func() {
				defer wg.Done()
				complete = js.PublishAsyncComplete()
			}()
			go func() {
				defer wg.Done()
				js.CleanupPublisher()
			}()
			wg.Wait()

			select {
			case <-complete:
			case <-time.After(5 * time.Second):
				t.Fatalf("iteration %d: completion signal was lost", i)
			}
			for j, paf := range futures {
				select {
				case <-paf.Ok():
					t.Fatalf("iteration %d future %d: unexpected success after cleanup", i, j)
				case <-paf.Err():
				case <-time.After(5 * time.Second):
					t.Fatalf("iteration %d future %d was not resolved", i, j)
				}
			}
			if pending := js.PublishAsyncPending(); pending != 0 {
				t.Fatalf("iteration %d: expected no pending publishes, got %d", i, pending)
			}
		}
	})
}

// A publish context is only meaningful for async publish.
func TestPublishContextRejectedForSyncPublish(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, _ jetstream.JetStream) {
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		ctx := newTesterCtx(t, 5*time.Second)
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := js.Publish(ctx, "FOO.1", []byte("msg"), jetstream.WithPublishContext(context.Background())); !errors.Is(err, jetstream.ErrInvalidOption) {
			t.Fatalf("expected ErrInvalidOption, got %v", err)
		}
	})
}

// Connection Drain while async publishes are outstanding and another caller
// waits on PublishAsyncComplete must terminate every future and close the
// completion channel without deadlocking.
func TestPublishAsyncDrainWhilePending(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, _ jetstream.JetStream) {
		js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(50))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		ctx := newTesterCtx(t, 5*time.Second)
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}, NoAck: true}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		futures := make([]jetstream.PubAckFuture, 0, 20)
		for i := 0; i < 20; i++ {
			paf, err := js.PublishAsync("FOO.A", []byte("hello"))
			if err != nil {
				t.Fatalf("publish %d: %v", i, err)
			}
			futures = append(futures, paf)
		}

		// Observe completion concurrently with the connection draining.
		complete := make(chan struct{})
		go func() {
			<-js.PublishAsyncComplete()
			close(complete)
		}()

		if err := nc.Drain(); err != nil {
			t.Fatalf("Drain: %v", err)
		}

		select {
		case <-complete:
		case <-time.After(5 * time.Second):
			t.Fatal("PublishAsyncComplete was not closed after Drain")
		}
		for i, paf := range futures {
			select {
			case <-paf.Ok():
				continue
			case <-paf.Err():
				continue
			case <-time.After(5 * time.Second):
				t.Fatalf("future %d was not resolved by Drain", i)
			}
		}
		if pending := js.PublishAsyncPending(); pending != 0 {
			t.Fatalf("expected no pending publishes, got %d", pending)
		}
	})
}
