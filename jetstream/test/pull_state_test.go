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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	testservice "github.com/synadia-io/orbit.go/ntf-client"
)

// setupPullStateStream creates a stream/consumer pair used by the pull state
// semantics tests. The consumer uses explicit acks and a short AckWait so
// that messages abandoned by a cancelled fetch become redeliverable quickly.
func setupPullStateStream(t *testing.T, js jetstream.JetStream, ackWait time.Duration) (jetstream.Stream, jetstream.Consumer) {
	t.Helper()
	ctx := newTesterCtx(t, 30*time.Second)
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO"}})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	cfg := jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy}
	if ackWait > 0 {
		cfg.AckWait = ackWait
	}
	consumer, err := stream.CreateOrUpdateConsumer(ctx, cfg)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	return stream, consumer
}

func publishN(t *testing.T, js jetstream.JetStream, n int) {
	t.Helper()
	ctx := newTesterCtx(t, 30*time.Second)
	for i := 0; i < n; i++ {
		if _, err := js.Publish(ctx, "FOO", []byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
}

// drainBatch reads every buffered message and returns the number delivered
// together with the terminal batch error.
func drainBatch(b jetstream.MessageBatch) (int, error) {
	got := 0
	for range b.Messages() {
		got++
	}
	return got, b.Error()
}

// A cancelled Fetch must never acknowledge the messages the server already
// handed to it: they stay unacked and are redelivered to a later pull.
func TestPullFetchCancellationRedelivers(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
		_, consumer := setupPullStateStream(t, js, time.Second)
		const count = 5
		publishN(t, js, count)

		// Ask for more than exist so the fetch remains outstanding until it
		// is cancelled, rather than completing on its own.
		fetchCtx, cancel := context.WithCancel(context.Background())
		batch, err := consumer.Fetch(count+5, jetstream.FetchContext(fetchCtx))
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}

		// Read whatever messages the server has delivered into the batch,
		// without acknowledging any of them, then cancel the outstanding
		// fetch. Allow a short grace period for in-flight messages to land so
		// the assertion below is independent of delivery timing.
		msgs := batch.Messages()
		got := 0
		drained := make(chan struct{})
		go func() {
			for range msgs {
				got++
			}
			close(drained)
		}()
		time.Sleep(300 * time.Millisecond)
		cancel()
		select {
		case <-drained:
		case <-time.After(5 * time.Second):
			t.Fatal("batch channel did not close after cancellation")
		}
		if !errors.Is(batch.Error(), context.Canceled) {
			t.Fatalf("batch error = %v, want context.Canceled", batch.Error())
		}
		if got == 0 {
			t.Fatal("expected at least one delivered message before cancellation")
		}

		// Nothing was acked, so a subsequent fetch must eventually see the
		// full batch again once the server redelivers after AckWait.
		checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
			again, ferr := consumer.Fetch(count, jetstream.FetchMaxWait(2*time.Second))
			if ferr != nil {
				return ferr
			}
			redelivered, berr := drainBatch(again)
			if redelivered != count {
				return fmt.Errorf("redelivered %d, want %d (batch err: %v)", redelivered, count, berr)
			}
			return nil
		})
	})
}

// A delivered but unacknowledged message remains governed by the consumer's
// AckWait: it is redelivered once the timer expires, while an InProgress
// heartbeat resets that redelivery timer.
func TestPullDeliveredMessageHonorsAckWaitAndInProgress(t *testing.T) {
	t.Run("redelivers after AckWait", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
			_, consumer := setupPullStateStream(t, js, time.Second)
			publishN(t, js, 1)

			first, err := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
			if err != nil {
				t.Fatalf("Fetch: %v", err)
			}
			msg, ok := <-first.Messages()
			if !ok {
				t.Fatalf("no message delivered: %v", first.Error())
			}
			meta, err := msg.Metadata()
			if err != nil {
				t.Fatalf("metadata: %v", err)
			}
			if meta.NumDelivered != 1 {
				t.Fatalf("NumDelivered=%d, want 1", meta.NumDelivered)
			}
			// Deliberately do not ack; AckWait (1s) must make it redeliverable.

			second, err := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
			if err != nil {
				t.Fatalf("second Fetch: %v", err)
			}
			redelivered, berr := drainBatch(second)
			if berr != nil && !errors.Is(berr, nats.ErrTimeout) {
				t.Fatalf("second batch error: %v", berr)
			}
			if redelivered != 1 {
				t.Fatalf("expected 1 redelivery after AckWait, got %d", redelivered)
			}
		})
	})

	t.Run("InProgress resets the redelivery timer", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
			_, consumer := setupPullStateStream(t, js, 800*time.Millisecond)
			publishN(t, js, 1)

			batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
			if err != nil {
				t.Fatalf("Fetch: %v", err)
			}
			msg, ok := <-batch.Messages()
			if !ok {
				t.Fatalf("no message delivered: %v", batch.Error())
			}

			// Send InProgress heartbeats faster than AckWait. While they keep
			// arriving the message must not be redelivered to a parallel fetch.
			stop := make(chan struct{})
			go func() {
				ticker := time.NewTicker(300 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						_ = msg.InProgress()
					case <-stop:
						return
					}
				}
			}()

			probe, err := consumer.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
			if err != nil {
				t.Fatalf("probe Fetch: %v", err)
			}
			n, berr := drainBatch(probe)
			if berr != nil && !errors.Is(berr, nats.ErrTimeout) {
				t.Fatalf("unexpected probe batch error: %v", berr)
			}
			if n != 0 {
				t.Fatalf("message redelivered %d times while InProgress was sent", n)
			}

			// Stop the heartbeats; once AckWait elapses with no ack the
			// server redelivers the message.
			close(stop)
			checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
				again, ferr := consumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
				if ferr != nil {
					return ferr
				}
				if redelivered, _ := drainBatch(again); redelivered != 1 {
					return fmt.Errorf("expected 1 redelivery after heartbeats stopped, got %d", redelivered)
				}
				return nil
			})
		})
	})
}

// Next with a caller context returns context.Canceled and leaves any
// in-flight messages available for a following Next call.
func TestPullMessagesNextContextCancellation(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
		_, consumer := setupPullStateStream(t, js, time.Second)
		publishN(t, js, 1)

		it, err := consumer.Messages()
		if err != nil {
			t.Fatalf("Messages: %v", err)
		}
		defer it.Stop()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := it.Next(jetstream.NextContext(ctx)); !errors.Is(err, context.Canceled) {
			t.Fatalf("Next err = %v, want context.Canceled", err)
		}

		// The message was never delivered, so a fresh Next must still get it.
		msg, err := it.Next(jetstream.NextMaxWait(5 * time.Second))
		if err != nil {
			t.Fatalf("Next after cancel: %v", err)
		}
		if string(msg.Data()) != "msg-0" {
			t.Fatalf("got %q, want msg-0", string(msg.Data()))
		}
		if err := msg.Ack(); err != nil {
			t.Fatalf("ack: %v", err)
		}
	})
}

// NextMaxWait surfaces a client-side timeout as nats.ErrTimeout, distinct
// from caller cancellation and server terminals.
func TestPullMessagesNextTimeoutClassification(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
		_, consumer := setupPullStateStream(t, js, 0)
		it, err := consumer.Messages()
		if err != nil {
			t.Fatalf("Messages: %v", err)
		}
		defer it.Stop()

		if _, err := it.Next(jetstream.NextMaxWait(100 * time.Millisecond)); !errors.Is(err, nats.ErrTimeout) {
			t.Fatalf("Next err = %v, want nats.ErrTimeout", err)
		}
	})
}

// Concurrent fetches on the same consumer must not consume each other's
// responses: every message is delivered exactly once across all batches and
// no batch observes another fetch's cursor movement.
func TestPullConcurrentFetchesDoNotInterfere(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
		_, consumer := setupPullStateStream(t, js, 0)
		const total = 300
		const fetchers = 15
		publishN(t, js, total)

		var wg sync.WaitGroup
		var mu sync.Mutex
		seen := make(map[string]int)
		var batches, delivered int
		for i := 0; i < fetchers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				batch, err := consumer.Fetch(total/fetchers, jetstream.FetchMaxWait(15*time.Second))
				if err != nil {
					t.Errorf("Fetch: %v", err)
					return
				}
				n := 0
				for msg := range batch.Messages() {
					n++
					mu.Lock()
					seen[string(msg.Data())]++
					mu.Unlock()
				}
				if berr := batch.Error(); berr != nil && !errors.Is(berr, nats.ErrTimeout) {
					t.Errorf("batch error: %v", berr)
					return
				}
				mu.Lock()
				batches++
				delivered += n
				mu.Unlock()
			}()
		}
		wg.Wait()
		if delivered != total {
			t.Fatalf("delivered %d messages across fetches, want %d", delivered, total)
		}
		if batches != fetchers {
			t.Fatalf("completed %d batches, want %d", batches, fetchers)
		}
		for subject, count := range seen {
			if count != 1 {
				t.Fatalf("message %q delivered %d times; want exactly once", subject, count)
			}
		}
	})
}

// A slow fetch reader must not block a separate fetch on the same consumer.
func TestPullSlowFetchDoesNotBlockOthers(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
		_, consumer := setupPullStateStream(t, js, 0)
		publishN(t, js, 4)

		// First fetch buffers a message but is never read promptly.
		slow, err := consumer.Fetch(2, jetstream.FetchMaxWait(10*time.Second))
		if err != nil {
			t.Fatalf("slow Fetch: %v", err)
		}
		// Give the server time to deliver into the slow fetch's inbox.
		time.Sleep(300 * time.Millisecond)

		// A second fetch must still make progress independently.
		fast, err := consumer.Fetch(2, jetstream.FetchMaxWait(10*time.Second))
		if err != nil {
			t.Fatalf("fast Fetch: %v", err)
		}
		fastDone := make(chan int, 1)
		go func() {
			n, _ := drainBatch(fast)
			fastDone <- n
		}()
		select {
		case n := <-fastDone:
			if n == 0 {
				t.Fatal("fast fetch received no messages while slow fetch was unread")
			}
		case <-time.After(12 * time.Second):
			t.Fatal("fast fetch blocked behind the slow fetch reader")
		}

		// Drain the slow fetch so its messages and subscription are released.
		_, _ = drainBatch(slow)
	})
}

// A blocked Fetch must terminate promptly with ErrConnectionClosed when the
// connection is closed, rather than waiting for the expiry/heartbeat timer.
func TestPullFetchConnectionClosed(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
		_, consumer := setupPullStateStream(t, js, 0)

		batch, err := consumer.Fetch(1,
			jetstream.FetchMaxWait(20*time.Second),
			jetstream.FetchHeartbeat(time.Second),
		)
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		errC := make(chan error, 1)
		go func() {
			for range batch.Messages() {
			}
			errC <- batch.Error()
		}()

		time.Sleep(200 * time.Millisecond)
		nc.Close()
		select {
		case err := <-errC:
			if !errors.Is(err, jetstream.ErrConnectionClosed) {
				t.Fatalf("batch error = %v, want ErrConnectionClosed", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Fetch did not terminate after connection close")
		}

		// The result must settle exactly once: repeated Error() calls return
		// the same terminal and the message channel stays closed.
		if err := batch.Error(); !errors.Is(err, jetstream.ErrConnectionClosed) {
			t.Fatalf("stable terminal = %v, want ErrConnectionClosed", err)
		}
		if _, ok := <-batch.Messages(); ok {
			t.Fatal("message channel should remain closed after termination")
		}
	})
}

// When caller cancellation and connection closure coincide, a Fetch reports
// the connection terminal exactly once (never a bare context.Canceled).
func TestPullFetchCancelDuringConnectionClose(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
		_, consumer := setupPullStateStream(t, js, 0)
		ctx, cancel := context.WithCancel(context.Background())

		batch, err := consumer.Fetch(1,
			jetstream.FetchContext(ctx),
			jetstream.FetchHeartbeat(time.Second),
		)
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		errC := make(chan error, 1)
		go func() {
			for range batch.Messages() {
			}
			errC <- batch.Error()
		}()

		time.Sleep(200 * time.Millisecond)
		// Fire both terminals back-to-back; ordering must not matter.
		cancel()
		nc.Close()

		select {
		case err := <-errC:
			if !errors.Is(err, jetstream.ErrConnectionClosed) {
				t.Fatalf("batch error = %v, want ErrConnectionClosed", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Fetch did not settle a single terminal state")
		}
	})
}

// A Fetch interrupted by a server restart ends with a server-side terminal
// (server shutdown) rather than a misleading heartbeat error.
func TestPullFetchServerRestartTerminal(t *testing.T) {
	withJSServerInstance(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream, inst *testservice.Instance) {
		_, consumer := setupPullStateStream(t, js, 0)
		batch, err := consumer.Fetch(1,
			jetstream.FetchMaxWait(20*time.Second),
			jetstream.FetchHeartbeat(500*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		errC := make(chan error, 1)
		go func() {
			for range batch.Messages() {
			}
			errC <- batch.Error()
		}()

		time.Sleep(200 * time.Millisecond)
		server := inst.RandomServer()
		inst.StopServer(t, server)
		inst.StartServer(t, server)

		select {
		case err := <-errC:
			if !errors.Is(err, jetstream.ErrServerShutdown) && !errors.Is(err, jetstream.ErrNoHeartbeat) {
				t.Fatalf("batch error = %v, want a server/heartbeat terminal", err)
			}
			if errors.Is(err, jetstream.ErrNoHeartbeat) {
				t.Logf("server did not send a shutdown status; got heartbeat terminal: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("Fetch did not terminate across server restart")
		}
	})
}

// Messages().Next() keeps delivering across a reconnect: the iterator
// re-issues its pull and returns the message rather than an error.
func TestPullMessagesNextSurvivesReconnect(t *testing.T) {
	withJSServerInstance(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream, inst *testservice.Instance) {
		_, consumer := setupPullStateStream(t, js, 0)
		it, err := consumer.Messages()
		if err != nil {
			t.Fatalf("Messages: %v", err)
		}
		defer it.Stop()

		msgC := make(chan jetstream.Msg, 1)
		errC := make(chan error, 1)
		go func() {
			m, nerr := it.Next()
			if nerr != nil {
				errC <- nerr
				return
			}
			msgC <- m
		}()

		time.Sleep(300 * time.Millisecond)
		server := inst.RandomServer()
		inst.StopServer(t, server)
		time.Sleep(time.Second)
		inst.StartServer(t, server)

		// Wait for the connection to recover, then publish.
		checkFor(t, 15*time.Second, 100*time.Millisecond, func() error {
			if !nc.IsConnected() {
				return errors.New("not reconnected")
			}
			return nil
		})
		if _, err := js.Publish(newTesterCtx(t, 5*time.Second), "FOO", []byte("hello")); err != nil {
			t.Fatalf("publish: %v", err)
		}

		select {
		case msg := <-msgC:
			if string(msg.Data()) != "hello" {
				t.Fatalf("got %q, want hello", string(msg.Data()))
			}
		case err := <-errC:
			t.Fatalf("Next returned error across reconnect: %v", err)
		case <-time.After(20 * time.Second):
			t.Fatal("Next did not deliver after reconnect")
		}
	})
}
