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
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	testservice "github.com/synadia-io/orbit.go/ntf-client"
)

// orderedRecoverySetup publishes n messages on FOO.A and returns the stream
// handle together with an ordered consumer.
func orderedRecoverySetup(t *testing.T, js jetstream.JetStream, n int) (jetstream.Stream, jetstream.Consumer) {
	t.Helper()
	ctx := newTesterCtx(t, 30*time.Second)
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for i := range n {
		if _, err := js.Publish(ctx, "FOO.A", []byte(fmt.Sprintf("msg-%d", i+1))); err != nil {
			t.Fatalf("Unexpected error during publish: %s", err)
		}
	}
	c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	return s, c
}

// assertContinuousStreamSeqs fails the test if the delivered stream sequences
// are not exactly want (in order) or contain a duplicate.
func assertContinuousStreamSeqs(t *testing.T, msgs []jetstream.Msg, want []uint64) {
	t.Helper()
	if len(msgs) != len(want) {
		t.Fatalf("Unexpected message count; want %d; got %d", len(want), len(msgs))
	}
	seen := make(map[uint64]struct{}, len(msgs))
	for i, msg := range msgs {
		meta, err := msg.Metadata()
		if err != nil {
			t.Fatalf("Unexpected metadata error: %v", err)
		}
		if meta.Sequence.Stream != want[i] {
			t.Fatalf("Message %d: expected stream sequence %d; got %d", i, want[i], meta.Sequence.Stream)
		}
		if _, dup := seen[meta.Sequence.Stream]; dup {
			t.Fatalf("Stream sequence %d delivered twice", meta.Sequence.Stream)
		}
		seen[meta.Sequence.Stream] = struct{}{}
	}
}

// TestOrderedConsumerRecoverySequenceGap verifies that a gap in the delivery
// sequence rebuilds the consumer and resumes at the expected next stream
// sequence, without redelivering an already handed-out message or skipping
// messages after the gap.
func TestOrderedConsumerRecoverySequenceGap(t *testing.T) {
	// Stealing a delivery before the ordered consumer subscribes makes its
	// first delivered message arrive at delivery sequence 2 while its cursor
	// expects 1: a deterministic delivery-sequence gap that the mismatch
	// branch must recover from. This mirrors the existing
	// fetchOneFromOrderedConsumer harness.
	t.Run("messages, delivery gap before first subscribe", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, _ *nats.Conn, js jetstream.JetStream) {
			_, c := orderedRecoverySetup(t, js, 10)
			fetchOneFromOrderedConsumer(t, js, "foo", c.CachedInfo().Name)

			it, err := c.Messages()
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer it.Stop()

			got := make([]jetstream.Msg, 0, 10)
			deadline := time.Now().Add(15 * time.Second)
			for len(got) < 10 {
				msg, err := it.Next(jetstream.NextMaxWait(time.Until(deadline)))
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				got = append(got, msg)
			}
			// All ten stream messages are delivered exactly once, in order.
			want := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
			assertContinuousStreamSeqs(t, got, want)
		})
	})

	t.Run("consume, delivery gap before first subscribe", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, _ *nats.Conn, js jetstream.JetStream) {
			_, c := orderedRecoverySetup(t, js, 10)
			fetchOneFromOrderedConsumer(t, js, "foo", c.CachedInfo().Name)

			var mu sync.Mutex
			got := make([]uint64, 0, 10)
			cc, err := c.Consume(func(msg jetstream.Msg) {
				meta, err := msg.Metadata()
				if err != nil {
					t.Errorf("Unexpected metadata error: %v", err)
					return
				}
				mu.Lock()
				got = append(got, meta.Sequence.Stream)
				mu.Unlock()
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer cc.Stop()

			deadline := time.Now().Add(15 * time.Second)
			for time.Now().Before(deadline) {
				mu.Lock()
				done := len(got) == 10
				mu.Unlock()
				if done {
					break
				}
				time.Sleep(15 * time.Millisecond)
			}
			mu.Lock()
			final := append([]uint64(nil), got...)
			mu.Unlock()
			want := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
			if len(final) != len(want) {
				t.Fatalf("Unexpected message count; want %d; got %d (%v)", len(want), len(final), final)
			}
			for i := range want {
				if final[i] != want[i] {
					t.Fatalf("Position %d: want stream sequence %d; got %d (full: %v)", i, want[i], final[i], final)
				}
			}
		})
	})

	// A message deleted from the stream creates a hole in the stream sequence.
	// The ordered consumer must transparently skip the hole and deliver every
	// surviving message exactly once. The deletion happens before the ordered
	// consumer is created, so no message can have been buffered client-side.
	t.Run("messages, stream sequence hole skipped on initial consumer", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, _ *nats.Conn, js jetstream.JetStream) {
			ctx := newTesterCtx(t, 30*time.Second)
			s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			for i := range 6 {
				if _, err := js.Publish(ctx, "FOO.A", []byte(fmt.Sprintf("m%d", i+1))); err != nil {
					t.Fatalf("Unexpected error during publish: %s", err)
				}
			}
			// Remove stream sequence 4 before any consumer exists.
			if err := s.DeleteMsg(ctx, 4); err != nil {
				t.Fatalf("Unexpected error deleting message: %v", err)
			}
			c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			it, err := c.Messages(jetstream.PullMaxMessages(1))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer it.Stop()

			seen := collectSequences(t, it, map[uint64]struct{}{1: {}, 2: {}, 3: {}, 5: {}, 6: {}})
			if _, bad := seen[4]; bad {
				t.Fatalf("Deleted stream message 4 was delivered")
			}
			for _, want := range []uint64{1, 2, 3, 5, 6} {
				if _, ok := seen[want]; !ok {
					t.Fatalf("Expected surviving stream sequence %d to be delivered; got %v", want, mapKeys(seen))
				}
			}
		})
	})

	// A rebuild encountered after some messages were already handed out must
	// resume from the next expected stream sequence: no redelivery of the
	// messages already consumed and nothing after the rebuild skipped.
	t.Run("messages, consumer deleted mid-stream resumes without duplicates", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, _ *nats.Conn, js jetstream.JetStream) {
			ctx := newTesterCtx(t, 30*time.Second)
			s, c := orderedRecoverySetup(t, js, 8)
			it, err := c.Messages(jetstream.PullHeartbeat(time.Second))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer it.Stop()

			first := make([]jetstream.Msg, 0, 3)
			for range 3 {
				msg, err := it.Next(jetstream.NextMaxWait(5 * time.Second))
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				first = append(first, msg)
			}
			assertContinuousStreamSeqs(t, first, []uint64{1, 2, 3})
			firstName := c.CachedInfo().Name

			// Force a rebuild after three messages were handed out. Buffered
			// deliveries may continue for a while, and Next may surface
			// transient errors during the rebuild.
			if err := s.DeleteConsumer(ctx, firstName); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			want := map[uint64]struct{}{4: {}, 5: {}, 6: {}, 7: {}, 8: {}}
			seen := collectSequences(t, it, want)

			for seq := range seen {
				if seq <= 3 {
					t.Fatalf("Already-delivered sequence %d redelivered after rebuild (full: %v)", seq, mapKeys(seen))
				}
			}
			for w := range want {
				if _, ok := seen[w]; !ok {
					t.Fatalf("Expected sequence %d after rebuild; got %v", w, mapKeys(seen))
				}
			}
		})
	})
}

// collectSequences drives the iterator until every sequence in want has been
// observed, tolerating transient Next errors while a rebuild is in flight. It
// fails the test on a closed iterator or an overall deadline, and returns the
// set of observed stream sequences.
func collectSequences(t *testing.T, it jetstream.MessagesContext, want map[uint64]struct{}) map[uint64]struct{} {
	t.Helper()
	seen := make(map[uint64]struct{}, len(want))
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for w := range want {
			if _, ok := seen[w]; !ok {
				all = false
				break
			}
		}
		if all {
			return seen
		}
		msg, err := it.Next(jetstream.NextMaxWait(2 * time.Second))
		if err != nil {
			if errors.Is(err, jetstream.ErrMsgIteratorClosed) {
				t.Fatalf("Iterator closed before all sequences collected; have %v want %v", mapKeys(seen), mapKeys(want))
			}
			// Transient error (timeout / no heartbeat) during recovery: keep
			// driving Next until the rebuilt consumer delivers.
			continue
		}
		meta, err := msg.Metadata()
		if err != nil {
			t.Fatalf("Unexpected metadata error: %v", err)
		}
		seq := meta.Sequence.Stream
		if _, dup := seen[seq]; dup {
			t.Fatalf("Stream sequence %d delivered twice", seq)
		}
		seen[seq] = struct{}{}
	}
	t.Fatalf("Timed out collecting sequences; have %v want %v", mapKeys(seen), mapKeys(want))
	return seen
}

func mapKeys(m map[uint64]struct{}) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// TestOrderedConsumerRecoveryCoalescedTriggers fires many recovery triggers
// concurrently (the heartbeat-loss and reconnect/consumer-deleted failure
// paths at once) and asserts that consumption stays correct: every message is
// delivered exactly once in order, even though duplicate failures would have
// been capable of starting duplicate rebuilds.
func TestOrderedConsumerRecoveryCoalescedTriggers(t *testing.T) {
	withJSServer(t, func(t *testing.T, _ *nats.Conn, js jetstream.JetStream) {
		ctx := newTesterCtx(t, 30*time.Second)
		s, c := orderedRecoverySetup(t, js, 5)

		var mu sync.Mutex
		got := make([]uint64, 0, 25)
		cc, err := c.Consume(func(msg jetstream.Msg) {
			meta, err := msg.Metadata()
			if err != nil {
				t.Errorf("Unexpected metadata error: %v", err)
				return
			}
			mu.Lock()
			got = append(got, meta.Sequence.Stream)
			mu.Unlock()
		}, jetstream.PullHeartbeat(time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer cc.Stop()

		// Hammer the consumer with concurrent deletes and publishes: each
		// delete makes the in-flight pull request fail (consumer deleted / no
		// responders), while each publish produces more deliveries. All of
		// these failures land concurrently in the error handler.
		var wg sync.WaitGroup
		for range 10 {
			wg.Add(2)
			go func() {
				defer wg.Done()
				_ = s.DeleteConsumer(ctx, c.CachedInfo().Name)
			}()
			go func(i int) {
				defer wg.Done()
				if _, err := js.Publish(ctx, "FOO.A", []byte(fmt.Sprintf("burst-%d", i))); err != nil {
					return
				}
			}(0)
		}
		wg.Wait()

		// Wait for all 15 stream messages (5 initial + 10 burst) to be
		// delivered exactly once and in stream order.
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			done := len(got) == 15
			mu.Unlock()
			if done {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		mu.Lock()
		final := append([]uint64(nil), got...)
		mu.Unlock()

		if len(final) != 15 {
			t.Fatalf("Unexpected message count; want 15; got %d (%v)", len(final), final)
		}
		seen := make(map[uint64]struct{}, len(final))
		for i, seq := range final {
			if seq != uint64(i+1) {
				t.Fatalf("Position %d: want stream sequence %d; got %d (full: %v)", i, i+1, seq, final)
			}
			if _, dup := seen[seq]; dup {
				t.Fatalf("Stream sequence %d delivered twice (full: %v)", seq, final)
			}
			seen[seq] = struct{}{}
		}
	})
}

// TestOrderedConsumerRecoveryServerRestart restarts the backing server and
// checks that in-flight ordered consumption resumes without duplicates or
// skipped messages across the restart.
func TestOrderedConsumerRecoveryServerRestart(t *testing.T) {
	t.Run("messages resumes across restart", func(t *testing.T) {
		withJSServerInstance(t, func(t *testing.T, _ *nats.Conn, js jetstream.JetStream, inst *testservice.Instance) {
			_, c := orderedRecoverySetup(t, js, 5)
			it, err := c.Messages(jetstream.PullHeartbeat(time.Second))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer it.Stop()

			before := make([]jetstream.Msg, 0, 5)
			for range 5 {
				msg, err := it.Next(jetstream.NextMaxWait(10 * time.Second))
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				before = append(before, msg)
			}
			assertContinuousStreamSeqs(t, before, []uint64{1, 2, 3, 4, 5})

			inst.StopServer(t, inst.Servers[0])
			inst.StartServer(t, inst.Servers[0])
			waitForStream(t, js, "foo")

			ctx := newTesterCtx(t, 20*time.Second)
			for i := range 5 {
				if _, err := js.Publish(ctx, "FOO.A", []byte(fmt.Sprintf("after-%d", i))); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			after := make([]jetstream.Msg, 0, 5)
			deadline := time.Now().Add(20 * time.Second)
			for len(after) < 5 {
				msg, err := it.Next(jetstream.NextMaxWait(time.Until(deadline)))
				if err != nil {
					t.Fatalf("Unexpected error after restart: %v", err)
				}
				after = append(after, msg)
			}
			assertContinuousStreamSeqs(t, after, []uint64{6, 7, 8, 9, 10})
		})
	})

	t.Run("consume resumes across restart", func(t *testing.T) {
		withJSServerInstance(t, func(t *testing.T, _ *nats.Conn, js jetstream.JetStream, inst *testservice.Instance) {
			_, c := orderedRecoverySetup(t, js, 5)
			var mu sync.Mutex
			got := make([]uint64, 0, 10)
			cc, err := c.Consume(func(msg jetstream.Msg) {
				meta, err := msg.Metadata()
				if err != nil {
					t.Errorf("Unexpected metadata error: %v", err)
					return
				}
				mu.Lock()
				got = append(got, meta.Sequence.Stream)
				mu.Unlock()
			}, jetstream.PullHeartbeat(time.Second))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer cc.Stop()

			waitCount := func(n int) bool {
				deadline := time.Now().Add(15 * time.Second)
				for time.Now().Before(deadline) {
					mu.Lock()
					cur := len(got)
					mu.Unlock()
					if cur >= n {
						return true
					}
					time.Sleep(15 * time.Millisecond)
				}
				return false
			}
			if !waitCount(5) {
				t.Fatalf("Expected 5 messages before restart; got %v", got)
			}

			inst.StopServer(t, inst.Servers[0])
			inst.StartServer(t, inst.Servers[0])
			waitForStream(t, js, "foo")

			ctx := newTesterCtx(t, 20*time.Second)
			for i := range 5 {
				if _, err := js.Publish(ctx, "FOO.A", []byte(fmt.Sprintf("after-%d", i))); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			if !waitCount(10) {
				mu.Lock()
				t.Fatalf("Expected 10 messages after restart; got %v", got)
			}
			mu.Lock()
			final := append([]uint64(nil), got...)
			mu.Unlock()
			want := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
			for i := range want {
				if final[i] != want[i] {
					t.Fatalf("Position %d: want %d; got %d (full: %v)", i, want[i], final[i], final)
				}
			}
		})
	})
}

// TestOrderedConsumerRecoveryContextCancel verifies that canceling the
// context passed to OrderedConsumer stops background recovery and that no
// rebuild keeps running after cancellation.
func TestOrderedConsumerRecoveryContextCancel(t *testing.T) {
	t.Run("cancel stops consume recovery", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
			ctx, cancel := context.WithCancel(context.Background())
			s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			for range 3 {
				if _, err := js.Publish(ctx, "FOO.A", []byte("m")); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			cc, err := c.Consume(func(msg jetstream.Msg) {})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Trigger a recovery, then cancel and make sure the loop terminates
			// and its Closed channel fires.
			name := c.CachedInfo().Name
			_ = s.DeleteConsumer(ctx, name)
			time.Sleep(200 * time.Millisecond)
			cancel()

			select {
			case <-cc.Closed():
			case <-time.After(5 * time.Second):
				t.Fatalf("Timeout waiting for consume to close after context cancel")
			}

			// A rebuild already in flight at cancel time is allowed to finish
			// (it cannot be unwound atomically), but no NEW rebuild may start
			// after cancellation. Record the name once Closed has fired and
			// verify it stays stable across a subsequent delete + publish.
			stableName := ""
			if info := c.CachedInfo(); info != nil {
				stableName = info.Name
			}
			if stableName != "" {
				_ = s.DeleteConsumer(context.Background(), stableName)
			}
			if _, err := js.Publish(context.Background(), "FOO.A", []byte("late")); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			time.Sleep(400 * time.Millisecond)
			if info := c.CachedInfo(); info != nil && stableName != "" && info.Name != stableName {
				t.Fatalf("New consumer rebuild started after cancellation: %s -> %s", stableName, info.Name)
			}
			_ = nc
		})
	})

	t.Run("cancel during messages recovery unblocks next", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, _ *nats.Conn, js jetstream.JetStream) {
			ctx, cancel := context.WithCancel(context.Background())
			s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if _, err := js.Publish(ctx, "FOO.A", []byte("m")); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			it, err := c.Messages()
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msg, err := it.Next(jetstream.NextMaxWait(5 * time.Second))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			meta, _ := msg.Metadata()
			if meta.Sequence.Stream != 1 {
				t.Fatalf("Unexpected stream sequence: %d", meta.Sequence.Stream)
			}

			// Break the consumer and cancel while the recovery backoff would
			// otherwise keep retrying.
			if err := s.DeleteConsumer(ctx, c.CachedInfo().Name); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			cancel()

			done := make(chan error, 1)
			go func() {
				_, err := it.Next(jetstream.NextMaxWait(10 * time.Second))
				done <- err
			}()
			select {
			case err := <-done:
				if !errors.Is(err, jetstream.ErrMsgIteratorClosed) {
					t.Fatalf("Expected ErrMsgIteratorClosed after cancel; got: %v", err)
				}
			case <-time.After(8 * time.Second):
				t.Fatalf("Next did not unblock after context cancel")
			}
		})
	})
}
