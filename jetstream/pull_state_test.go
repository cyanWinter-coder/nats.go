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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nats-io/nats.go"
)

func TestPullStateMachineInOrderDelivery(t *testing.T) {
	sm := newPullStateMachine(1)
	if snap := sm.snapshot(); snap.State != PullStateReady || snap.NextSeq != 0 || snap.Generation != 1 {
		t.Fatalf("Unexpected initial snapshot: %+v", snap)
	}
	for i := uint64(1); i <= 3; i++ {
		if state := sm.observeDelivery(i); state != PullStateDelivering {
			t.Fatalf("Expected state %v after in-order delivery, got %v", PullStateDelivering, state)
		}
	}
	if snap := sm.snapshot(); snap.NextSeq != 4 {
		t.Fatalf("Expected next pending sequence 4, got %d", snap.NextSeq)
	}
}

func TestPullStateMachineDuplicateDelivery(t *testing.T) {
	sm := newPullStateMachine(1)
	sm.observeDelivery(1)
	sm.observeDelivery(2)
	if state := sm.observeDelivery(2); state != PullStateDuplicate {
		t.Fatalf("Expected state %v for a duplicate, got %v", PullStateDuplicate, state)
	}
	// a duplicate must not move the pending sequence
	if snap := sm.snapshot(); snap.NextSeq != 3 {
		t.Fatalf("Expected next pending sequence to stay at 3, got %d", snap.NextSeq)
	}
	// and the next in-order delivery resumes normal progress
	if state := sm.observeDelivery(3); state != PullStateDelivering {
		t.Fatalf("Expected state %v after recovery, got %v", PullStateDelivering, state)
	}
}

func TestPullStateMachineOutOfOrderDelivery(t *testing.T) {
	sm := newPullStateMachine(1)
	sm.observeDelivery(1)
	// a gap: sequence 3 arrives before 2
	if state := sm.observeDelivery(3); state != PullStateOutOfOrder {
		t.Fatalf("Expected state %v for a gap, got %v", PullStateOutOfOrder, state)
	}
	// the cursor resynchronizes so the gap is reported exactly once
	if snap := sm.snapshot(); snap.NextSeq != 4 {
		t.Fatalf("Expected next pending sequence resynchronized to 4, got %d", snap.NextSeq)
	}
	if state := sm.observeDelivery(4); state != PullStateDelivering {
		t.Fatalf("Expected state %v after resync, got %v", PullStateDelivering, state)
	}
	// a late arrival of the skipped sequence is a duplicate, not a new gap
	if state := sm.observeDelivery(2); state != PullStateDuplicate {
		t.Fatalf("Expected state %v for the skipped sequence, got %v", PullStateDuplicate, state)
	}
}

func TestPullStateMachineHeartbeatEndsWaitOnly(t *testing.T) {
	sm := newPullStateMachine(1)
	deadline := 100 * time.Millisecond
	sm.armHeartbeat(deadline)
	if sm.heartbeatExpired(time.Now()) {
		t.Fatal("Heartbeat must not expire before the deadline")
	}
	if !sm.heartbeatExpired(time.Now().Add(2 * deadline)) {
		t.Fatal("Heartbeat must expire after the deadline")
	}
	snap := sm.snapshot()
	if snap.State != PullStateHeartbeatMissed {
		t.Fatalf("Expected state %v, got %v", PullStateHeartbeatMissed, snap.State)
	}
	// the miss ends only the current wait: the generation is not closed
	// and keeps accepting deliveries
	if state := sm.observeDelivery(1); state != PullStateDelivering {
		t.Fatalf("Expected generation to keep delivering after a heartbeat miss, got %v", state)
	}
}

func TestPullStateMachineStaleHeartbeatTimer(t *testing.T) {
	sm := newPullStateMachine(1)
	sm.armHeartbeat(100 * time.Millisecond)
	// a delivery re-arms the deadline after the timer already fired
	sm.armHeartbeat(time.Hour)
	if sm.heartbeatExpired(time.Now().Add(200 * time.Millisecond)) {
		t.Fatal("A re-armed deadline must suppress the stale timer expiry")
	}
	if snap := sm.snapshot(); snap.State == PullStateHeartbeatMissed {
		t.Fatal("State must not move to heartbeat-missed on a stale timer")
	}
}

func TestPullStateMachineSingleTerminalState(t *testing.T) {
	sm := newPullStateMachine(1)
	const callers = 16
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if sm.terminate(fmt.Errorf("cause %d", i)) {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := wins.Load(); got != 1 {
		t.Fatalf("Expected exactly one terminal transition, got %d", got)
	}
	snap := sm.snapshot()
	if snap.State != PullStateClosed || snap.TerminalErr == nil {
		t.Fatalf("Unexpected terminal snapshot: %+v", snap)
	}
	// no transition leaves the terminal state
	if state := sm.observeDelivery(7); state != PullStateClosed {
		t.Fatalf("Expected state to stay %v, got %v", PullStateClosed, state)
	}
	sm.transition(PullStateFetching)
	if sm.heartbeatExpired(time.Now().Add(time.Hour)) {
		t.Fatal("A closed generation must not report heartbeat expiry")
	}
	if sm.terminate(nil) {
		t.Fatal("Second terminate must not win")
	}
	if snap := sm.snapshot(); snap.State != PullStateClosed {
		t.Fatalf("Expected state to stay %v, got %v", PullStateClosed, snap.State)
	}
}

func TestPullSubscriptionStatusMsgStates(t *testing.T) {
	newSub := func() *pullSubscription {
		return &pullSubscription{
			consumer:    &pullConsumer{},
			consumeOpts: &consumeOpts{},
			done:        make(chan struct{}, 1),
			sm:          newPullStateMachine(1),
		}
	}

	t.Run("server end", func(t *testing.T) {
		sub := newSub()
		sub.pending.msgCount = 5
		termErr, notifyErr := sub.handleStatusMsg(&nats.Msg{}, nats.ErrTimeout)
		if termErr != nil || notifyErr != nil {
			t.Fatalf("Expected no terminal/notify error, got %v / %v", termErr, notifyErr)
		}
		if snap := sub.PullState(); snap.State != PullStateServerEnded {
			t.Fatalf("Expected state %v, got %v", PullStateServerEnded, snap.State)
		}
	})

	t.Run("transient error", func(t *testing.T) {
		sub := newSub()
		termErr, notifyErr := sub.handleStatusMsg(&nats.Msg{}, ErrConsumerLeadershipChanged)
		if termErr != nil || !errors.Is(notifyErr, ErrConsumerLeadershipChanged) {
			t.Fatalf("Expected notify error %v, got %v / %v", ErrConsumerLeadershipChanged, termErr, notifyErr)
		}
		snap := sub.PullState()
		if snap.State != PullStateTransientError {
			t.Fatalf("Expected state %v, got %v", PullStateTransientError, snap.State)
		}
		if snap.TerminalErr != nil {
			t.Fatalf("Transient error must not record a terminal cause, got %v", snap.TerminalErr)
		}
	})

	t.Run("terminal error", func(t *testing.T) {
		sub := newSub()
		termErr, _ := sub.handleStatusMsg(&nats.Msg{}, ErrConsumerDeleted)
		if !errors.Is(termErr, ErrConsumerDeleted) {
			t.Fatalf("Expected terminal error %v, got %v", ErrConsumerDeleted, termErr)
		}
		// the caller's Stop collapses into the same terminal state
		sub.Stop()
		snap := sub.PullState()
		if snap.State != PullStateClosed {
			t.Fatalf("Expected state %v, got %v", PullStateClosed, snap.State)
		}
		if !errors.Is(snap.TerminalErr, ErrConsumerDeleted) {
			t.Fatalf("Expected terminal cause %v, got %v", ErrConsumerDeleted, snap.TerminalErr)
		}
	})
}

func TestPullSubscriptionStopDrainRaceSingleTerminal(t *testing.T) {
	sub := &pullSubscription{
		done:      make(chan struct{}, 1),
		fetchNext: make(chan *pullRequest, 1),
		sm:        newPullStateMachine(1),
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(2)
		go func() { defer wg.Done(); sub.Stop() }()
		go func() { defer wg.Done(); sub.Drain() }()
	}
	wg.Wait()
	if sub.closed.Load() != 1 {
		t.Fatal("Expected subscription to be closed")
	}
	if snap := sub.PullState(); snap.State != PullStateClosed {
		t.Fatalf("Expected single terminal state %v, got %v", PullStateClosed, snap.State)
	}
	select {
	case <-sub.done:
	default:
		t.Fatal("Expected done channel to be closed exactly once")
	}
}

func TestPullSubscriptionCleanupDiscardsQueuedFetches(t *testing.T) {
	sub := &pullSubscription{
		done:      make(chan struct{}, 1),
		fetchNext: make(chan *pullRequest, 1),
		sm:        newPullStateMachine(1),
	}
	sub.requestNext(&pullRequest{Batch: 10})
	if len(sub.fetchNext) != 1 {
		t.Fatalf("Expected a queued fetch request, got %d", len(sub.fetchNext))
	}
	sub.Stop()
	sub.cleanup()
	if got := len(sub.fetchNext); got != 0 {
		t.Fatalf("Expected queued fetch requests to be discarded, got %d", got)
	}
	if snap := sub.PullState(); snap.State != PullStateClosed {
		t.Fatalf("Expected state %v, got %v", PullStateClosed, snap.State)
	}
}

func TestPullSubscriptionHeartbeatTimeoutEndsCurrentWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sub := &pullSubscription{
			errs: make(chan error, 1),
			done: make(chan struct{}, 1),
			sm:   newPullStateMachine(1),
		}
		monitor := sub.scheduleHeartbeatCheck(100 * time.Millisecond)
		defer monitor.Close()

		// advance past the 2*heartbeat deadline of the first wait
		time.Sleep(250 * time.Millisecond)
		synctest.Wait()
		select {
		case err := <-sub.errs:
			if !errors.Is(err, ErrNoHeartbeat) {
				t.Fatalf("Expected %v, got %v", ErrNoHeartbeat, err)
			}
		default:
			t.Fatal("Expected a missing-heartbeat error")
		}
		if snap := sub.PullState(); snap.State != PullStateHeartbeatMissed {
			t.Fatalf("Expected state %v, got %v", PullStateHeartbeatMissed, snap.State)
		}
		// the timeout ends only the current wait
		if sub.closed.Load() == 1 {
			t.Fatal("Heartbeat timeout must not close the consumer")
		}

		// the next wait re-arms and expires on its own deadline
		rearmHeartbeat(monitor, sub.sm, 200*time.Millisecond)
		if snap := sub.PullState(); snap.HeartbeatDeadline.IsZero() {
			t.Fatal("Expected a re-armed heartbeat deadline")
		}
		time.Sleep(250 * time.Millisecond)
		synctest.Wait()
		select {
		case err := <-sub.errs:
			if !errors.Is(err, ErrNoHeartbeat) {
				t.Fatalf("Expected %v, got %v", ErrNoHeartbeat, err)
			}
		default:
			t.Fatal("Expected a missing-heartbeat error for the second wait")
		}
	})
}

func TestPullSubscriptionStaleHeartbeatTimerSuppressed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sub := &pullSubscription{
			errs: make(chan error, 1),
			done: make(chan struct{}, 1),
			sm:   newPullStateMachine(1),
		}
		monitor := sub.scheduleHeartbeatCheck(100 * time.Millisecond)
		defer monitor.Close()

		// a delivery arrives mid-wait and re-arms the deadline; the timer
		// of the first wait must not report a miss when it fires
		time.Sleep(150 * time.Millisecond)
		rearmHeartbeat(monitor, sub.sm, time.Hour)
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		select {
		case err := <-sub.errs:
			t.Fatalf("Stale timer reported a heartbeat miss: %v", err)
		default:
		}
		if snap := sub.PullState(); snap.State == PullStateHeartbeatMissed {
			t.Fatal("State must not move to heartbeat-missed on a stale timer")
		}
	})
}

func TestPullSubscriptionObserveDeliverySeq(t *testing.T) {
	sub := &pullSubscription{sm: newPullStateMachine(1)}
	msg := &nats.Msg{Reply: "$JS.ACK.TEST.cons.1.5.3.1689774636057300142.1"}
	sub.observeDeliverySeq(msg)
	if snap := sub.PullState(); snap.State != PullStateDelivering || snap.NextSeq != 4 {
		t.Fatalf("Unexpected snapshot after first delivery: %+v", snap)
	}
	// messages without deliverable metadata are ignored
	sub.observeDeliverySeq(&nats.Msg{})
	if snap := sub.PullState(); snap.NextSeq != 4 {
		t.Fatalf("Metadata-less message must not move the cursor: %+v", snap)
	}
	// out-of-order metadata moves the generation to the gap state
	gap := &nats.Msg{Reply: "$JS.ACK.TEST.cons.1.5.9.1689774636057300142.1"}
	sub.observeDeliverySeq(gap)
	if snap := sub.PullState(); snap.State != PullStateOutOfOrder || snap.NextSeq != 10 {
		t.Fatalf("Unexpected snapshot after gap: %+v", snap)
	}
}

func TestPullSubscriptionPullStateReporter(t *testing.T) {
	sub := &pullSubscription{sm: newPullStateMachine(42)}
	var reporter PullStateReporter = sub
	snap := reporter.PullState()
	if snap.Generation != 42 || snap.State != PullStateReady {
		t.Fatalf("Unexpected snapshot: %+v", snap)
	}
}
