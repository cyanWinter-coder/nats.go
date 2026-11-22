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
	"sync"
	"time"
)

// PullSubscriptionState identifies the observable state of a single pull
// subscription generation. A generation spans one Consume/Messages/Fetch
// invocation on a consumer; an ordered consumer starts a new generation on
// every reset.
type PullSubscriptionState uint8

const (
	// PullStateReady is the initial state of a generation, before any pull
	// request has been sent.
	PullStateReady PullSubscriptionState = iota
	// PullStateFetching means a pull request is in flight and the
	// generation is waiting for messages or status replies.
	PullStateFetching
	// PullStateDelivering means in-order messages are flowing.
	PullStateDelivering
	// PullStateDuplicate means a delivery with an already-seen consumer
	// sequence was observed. The pending sequence is left unchanged, so an
	// in-order delivery moves the generation back to PullStateDelivering.
	PullStateDuplicate
	// PullStateOutOfOrder means a delivery skipped at least one consumer
	// sequence. The pending sequence is resynchronized to the observed
	// delivery so the gap is reported exactly once.
	PullStateOutOfOrder
	// PullStateServerEnded means the server ended the current wait
	// (batch completed, max bytes exceeded or request timeout).
	PullStateServerEnded
	// PullStateTransientError means a recoverable error was observed
	// (e.g. leadership change or pin mismatch); the generation continues.
	PullStateTransientError
	// PullStateHeartbeatMissed means the heartbeat deadline passed. Only
	// the current wait is ended; the generation is not closed.
	PullStateHeartbeatMissed
	// PullStateClosed is the single terminal state of a generation,
	// reached by Stop, Drain, a terminal server status or the final
	// message of a StopAfter budget, whichever comes first.
	PullStateClosed
)

// String returns a human-readable name of the state.
func (s PullSubscriptionState) String() string {
	switch s {
	case PullStateReady:
		return "ready"
	case PullStateFetching:
		return "fetching"
	case PullStateDelivering:
		return "delivering"
	case PullStateDuplicate:
		return "duplicate"
	case PullStateOutOfOrder:
		return "out-of-order"
	case PullStateServerEnded:
		return "server-ended"
	case PullStateTransientError:
		return "transient-error"
	case PullStateHeartbeatMissed:
		return "heartbeat-missed"
	case PullStateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// PullSubscriptionStateSnapshot is a point-in-time view of a pull
// subscription generation.
type PullSubscriptionStateSnapshot struct {
	// State is the current observable state of the generation.
	State PullSubscriptionState
	// Generation identifies the subscription generation within its
	// consumer. It is incremented for every new Consume/Messages/Fetch
	// invocation (and on every ordered consumer reset).
	Generation uint64
	// NextSeq is the next consumer delivery sequence the generation
	// expects. Zero means no delivery has been observed yet.
	NextSeq uint64
	// HeartbeatDeadline is the deadline of the current heartbeat wait.
	// Zero when no heartbeat is armed.
	HeartbeatDeadline time.Time
	// TerminalErr is the error that moved the generation to
	// PullStateClosed, if the close was caused by one.
	TerminalErr error
}

// PullStateReporter is implemented by the [ConsumeContext] and
// [MessagesContext] values returned by pull consumers. Use a type
// assertion to access it:
//
//	if sr, ok := cc.(jetstream.PullStateReporter); ok {
//		snap := sr.PullState()
//	}
type PullStateReporter interface {
	// PullState returns a snapshot of the current state of the
	// subscription generation.
	PullState() PullSubscriptionStateSnapshot
}

// pullSubscription generations report their state.
var _ PullStateReporter = (*pullSubscription)(nil)

// pullStateMachine tracks the ordered-consumption state of one pull
// subscription generation: the next pending delivery sequence, the
// current heartbeat deadline and the single terminal transition.
type pullStateMachine struct {
	mu         sync.Mutex
	generation uint64
	state      PullSubscriptionState
	nextSeq    uint64
	hbDeadline time.Time
	termErr    error
}

func newPullStateMachine(generation uint64) *pullStateMachine {
	return &pullStateMachine{generation: generation, state: PullStateReady}
}

// observeDelivery records a delivered consumer sequence and returns the
// resulting state. A sequence below the pending one is a duplicate and
// does not move the cursor; a sequence above it is a gap and
// resynchronizes the cursor so the gap is reported exactly once.
func (sm *pullStateMachine) observeDelivery(seq uint64) PullSubscriptionState {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.state == PullStateClosed {
		return sm.state
	}
	switch {
	case sm.nextSeq == 0:
		// first delivery of the generation establishes the baseline
		sm.nextSeq = seq + 1
		sm.state = PullStateDelivering
	case seq < sm.nextSeq:
		sm.state = PullStateDuplicate
	case seq > sm.nextSeq:
		sm.nextSeq = seq + 1
		sm.state = PullStateOutOfOrder
	default:
		sm.nextSeq = seq + 1
		sm.state = PullStateDelivering
	}
	return sm.state
}

// transition moves the generation to a non-terminal state. It is a no-op
// once the generation is closed.
func (sm *pullStateMachine) transition(state PullSubscriptionState) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.state == PullStateClosed {
		return
	}
	sm.state = state
}

// armHeartbeat records the deadline of the current heartbeat wait.
func (sm *pullStateMachine) armHeartbeat(d time.Duration) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.hbDeadline = time.Now().Add(d)
}

// disarmHeartbeat clears the heartbeat deadline without changing state.
func (sm *pullStateMachine) disarmHeartbeat() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.hbDeadline = time.Time{}
}

// heartbeatExpired reports whether the current heartbeat deadline has
// passed. If so, the generation moves to PullStateHeartbeatMissed,
// ending only the current wait; the generation itself stays open. A
// deadline re-armed after the timer fired but before this check runs
// suppresses the stale expiry.
func (sm *pullStateMachine) heartbeatExpired(now time.Time) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.state == PullStateClosed || sm.hbDeadline.IsZero() || now.Before(sm.hbDeadline) {
		return false
	}
	sm.hbDeadline = time.Time{}
	sm.state = PullStateHeartbeatMissed
	return true
}

// terminate moves the generation to its single terminal state. Only the
// first call performs the transition and reports true; concurrent Stop,
// Drain, terminal status and final-message paths collapse into it.
func (sm *pullStateMachine) terminate(err error) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.state == PullStateClosed {
		return false
	}
	sm.state = PullStateClosed
	sm.termErr = err
	sm.hbDeadline = time.Time{}
	return true
}

func (sm *pullStateMachine) snapshot() PullSubscriptionStateSnapshot {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return PullSubscriptionStateSnapshot{
		State:             sm.state,
		Generation:        sm.generation,
		NextSeq:           sm.nextSeq,
		HeartbeatDeadline: sm.hbDeadline,
		TerminalErr:       sm.termErr,
	}
}
