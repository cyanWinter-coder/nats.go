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
	"runtime"
	"sync"
	"sync/atomic"
)

// defaultFairDispatchQuantum is the default maximum number of messages a
// single subscription may deliver during one worker turn before the
// subscription is returned to the tail of the round-robin ring.
var defaultFairDispatchQuantum = 16

// maxFairDispatchWorkers caps the default worker pool so a very large
// GOMAXPROCS does not translate into an equally large pool of idle goroutines
// per connection.
const maxFairDispatchWorkers = 32

// defaultFairDispatchWorkers sizes the worker pool to a small multiple of
// GOMAXPROCS, capped at maxFairDispatchWorkers and never below 2: at least
// two workers are what lets a blocked callback on one subscription leave
// delivery to every other subscription unaffected.
func defaultFairDispatchWorkers() int {
	n := runtime.GOMAXPROCS(0)
	if n < 1 {
		n = 1
	}
	n *= 2
	if n < 2 {
		n = 2
	}
	if n > maxFairDispatchWorkers {
		n = maxFairDispatchWorkers
	}
	return n
}

// fairDispatcher is a per-connection, bounded round-robin delivery scheduler
// backed by a fixed pool of worker goroutines.
//
// Resource ownership:
//   - Every async subscription owns its own bounded pending queue (the
//     subscription's pList plus its pMsgs/pBytes limits). The dispatcher
//     never owns queued messages; workers only borrow the head of a queue.
//   - The dispatcher owns only the scheduling state: the round-robin ring of
//     subscriptions that currently have work, the per-subscription in-flight
//     and closing markers, and the fixed worker pool.
//
// Fairness and isolation:
//   - A subscription is present in the ring only while it has work and has no
//     callback in flight. Workers always pop the ring head, so ready
//     subscriptions advance in round-robin order and no subscription can be
//     starved. At most one callback per subscription is ever in flight, which
//     preserves strict per-subscription FIFO ordering, while callbacks of
//     different subscriptions proceed concurrently on different workers -
//     mirroring the historical per-subscription goroutine model, but bounded.
//   - A subscription whose handler blocks occupies at most one worker. The
//     remaining workers keep delivering every other subscription, and a full
//     pending queue only isolates the offending subscription.
//
// Bounded cleanup:
//   - Closing a subscription removes its interest (so no new delivery can be
//     enqueued) and latches fdClosing. The worker currently serving it (or a
//     later one) drains the remaining queue in deterministic order and runs
//     the closed handler exactly once, after which all markers are cleared.
//
// The dispatcher intentionally owns no timers: workers park on a sync.Cond
// until a subscription signals work or shutdown, so shutdown cannot leave a
// timer behind.
type fairDispatcher struct {
	nc *Conn

	mu     sync.Mutex
	cond   *sync.Cond
	ring   []*Subscription // round-robin ring of work-ready subscriptions
	inRing map[*Subscription]struct{}
	closed bool

	quantum int
	workers int

	// done is closed once every worker goroutine has exited. Close itself does
	// not block on it: a message callback running on a worker may itself call
	// connection Close, and waiting synchronously would self-deadlock. The
	// workers always terminate on their own because shutdown stops new
	// scheduling and every scheduled subscription drains to its one-time
	// terminal cleanup, after which the ring stays empty.
	done chan struct{}

	// wg tracks the worker pool; a dedicated closer waits on it so user-facing
	// shutdown never has to block a worker.
	wg sync.WaitGroup

	// test hooks (nil outside internal_testing builds).
	onTurnStart func(s *Subscription)
	onTurnDone  func(s *Subscription)
}

// newFairDispatcher creates a dispatcher and starts its fixed worker pool.
func newFairDispatcher(nc *Conn, quantum, workers int) *fairDispatcher {
	if quantum <= 0 {
		quantum = defaultFairDispatchQuantum
	}
	if workers <= 0 {
		workers = defaultFairDispatchWorkers()
	}
	d := &fairDispatcher{
		nc:      nc,
		ring:    make([]*Subscription, 0, 16),
		inRing:  make(map[*Subscription]struct{}),
		quantum: quantum,
		workers: workers,
		done:    make(chan struct{}),
	}
	d.cond = sync.NewCond(&d.mu)
	d.wg.Add(workers)
	for range workers {
		go d.worker()
	}
	go func() {
		d.wg.Wait()
		close(d.done)
	}()
	return d
}

// enqueueLocked appends m to the subscription's owned queue and ensures the
// subscription is scheduled. The subscription mutex must be held by the
// caller (processMsg or Barrier); the dispatcher lock is taken internally, so
// the global lock order is always subscription mutex -> dispatcher mutex.
// Workers acquire the two locks in that same order and never hold both in
// reverse, so the two cannot deadlock.
func (d *fairDispatcher) enqueueLocked(s *Subscription, m *Msg) {
	if s.pHead == nil {
		s.pHead = m
		s.pTail = m
	} else {
		s.pTail.next = m
		s.pTail = m
	}
	d.scheduleLocked(s)
}

// scheduleLocked ensures the subscription will get a worker turn. It is used
// for newly produced messages and, via removeSub/connection close, for the
// final turn of a closing subscription. Subscription mutex held by the
// caller.
func (d *fairDispatcher) scheduleLocked(s *Subscription) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if s.terminalDone {
		return
	}
	if s.closed {
		// Latch the terminal intent. It survives every ordinary turn-end and
		// is cleared only by terminalCleanup.
		s.fdClosing = true
	}
	if d.closed {
		// Shutting down: the ring is being drained but no new scheduling is
		// accepted. fdClosing above still ensures an already-running worker
		// finishes its cleanup.
		return
	}
	if s.fdInflight {
		// The worker currently serving this subscription observes the new
		// work or the fdClosing latch on its next iteration.
		return
	}
	if _, ok := d.inRing[s]; ok {
		return
	}
	d.ring = append(d.ring, s)
	d.inRing[s] = struct{}{}
	d.cond.Signal()
}

// shutdown stops accepting new work and wakes every worker. It does not block:
// a message callback running on a worker may itself trigger connection close,
// in which case waiting for the worker pool would self-deadlock. The pool
// always terminates on its own - no new scheduling is accepted and every
// already-scheduled subscription drains to its one-time terminal cleanup,
// leaving the ring permanently empty - and completion is observable through
// waitForShutdown. Must be called at most once and without holding the
// connection or any subscription lock.
func (d *fairDispatcher) shutdown() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.closed = true
	d.cond.Broadcast()
	d.mu.Unlock()
}

// waitForShutdown blocks until every worker goroutine has exited. It must not
// be called from a dispatcher worker. It exists for tests and for call sites
// that genuinely need to join the delivery pool; ordinary connection Close
// uses the non-blocking shutdown.
func (d *fairDispatcher) waitForShutdown() {
	<-d.done
}

// worker is the delivery loop run by every pool goroutine. Each iteration
// pops one work-ready subscription from the ring head and serves one turn.
// A blocked handler blocks only this worker; the rest of the pool keeps
// advancing the ring.
func (d *fairDispatcher) worker() {
	defer d.wg.Done()

	for {
		d.mu.Lock()
		for len(d.ring) == 0 {
			if d.closed {
				d.mu.Unlock()
				return
			}
			d.cond.Wait()
		}
		s := d.ring[0]
		d.ring = d.ring[1:]
		delete(d.inRing, s)
		s.fdInflight = true
		d.mu.Unlock()

		d.runTurn(s)
	}
}

// wantsAnotherTurn is the single predicate deciding whether a subscription
// must keep occupying a worker after the current turn: it has pending
// messages, is latched for closing (its queue must drain to terminal
// cleanup), or is already marked closed. Dispatcher lock held.
func (d *fairDispatcher) wantsAnotherTurn(s *Subscription, hasWork, closed bool) bool {
	if s.terminalDone {
		return false
	}
	return hasWork || closed || s.fdClosing
}

// endTurn applies the common end-of-turn transition. It reports whether the
// caller should keep serving the same subscription locally (only possible
// while the dispatcher is shutting down, when the shared ring no longer
// accepts entries): the subscription keeps its in-flight marker and the
// worker drains it to terminal cleanup without another scheduling event.
// Otherwise the subscription is either appended to the ring tail (in-flight
// marker retained) or the worker is released (marker cleared). The
// subscription mutex is held on entry for reading the queue and is released
// here.
func (d *fairDispatcher) endTurn(s *Subscription) bool {
	hasWork := s.pHead != nil
	closed := s.closed
	d.mu.Lock()
	localContinue := false
	if d.wantsAnotherTurn(s, hasWork, closed) {
		if d.closed {
			// Keep draining on this very worker; fdInflight stays set.
			localContinue = true
		} else if _, ok := d.inRing[s]; !ok {
			d.ring = append(d.ring, s)
			d.inRing[s] = struct{}{}
			d.cond.Signal()
		}
	} else {
		s.fdInflight = false
	}
	d.mu.Unlock()
	s.mu.Unlock()
	return localContinue
}

// runTurn serves messages from one subscription's owned queue, in order,
// bounded per turn by the quantum. While the dispatcher is shutting down and
// the subscription still has to drain to its terminal cleanup, turns chain
// locally on the same worker rather than re-entering the shared ring.
func (d *fairDispatcher) runTurn(s *Subscription) {
	if d.onTurnStart != nil {
		d.onTurnStart(s)
	}
	defer func() {
		if d.onTurnDone != nil {
			d.onTurnDone(s)
		}
	}()

	nc := d.nc

	for {
		turn := 0
		for turn < d.quantum {
			s.mu.Lock()
			m := s.pHead
			closed := s.closed
			mcb := s.mcb
			max := s.max
			var fcReply string

			if m == nil {
				if closed {
					s.mu.Unlock()
					d.terminalCleanup(s)
					return
				}
				// Empty, still-open queue. If a close raced in, endTurn keeps
				// the subscription scheduled for its terminal turn; otherwise
				// the worker is released until a future message schedules it.
				if d.endTurn(s) {
					continue
				}
				return
			}

			// Pop the head of the subscription's owned queue.
			s.pHead = m.next
			if s.pHead == nil {
				s.pTail = nil
			}

			// Barriers (connection drain/fence) are accounting markers, not
			// user messages, never count against pending limits and do not
			// consume the quantum.
			if m.barrier != nil {
				s.mu.Unlock()
				if atomic.AddInt64(&m.barrier.refs, -1) == 0 {
					m.barrier.f()
				}
				continue
			}

			msgLen := m.pendingSize()
			var delivered uint64
			if !closed {
				s.delivered++
				delivered = s.delivered
				if s.jsi != nil {
					fcReply = s.checkForFlowControlResponse()
				}
			}
			// Undo the reservation made in processMsg now that the message
			// has reached the delivery point.
			s.pMsgs--
			s.pBytes -= msgLen
			if s.jsi != nil && s.jsi.resetPending {
				s.tryResetOrderedConsumer()
			}
			s.mu.Unlock()

			if fcReply != _EMPTY_ {
				nc.Publish(fcReply, nil)
			}

			if closed {
				// Interest is gone: do not start a new callback. The message
				// was already dequeued in order, so the remaining queue keeps
				// its deterministic order while it drains.
				continue
			}

			// Honor AutoUnsubscribe's max, matching waitForMsgs: a message
			// past max is not delivered.
			if max == 0 || delivered <= max {
				mcb(m)
			}
			turn++

			if max > 0 && delivered >= max {
				nc.mu.Lock()
				nc.removeSub(s)
				nc.mu.Unlock()
				// removeSub latched closing; continue draining/cleanup on the
				// same worker via endTurn.
				s.mu.Lock()
				if d.endTurn(s) {
					break
				}
				return
			}
		}

		// Quantum reached (or a max-induced break): rotate through endTurn.
		s.mu.Lock()
		if !d.endTurn(s) {
			return
		}
		// Shutdown drain: chain another bounded turn locally.
	}
}

// terminalCleanup drains any remaining barrier markers in subscription order
// and runs the closed handler exactly once. It is reached at most once per
// subscription, on a worker goroutine, after the queue has been fully
// processed and no further delivery can be enqueued (the subscription is
// closed and removed from the connection's subscription map).
func (d *fairDispatcher) terminalCleanup(s *Subscription) {
	s.mu.Lock()
	if s.terminalDone {
		s.mu.Unlock()
		return
	}
	for m := s.pHead; m != nil; m = s.pHead {
		if m.barrier != nil {
			s.mu.Unlock()
			if atomic.AddInt64(&m.barrier.refs, -1) == 0 {
				m.barrier.f()
			}
			s.mu.Lock()
		}
		s.pHead = m.next
	}
	s.pTail = nil
	done := s.pDone
	s.terminalDone = true
	s.mu.Unlock()

	d.mu.Lock()
	s.fdInflight = false
	s.fdClosing = false
	d.mu.Unlock()

	if done != nil {
		done(s.Subject)
	}
}
