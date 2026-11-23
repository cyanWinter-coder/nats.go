// Copyright 2023 The NATS Authors
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

//go:build internal_testing

// Functions in this file are only available when building nats.go with the
// internal_testing build tag. They are used by the nats.go test suite.
package nats

// AddMsgFilter adds a message filter for the given subject
// to the connection. The filter will be called for each
// message received on the subject. If the filter returns
// nil, the message will be dropped.
func (nc *Conn) AddMsgFilter(subject string, filter msgFilter) {
	nc.subsMu.Lock()
	defer nc.subsMu.Unlock()

	if nc.filters == nil {
		nc.filters = make(map[string]msgFilter)
	}
	nc.filters[subject] = filter
}

// RemoveMsgFilter removes a message filter for the given subject.
func (nc *Conn) RemoveMsgFilter(subject string) {
	nc.subsMu.Lock()
	defer nc.subsMu.Unlock()

	if nc.filters != nil {
		delete(nc.filters, subject)
		if len(nc.filters) == 0 {
			nc.filters = nil
		}
	}
}

// IsJSControlMessage returns true if the message is a JetStream control message.
func IsJSControlMessage(msg *Msg) (bool, int) {
	return isJSControlMessage(msg)
}

// CloseTCPConn closes the underlying TCP connection.
// It can be used to simulate a disconnect.
func (nc *Conn) CloseTCPConn() {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	nc.conn.Close()
}

// FairDispatchEnabled reports whether the connection was created with the
// FairDispatch option and therefore delivers async subscriptions through the
// per-connection bounded round-robin dispatcher.
func (nc *Conn) FairDispatchEnabled() bool {
	return nc.fd != nil
}

// WaitForFairDispatchShutdown blocks until the fair dispatcher's worker pool
// has fully exited after the connection was closed. It is intended for tests
// that need to assert on work completed by the dispatcher; ordinary code
// should rely on the closed handler instead. It must not be called from a
// message handler. It is a no-op without fair dispatch.
func (nc *Conn) WaitForFairDispatchShutdown() {
	if nc.fd == nil {
		return
	}
	nc.fd.waitForShutdown()
}

// SetFairDispatchTurnHooks installs callbacks invoked at the start and end of
// every fair dispatcher turn for the given subscription. Either hook may be
// nil. They run on the dispatcher goroutine and must not block indefinitely.
// It is a no-op on connections without fair dispatch.
func (nc *Conn) SetFairDispatchTurnHooks(onStart, onDone func(*Subscription)) {
	if nc.fd == nil {
		return
	}
	nc.fd.mu.Lock()
	defer nc.fd.mu.Unlock()
	nc.fd.onTurnStart = onStart
	nc.fd.onTurnDone = onDone
}

// FairDispatchRingSnapshot returns the subjects of the subscriptions currently
// scheduled in the fair dispatcher's round-robin ring, in scheduling order.
// It is intended for deterministic assertions in tests.
func (nc *Conn) FairDispatchRingSnapshot() []string {
	if nc.fd == nil {
		return nil
	}
	nc.fd.mu.Lock()
	defer nc.fd.mu.Unlock()
	out := make([]string, 0, len(nc.fd.ring))
	for _, s := range nc.fd.ring {
		out = append(out, s.Subject)
	}
	return out
}
