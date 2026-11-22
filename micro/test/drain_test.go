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

package micro_test

import (
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// waitForNoResponders polls subject until requests fail with ErrNoResponders,
// i.e. until every service on subject has removed its interest.
func waitForNoResponders(t *testing.T, nc *nats.Conn, subject string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, err := nc.Request(subject, nil, 100*time.Millisecond)
		if errors.Is(err, nats.ErrNoResponders) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("service on %q still accepts requests", subject)
}

// TestServiceStopDrainsInFlightRequests verifies the drain commit boundary:
// once Stop starts, no new requests are accepted while the request accepted
// before Stop is still allowed to finish and gets its response delivered.
func TestServiceStopDrainsInFlightRequests(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		entered := make(chan struct{})
		release := make(chan struct{})
		stopped := make(chan struct{})
		var doneCount atomic.Int32

		srv, err := micro.AddService(nc, micro.Config{
			Name:    "drain_inflight",
			Version: "0.1.0",
			DoneHandler: func(micro.Service) {
				doneCount.Add(1)
			},
			Endpoint: &micro.EndpointConfig{
				Subject: "drain.work",
				Handler: micro.HandlerFunc(func(req micro.Request) {
					entered <- struct{}{}
					<-release
					if err := req.Respond([]byte("done")); err != nil {
						t.Errorf("Respond: %v", err)
					}
				}),
			},
		})
		if err != nil {
			t.Fatalf("AddService: %v", err)
		}

		inbox := nats.NewInbox()
		replies, err := nc.SubscribeSync(inbox)
		if err != nil {
			t.Fatalf("SubscribeSync: %v", err)
		}
		if err := nc.PublishRequest("drain.work", inbox, nil); err != nil {
			t.Fatalf("PublishRequest: %v", err)
		}
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("handler did not start")
		}

		go func() {
			if err := srv.Stop(); err != nil {
				t.Errorf("Stop: %v", err)
			}
			close(stopped)
		}()

		// Interest must be removed even though the accepted request is still
		// running: no new request can be admitted to the draining service.
		waitForNoResponders(t, nc, "drain.work")
		if srv.Stopped() {
			t.Fatal("service must not be fully stopped while a request is in flight")
		}

		close(release)
		select {
		case <-stopped:
		case <-time.After(2 * time.Second):
			t.Fatal("Stop did not return after the in-flight request finished")
		}

		resp, err := replies.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("expected the accepted request's response: %v", err)
		}
		if string(resp.Data) != "done" {
			t.Fatalf("unexpected response: %q", resp.Data)
		}
		if _, err := replies.NextMsg(100 * time.Millisecond); err == nil {
			t.Fatal("accepted request produced more than one response")
		}
		if got := doneCount.Load(); got != 1 {
			t.Fatalf("DoneHandler count: want 1; got %d", got)
		}
	})
}

// TestServiceStopDrainTimeout verifies the bounded drain: a stuck request
// makes Stop return ErrServiceDrainTimeout without committing the stop, and
// a later Stop completes once the request converges.
func TestServiceStopDrainTimeout(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		entered := make(chan struct{})
		release := make(chan struct{})
		done := make(chan struct{}, 1)
		var doneCount atomic.Int32

		srv, err := micro.AddService(nc, micro.Config{
			Name:         "drain_timeout",
			Version:      "0.1.0",
			DrainTimeout: 150 * time.Millisecond,
			DoneHandler: func(micro.Service) {
				doneCount.Add(1)
				done <- struct{}{}
			},
			Endpoint: &micro.EndpointConfig{
				Subject: "drain.timeout",
				Handler: micro.HandlerFunc(func(req micro.Request) {
					entered <- struct{}{}
					<-release
					if err := req.Respond([]byte("late")); err != nil {
						t.Errorf("Respond: %v", err)
					}
				}),
			},
		})
		if err != nil {
			t.Fatalf("AddService: %v", err)
		}

		if err := nc.PublishRequest("drain.timeout", nats.NewInbox(), nil); err != nil {
			t.Fatalf("PublishRequest: %v", err)
		}
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("handler did not start")
		}

		start := time.Now()
		err = srv.Stop()
		if !errors.Is(err, micro.ErrServiceDrainTimeout) {
			t.Fatalf("Stop: want %v; got %v", micro.ErrServiceDrainTimeout, err)
		}
		if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
			t.Fatalf("Stop returned before the drain deadline elapsed (%v)", elapsed)
		}
		if srv.Stopped() {
			t.Fatal("timed-out stop must not mark the service stopped")
		}
		if got := doneCount.Load(); got != 0 {
			t.Fatalf("DoneHandler ran before requests converged: %d", got)
		}
		// Interest was removed on the first attempt; the service stays
		// unavailable while stopping.
		waitForNoResponders(t, nc, "drain.timeout")
		if err := srv.AddEndpoint("more", micro.HandlerFunc(func(micro.Request) {}),
			micro.WithEndpointSubject("drain.more")); !errors.Is(err, micro.ErrServiceStopped) {
			t.Fatalf("AddEndpoint while stopping: want %v; got %v", micro.ErrServiceStopped, err)
		}

		// A retried Stop while the request is still running must wait again
		// and time out, not commit the stop early.
		retryStart := time.Now()
		if err := srv.Stop(); !errors.Is(err, micro.ErrServiceDrainTimeout) {
			t.Fatalf("retry Stop while in flight: want %v; got %v", micro.ErrServiceDrainTimeout, err)
		}
		if time.Since(retryStart) < 100*time.Millisecond {
			t.Fatal("retried Stop returned without waiting for the request")
		}
		if srv.Stopped() {
			t.Fatal("retried Stop must not commit while a request is in flight")
		}

		close(release)
		if err := srv.Stop(); err != nil {
			t.Fatalf("retry Stop: %v", err)
		}
		if !srv.Stopped() {
			t.Fatal("service should be stopped after the retry")
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("DoneHandler did not fire; want count 1, got %d", doneCount.Load())
		}
		if got := doneCount.Load(); got != 1 {
			t.Fatalf("DoneHandler count: want 1; got %d", got)
		}
		// Stop stays idempotent once committed.
		if err := srv.Stop(); err != nil {
			t.Fatalf("second Stop: %v", err)
		}
		select {
		case <-done:
			t.Fatal("DoneHandler ran more than once")
		case <-time.After(100 * time.Millisecond):
		}
	})
}

// TestRequestRespondsExactlyOnce verifies response ownership: the first
// Respond/Error wins, every later attempt fails with
// ErrRequestAlreadyResponded and only one message reaches the reply subject.
func TestRequestRespondsExactlyOnce(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		secondRespond := make(chan error, 1)
		thirdError := make(chan error, 1)
		srv, err := micro.AddService(nc, micro.Config{
			Name:    "single_response",
			Version: "0.1.0",
			Endpoint: &micro.EndpointConfig{
				Subject: "single.response",
				Handler: micro.HandlerFunc(func(req micro.Request) {
					if err := req.Respond([]byte("first")); err != nil {
						t.Errorf("first Respond: %v", err)
					}
					secondRespond <- req.Respond([]byte("second"))
					thirdError <- req.Error("500", "late error", nil)
				}),
			},
		})
		if err != nil {
			t.Fatalf("AddService: %v", err)
		}
		defer func() {
			if err := srv.Stop(); err != nil {
				t.Errorf("Stop: %v", err)
			}
		}()

		inbox := nats.NewInbox()
		replies, err := nc.SubscribeSync(inbox)
		if err != nil {
			t.Fatalf("SubscribeSync: %v", err)
		}
		if err := nc.PublishRequest("single.response", inbox, nil); err != nil {
			t.Fatalf("PublishRequest: %v", err)
		}

		select {
		case err := <-secondRespond:
			if !errors.Is(err, micro.ErrRequestAlreadyResponded) {
				t.Fatalf("second Respond: want %v; got %v", micro.ErrRequestAlreadyResponded, err)
			}
		case <-time.After(time.Second):
			t.Fatal("second Respond did not return")
		}
		select {
		case err := <-thirdError:
			if !errors.Is(err, micro.ErrRequestAlreadyResponded) {
				t.Fatalf("late Error: want %v; got %v", micro.ErrRequestAlreadyResponded, err)
			}
		case <-time.After(time.Second):
			t.Fatal("late Error did not return")
		}

		msg, err := replies.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("first response: %v", err)
		}
		if string(msg.Data) != "first" {
			t.Fatalf("want first response; got %q", msg.Data)
		}
		if _, err := replies.NextMsg(100 * time.Millisecond); err == nil {
			t.Fatal("a late response was published")
		}
	})
}

// TestRequestFailedFirstResponseStillClaims verifies that even when the first
// response cannot be delivered (no reply subject), the request stays claimed
// and later attempts get the already-responded error rather than retrying.
func TestRequestFailedFirstResponseStillClaims(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		firstErr := make(chan error, 1)
		secondErr := make(chan error, 1)
		srv, err := micro.AddService(nc, micro.Config{
			Name:    "failed_response",
			Version: "0.1.0",
			Endpoint: &micro.EndpointConfig{
				Subject: "failed.response",
				Handler: micro.HandlerFunc(func(req micro.Request) {
					firstErr <- req.Respond([]byte("lost"))
					secondErr <- req.Respond([]byte("also lost"))
				}),
			},
		})
		if err != nil {
			t.Fatalf("AddService: %v", err)
		}
		defer func() {
			if err := srv.Stop(); err != nil {
				t.Errorf("Stop: %v", err)
			}
		}()

		// No reply subject: the first response fails at the connection.
		if err := nc.Publish("failed.response", nil); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		select {
		case err := <-firstErr:
			if !errors.Is(err, micro.ErrRespond) {
				t.Fatalf("first Respond: want an %v wrapper; got %v", micro.ErrRespond, err)
			}
		case <-time.After(time.Second):
			t.Fatal("first Respond did not return")
		}
		select {
		case err := <-secondErr:
			if !errors.Is(err, micro.ErrRequestAlreadyResponded) {
				t.Fatalf("second Respond: want %v; got %v", micro.ErrRequestAlreadyResponded, err)
			}
		case <-time.After(time.Second):
			t.Fatal("second Respond did not return")
		}
	})
}

// TestQueueGroupMemberExit verifies that a stopped queue-group member no
// takes part in the group and none of its replies can affect the surviving
// group once it has left.
func TestQueueGroupMemberExit(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		makeMember := func(id string) micro.Service {
			srv, err := micro.AddService(nc, micro.Config{
				Name:    "qg_members",
				Version: "0.1.0",
				Endpoint: &micro.EndpointConfig{
					Subject:    "qg.work",
					QueueGroup: "workers",
					Handler: micro.HandlerFunc(func(req micro.Request) {
						if err := req.Respond([]byte(id)); err != nil {
							t.Errorf("Respond: %v", err)
						}
					}),
				},
			})
			if err != nil {
				t.Fatalf("AddService %q: %v", id, err)
			}
			return srv
		}
		first := makeMember("first")
		second := makeMember("second")

		// Both members are reachable through the queue group.
		seen := map[string]bool{}
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) && (!seen["first"] || !seen["second"]) {
			resp, err := nc.Request("qg.work", nil, time.Second)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			seen[string(resp.Data)] = true
		}
		if !seen["first"] || !seen["second"] {
			t.Fatalf("expected both members to serve requests; got %v", seen)
		}

		if err := first.Stop(); err != nil {
			t.Fatalf("Stop first: %v", err)
		}

		// After the member leaves, every request is served by the survivor:
		// a reply from the old member would show up as a "first" payload.
		for range 20 {
			resp, err := nc.Request("qg.work", nil, time.Second)
			if err != nil {
				t.Fatalf("request after member exit: %v", err)
			}
			if string(resp.Data) != "second" {
				t.Fatalf("old queue-group member still responded: %q", string(resp.Data))
			}
		}

		// Monitoring only reports the surviving instance.
		pingSubject, err := micro.ControlSubject(micro.PingVerb, "qg_members", "")
		if err != nil {
			t.Fatalf("ControlSubject: %v", err)
		}
		inbox := nats.NewInbox()
		pingSub, err := nc.SubscribeSync(inbox)
		if err != nil {
			t.Fatalf("SubscribeSync: %v", err)
		}
		if err := nc.PublishRequest(pingSubject, inbox, nil); err != nil {
			t.Fatalf("PublishRequest: %v", err)
		}
		pings := 0
		for {
			msg, err := pingSub.NextMsg(200 * time.Millisecond)
			if err != nil {
				break
			}
			var ping micro.Ping
			if err := json.Unmarshal(msg.Data, &ping); err != nil {
				t.Fatalf("ping decode: %v", err)
			}
			if ping.ID == first.Info().ID {
				t.Fatal("stopped member still answers monitoring pings")
			}
			pings++
		}
		if pings != 1 {
			t.Fatalf("want 1 reachable member; got %d", pings)
		}
		if err := second.Stop(); err != nil {
			t.Fatalf("Stop second: %v", err)
		}
	})
}

// TestDoneHandlerWaitsForAllRequests verifies the DoneHandler fires exactly
// once, and only after every accepted request handler has returned.
//
// A NATS subscription delivers its messages through one goroutine, so the
// accepted requests form a serial backlog: the handler blocks on the first
// message while the rest wait in the subscription's pending queue. Stop
// must not invoke the DoneHandler until that whole backlog has drained.
func TestDoneHandlerWaitsForAllRequests(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		const total = 20
		var handled atomic.Int32
		release := make(chan struct{})
		entered := make(chan struct{})
		done := make(chan struct{}, 4)
		var doneCount atomic.Int32

		srv, err := micro.AddService(nc, micro.Config{
			Name:    "done_wait",
			Version: "0.1.0",
			DoneHandler: func(micro.Service) {
				doneCount.Add(1)
				done <- struct{}{}
			},
		})
		if err != nil {
			t.Fatalf("AddService: %v", err)
		}
		if err := srv.AddEndpoint("work",
			micro.HandlerFunc(func(req micro.Request) {
				// Only the first request is being handled while release is
				// closed; signal once so the test knows a request was accepted.
				select {
				case entered <- struct{}{}:
				default:
				}
				<-release
				handled.Add(1)
				if err := req.Respond(nil); err != nil {
					t.Errorf("Respond: %v", err)
				}
			}),
			micro.WithEndpointSubject("done.wait"),
			micro.WithEndpointPendingLimits(total*2, -1)); err != nil {
			t.Fatalf("AddEndpoint: %v", err)
		}

		errs := make(chan error, total)
		var wg sync.WaitGroup
		for range total {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := nc.Request("done.wait", nil, 5*time.Second)
				errs <- err
			}()
		}
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("the first request was not accepted")
		}
		// Let the backlog settle in the subscription's pending queue.
		if err := nc.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}

		stopErr := make(chan error, 1)
		go func() { stopErr <- srv.Stop() }()
		select {
		case <-done:
			t.Fatal("DoneHandler fired while requests are still in flight")
		case <-time.After(100 * time.Millisecond):
		}

		close(release)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("request error: %v", err)
			}
		}

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("DoneHandler did not fire after all requests converged")
		}
		if err := <-stopErr; err != nil {
			t.Fatalf("Stop: %v", err)
		}
		if got := handled.Load(); got != total {
			t.Fatalf("handled: want %d; got %d", total, got)
		}
		if got := doneCount.Load(); got != 1 {
			t.Fatalf("DoneHandler count: want 1; got %d", got)
		}
	})
}

// TestServiceStopRejectsNewEndpoints verifies the stopped service cannot be
// extended with new endpoints after the drain boundary.
func TestServiceStopRejectsNewEndpoints(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		srv, err := micro.AddService(nc, micro.Config{
			Name:    "no_new_endpoints",
			Version: "0.1.0",
		})
		if err != nil {
			t.Fatalf("AddService: %v", err)
		}
		if err := srv.Stop(); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		err = srv.AddEndpoint("late", micro.HandlerFunc(func(micro.Request) {}),
			micro.WithEndpointSubject("late.endpoint"))
		if !errors.Is(err, micro.ErrServiceStopped) {
			t.Fatalf("AddEndpoint after Stop: want %v; got %v", micro.ErrServiceStopped, err)
		}
	})
}

// TestServiceStopOnConnClose verifies closing the connection commits the
// stop, fires the DoneHandler once, releases interest, and never deadlocks
// on a handler that cannot finish because its connection is gone.
func TestServiceStopOnConnClose(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		entered := make(chan struct{})
		done := make(chan struct{}, 2)
		var doneCount atomic.Int32

		srv, err := micro.AddService(nc, micro.Config{
			Name:    "conn_close",
			Version: "0.1.0",
			DoneHandler: func(micro.Service) {
				doneCount.Add(1)
				done <- struct{}{}
			},
			Endpoint: &micro.EndpointConfig{
				Subject: "conn.close",
				Handler: micro.HandlerFunc(func(micro.Request) {
					entered <- struct{}{}
					// Deliberately never returns: the connection close must
					// not wait on handlers that can no longer respond.
					select {}
				}),
			},
		})
		if err != nil {
			t.Fatalf("AddService: %v", err)
		}

		if err := nc.PublishRequest("conn.close", nats.NewInbox(), nil); err != nil {
			t.Fatalf("PublishRequest: %v", err)
		}
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("handler did not start")
		}

		// A second connection observes the interest the service holds.
		observer, err := nats.Connect(nc.ConnectedUrl())
		if err != nil {
			t.Fatalf("observer connect: %v", err)
		}
		defer observer.Close()

		nc.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("DoneHandler did not fire after the connection closed")
		}
		if !srv.Stopped() {
			t.Fatal("service should be stopped after the connection closed")
		}
		if got := doneCount.Load(); got != 1 {
			t.Fatalf("DoneHandler count: want 1; got %d", got)
		}
		waitForNoResponders(t, observer, "conn.close")
	})
}

// TestServiceStopIdempotentUnderContention hammers Stop and request
// responses concurrently to make the commit boundary deterministic under
// repetition (run with -race and -count).
func TestServiceStopIdempotentUnderContention(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		var doneCount atomic.Int32
		srv, err := micro.AddService(nc, micro.Config{
			Name:         "stop_contention",
			Version:      "0.1.0",
			DrainTimeout: time.Second,
			DoneHandler: func(micro.Service) {
				doneCount.Add(1)
			},
			Endpoint: &micro.EndpointConfig{
				Subject: "stop.contention",
				Handler: micro.HandlerFunc(func(req micro.Request) {
					if err := req.Respond([]byte("ok")); err != nil &&
						!errors.Is(err, micro.ErrRequestAlreadyResponded) {
						t.Errorf("Respond: %v", err)
					}
				}),
			},
		})
		if err != nil {
			t.Fatalf("AddService: %v", err)
		}

		var wg sync.WaitGroup
		for range 20 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := nc.Request("stop.contention", nil, time.Second); err != nil &&
					!errors.Is(err, nats.ErrNoResponders) {
					t.Errorf("request: %v", err)
				}
			}()
		}
		for range 5 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := srv.Stop(); err != nil {
					t.Errorf("Stop: %v", err)
				}
			}()
		}
		wg.Wait()

		if !srv.Stopped() {
			t.Fatal("service should be stopped")
		}
		if got := doneCount.Load(); got != 1 {
			t.Fatalf("DoneHandler count: want 1; got %d", got)
		}
	})
}
