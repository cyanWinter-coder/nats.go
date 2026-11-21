// Copyright 2022-2024 The NATS Authors
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
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/parser"
)

type (
	orderedConsumer struct {
		js              *jetStream
		cfg             *OrderedConsumerConfig
		stream          string
		currentConsumer *pullConsumer
		currentSub      *pullSubscription
		cursor          cursor
		namePrefix      string
		serial          int
		consumerType    consumerType
		// doReset is a size-1 tick channel used by the Consume recovery
		// goroutine. triggerResetLocked coalesces repeated requests so that a
		// burst of failures (e.g. missing heartbeat and reconnect at the same
		// time) schedules at most one recovery: once the channel holds a tick,
		// every further trigger is a non-blocking no-op until the recovery
		// loop drains it.
		doReset chan struct{}
		// resetMu serializes full consumer rebuilds. Every reset, including
		// synchronous ones from Fetch/Messages, goes through runRecovery, so
		// an older rebuild can never finish after and overwrite the state
		// installed by a newer one.
		resetMu sync.Mutex
		// ctx/cancel bound the lifetime of the ordered consumer and every
		// background operation it starts (rebuilds, deferred cleanup).
		ctx    context.Context
		cancel context.CancelFunc

		userErrHandler    ConsumeErrHandler
		stopAfter         int // StopAfter of Consume or Messages, 0 if not set
		deliveredMessages int // messages handed to the caller, across resets
		runningFetch      *fetchResult
		fetchInProgress   bool
		subscription      *orderedSubscription
		sync.Mutex
	}

	orderedSubscription struct {
		consumer *orderedConsumer
		opts     []PullMessagesOpt
		done     chan struct{}
		closed   atomic.Uint32
	}

	cursor struct {
		streamSeq  uint64
		deliverSeq uint64
	}

	consumerType int
)

const (
	consumerTypeNotSet consumerType = iota
	consumerTypeConsume
	consumerTypeFetch
)

var (
	errOrderedSequenceMismatch = errors.New("sequence mismatch")
	errOrderedConsumerClosed   = errors.New("ordered consumer closed")
)

// Consume can be used to continuously receive messages and handle them
// with the provided callback function. Consume cannot be used concurrently
// when using ordered consumer.
//
// See [Consumer.Consume] for more details.
func (c *orderedConsumer) Consume(handler MessageHandler, opts ...PullConsumeOpt) (ConsumeContext, error) {
	c.Lock()
	if (c.consumerType == consumerTypeNotSet || c.consumerType == consumerTypeConsume) && c.currentConsumer == nil {
		// The constructor always creates the initial consumer, so this only
		// happens after a failed/canceled recovery. runRecovery takes the lock
		// itself, so release it across the call.
		c.Unlock()
		if err := c.runRecovery(); err != nil {
			return nil, err
		}
		c.Lock()
	} else if c.consumerType == consumerTypeConsume && c.currentConsumer != nil {
		c.Unlock()
		return nil, ErrOrderedConsumerConcurrentRequests
	}
	if c.consumerType == consumerTypeFetch {
		c.Unlock()
		return nil, ErrOrderConsumerUsedAsFetch
	}
	c.consumerType = consumerTypeConsume
	consumeOpts, err := parseConsumeOpts(true, opts...)
	if err != nil {
		c.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrInvalidOption, err)
	}
	c.userErrHandler = consumeOpts.ErrHandler
	c.stopAfter = max(consumeOpts.StopAfter, 0)
	c.deliveredMessages = 0
	// Preserve the resolved heartbeat when creating the pull consumer.
	opts = append(opts, PullHeartbeat(consumeOpts.Heartbeat), consumeReconnectNotify())
	// innerOpts returns the options for the inner pull subscription of the
	// current serial. Lock should be held when calling it.
	innerOpts := func() []PullConsumeOpt {
		o := append(slices.Clip(opts), ConsumeErrHandler(c.errHandler(c.serial)))
		if c.stopAfter > 0 {
			o = append(o, StopAfter(c.stopAfter-c.deliveredMessages))
		}
		return o
	}
	sub := &orderedSubscription{
		consumer: c,
		done:     make(chan struct{}, 1),
	}
	c.subscription = sub
	internalHandler := func(serial int) func(msg Msg) {
		return func(msg Msg) {
			c.Lock()
			// handler is a noop if message was delivered for a consumer with different serial
			if serial != c.serial {
				c.Unlock()
				return
			}
			tokens, err := msg.(*jetStreamMsg).metadataFields()
			if err != nil {
				currentSub := c.currentSub
				c.Unlock()
				c.errHandler(serial)(currentSub, err)
				return
			}
			dseq := parser.ParseNum(tokens[parser.AckConsumerSeqTokenPos])
			if dseq != c.cursor.deliverSeq+1 {
				// A gap in the delivery sequence means messages were lost or
				// redelivered by the server-side consumer. Rebuild from the
				// last stream sequence handed to the caller instead of
				// processing a message out of order.
				c.Unlock()
				c.errHandler(serial)(sub, errOrderedSequenceMismatch)
				return
			}
			if c.stopAfter > 0 && c.deliveredMessages >= c.stopAfter {
				c.Unlock()
				return
			}
			c.cursor.deliverSeq = dseq
			c.cursor.streamSeq = parser.ParseNum(tokens[parser.AckStreamSeqTokenPos])
			c.deliveredMessages++
			last := c.stopAfter > 0 && c.deliveredMessages == c.stopAfter
			c.Unlock()
			handler(msg)
			if last {
				sub.Stop()
			}
		}
	}

	cc, err := c.currentConsumer.Consume(internalHandler(c.serial), innerOpts()...)
	if err != nil {
		c.Unlock()
		return nil, err
	}
	c.currentSub = cc.(*pullSubscription)
	c.Unlock()

	go c.consumeRecoveryLoop(sub, internalHandler, innerOpts)
	return sub, nil
}

// consumeRecoveryLoop owns all asynchronous rebuilds for a Consume
// subscription. doReset ticks are coalesced: while one recovery is running,
// additional failures only set the flag again and trigger a single follow-up
// rebuild once the current one finishes. Every rebuild goes through
// runRecovery (which holds resetMu), and the serial is re-read after it
// returns, so a stale rebuild can never overwrite newer consumer state.
func (c *orderedConsumer) consumeRecoveryLoop(
	sub *orderedSubscription,
	internalHandler func(serial int) func(msg Msg),
	innerOpts func() []PullConsumeOpt,
) {
	for {
		select {
		case <-c.ctx.Done():
			// Lifecycle canceled: stop the active subscription so Closed()
			// fires and no further deliveries or background reads happen.
			sub.Stop()
			return
		case <-sub.done:
			s := sub.consumer.currentSub
			if s != nil {
				sub.consumer.Lock()
				s.Stop()
				sub.consumer.Unlock()
			}
			return
		case <-c.doReset:
		}

		c.Lock()
		done := c.stopAfter > 0 && c.deliveredMessages >= c.stopAfter
		c.Unlock()
		if done {
			sub.Stop()
			return
		}
		if err := c.runRecovery(); err != nil {
			if errors.Is(err, errOrderedConsumerClosed) || errors.Is(err, context.Canceled) {
				continue
			}
			c.Lock()
			serial := c.serial
			currentSub := c.currentSub
			// Re-arm the recovery so the loop retries instead of
			// re-subscribing to the consumer that just failed.
			c.triggerResetLocked()
			c.Unlock()
			c.errHandler(serial)(currentSub, err)
			continue
		}
		// A failure may have arrived while the rebuild was in progress. If the
		// tick channel is still armed, run another recovery instead of
		// re-subscribing to the consumer just rebuilt.
		if len(c.doReset) > 0 {
			continue
		}
		c.Lock()
		serial := c.serial
		consumeOpts := innerOpts()
		closed := sub.closed.Load() == 1
		c.Unlock()
		if closed {
			return
		}
		if cc, err := c.currentConsumer.Consume(internalHandler(serial), consumeOpts...); err != nil {
			c.errHandler(serial)(cc, err)
		} else {
			c.Lock()
			c.currentSub = cc.(*pullSubscription)
			c.Unlock()
		}
	}
}

func (c *orderedConsumer) errHandler(serial int) func(cc ConsumeContext, err error) {
	return func(cc ConsumeContext, err error) {
		c.Lock()

		if c.userErrHandler != nil && !errors.Is(err, errOrderedSequenceMismatch) && !errors.Is(err, errConnected) {
			c.userErrHandler(cc, err)
		}
		if errors.Is(err, ErrConnectionClosed) {
			if c.subscription != nil {
				c.Unlock()
				c.subscription.Stop()
				return
			}
			c.Unlock()
			return
		}

		if errors.Is(err, ErrNoHeartbeat) ||
			errors.Is(err, errOrderedSequenceMismatch) ||
			errors.Is(err, ErrConsumerDeleted) ||
			errors.Is(err, errConnected) ||
			errors.Is(err, nats.ErrNoResponders) {
			// Only reset if the error comes from the current consumer serial.
			// triggerReset coalesces concurrent failures (e.g. a missing
			// heartbeat arriving together with a reconnect) into a single
			// rebuild.
			if serial == c.serial {
				c.triggerResetLocked()
			}
		}
		c.Unlock()
	}
}

// triggerResetLocked schedules an asynchronous consumer rebuild for the
// Consume recovery loop. Repeated calls while a recovery is already queued or
// in flight are coalesced by the size-1 channel, so concurrent
// heartbeat/reconnect failures cannot start duplicate rebuilds and error
// delivery can never block on a full channel. The caller must hold c's lock.
func (c *orderedConsumer) triggerResetLocked() {
	if c.ctx.Err() != nil {
		return
	}
	select {
	case c.doReset <- struct{}{}:
	default:
	}
}

// Messages returns MessagesContext, allowing continuously iterating
// over messages on a stream. Messages cannot be used concurrently
// when using ordered consumer.
//
// See [Consumer.Messages] for more details.
func (c *orderedConsumer) Messages(opts ...PullMessagesOpt) (MessagesContext, error) {
	c.Lock()
	if (c.consumerType == consumerTypeNotSet || c.consumerType == consumerTypeConsume) && c.currentConsumer == nil {
		c.Unlock()
		if err := c.runRecovery(); err != nil {
			return nil, err
		}
		c.Lock()
	} else if c.consumerType == consumerTypeConsume && c.currentConsumer != nil {
		c.Unlock()
		return nil, ErrOrderedConsumerConcurrentRequests
	}
	if c.consumerType == consumerTypeFetch {
		c.Unlock()
		return nil, ErrOrderConsumerUsedAsFetch
	}
	c.consumerType = consumerTypeConsume
	consumeOpts, err := parseMessagesOpts(true, opts...)
	if err != nil {
		c.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrInvalidOption, err)
	}
	// Preserve the resolved heartbeat when creating the pull consumer.
	opts = append(opts,
		PullHeartbeat(consumeOpts.Heartbeat),
		WithMessagesErrOnMissingHeartbeat(true),
		messagesReconnectNotify())
	c.stopAfter = max(consumeOpts.StopAfter, 0)
	c.deliveredMessages = 0
	c.userErrHandler = consumeOpts.ErrHandler
	consumer := c.currentConsumer
	c.Unlock()
	cc, err := consumer.Messages(opts...)
	if err != nil {
		return nil, err
	}
	c.Lock()
	c.currentSub = cc.(*pullSubscription)

	sub := &orderedSubscription{
		consumer: c,
		opts:     opts,
		done:     make(chan struct{}, 1),
	}
	c.subscription = sub
	c.Unlock()

	return sub, nil
}

func (s *orderedSubscription) Next(opts ...NextOpt) (Msg, error) {
	for {
		msg, err := s.consumer.currentSub.Next(opts...)
		if err != nil {
			// Check for errors which should be returned directly
			// without resetting the consumer
			if errors.Is(err, ErrInvalidOption) {
				return nil, err
			}
			if errors.Is(err, nats.ErrTimeout) {
				return nil, err
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			if errors.Is(err, ErrMsgIteratorClosed) {
				s.Stop()
				return nil, err
			}
			if err := s.resetMessages(); err != nil {
				return nil, err
			}
			continue
		}

		tokens, err := msg.(*jetStreamMsg).metadataFields()
		if err != nil {
			return nil, err
		}
		c := s.consumer
		c.Lock()
		serial := serialNumberFromConsumer(tokens[parser.AckConsumerTokenPos])
		if serial != c.serial {
			// Message from a consumer generation already replaced by a
			// recovery; drop it and read again from the current generation.
			c.Unlock()
			continue
		}
		dseq := parser.ParseNum(tokens[parser.AckConsumerSeqTokenPos])
		if dseq != c.cursor.deliverSeq+1 {
			// Delivery gap: the current server-side consumer lost or skipped
			// messages. Rebuild from the last stream sequence returned to the
			// caller so nothing after the gap is skipped and nothing already
			// delivered is handed out twice.
			c.Unlock()
			if err := s.resetMessages(); err != nil {
				return nil, err
			}
			continue
		}
		c.cursor.deliverSeq = dseq
		c.cursor.streamSeq = parser.ParseNum(tokens[parser.AckStreamSeqTokenPos])
		c.deliveredMessages++
		c.Unlock()
		return msg, nil
	}
}

// resetMessages replaces the inner iterator with a new one, created on a
// new consumer which resumes after the last message returned by Next.
// With StopAfter set, the new iterator is limited to the messages that
// are left to deliver.
func (s *orderedSubscription) resetMessages() error {
	c := s.consumer
	if err := c.runRecovery(); err != nil {
		if errors.Is(err, errOrderedConsumerClosed) || errors.Is(err, context.Canceled) {
			return ErrMsgIteratorClosed
		}
		return err
	}
	opts := s.opts
	if c.stopAfter > 0 {
		c.Lock()
		remaining := c.stopAfter - c.deliveredMessages
		c.Unlock()
		if remaining <= 0 {
			s.Stop()
			return ErrMsgIteratorClosed
		}
		opts = append(slices.Clip(opts), StopAfter(remaining))
	}
	cc, err := c.currentConsumer.Messages(opts...)
	if err != nil {
		return err
	}
	c.Lock()
	c.currentSub = cc.(*pullSubscription)
	c.Unlock()
	return nil
}

func (s *orderedSubscription) Stop() {
	if !s.closed.CompareAndSwap(0, 1) {
		return
	}
	s.consumer.Lock()
	defer s.consumer.Unlock()
	if s.consumer.currentSub != nil {
		s.consumer.currentSub.Stop()
	}
	if s.consumer.cancel != nil {
		s.consumer.cancel()
	}
	close(s.done)
}

func (s *orderedSubscription) Drain() {
	if !s.closed.CompareAndSwap(0, 1) {
		return
	}
	s.consumer.Lock()
	defer s.consumer.Unlock()
	if s.consumer.currentSub != nil {
		s.consumer.currentSub.Drain()
	}
	if s.consumer.cancel != nil {
		s.consumer.cancel()
	}
	close(s.done)
}

// Closed returns a channel that is closed when the consuming is
// fully stopped/drained. When the channel is closed, no more messages
// will be received and processing is complete.
func (s *orderedSubscription) Closed() <-chan struct{} {
	closedCh := make(chan struct{})

	go func() {
		// First wait for s.done to be closed
		<-s.done

		// Then ensure underlying consumer is also closed (it may still be draining)
		s.consumer.Lock()
		if s.consumer.currentSub != nil {
			closed := s.consumer.currentSub.Closed()
			s.consumer.Unlock()
			<-closed
		} else {
			s.consumer.Unlock()
		}

		close(closedCh)
	}()
	return closedCh
}

// Fetch is used to retrieve up to a provided number of messages from a
// stream. This method will always send a single request and wait until
// either all messages are retrieved or request times out.
//
// It is not efficient to use Fetch with on an ordered consumer, as it will
// reset the consumer for each subsequent Fetch call.
// Consider using [Consumer.Consume] or [Consumer.Messages] instead.
func (c *orderedConsumer) Fetch(batch int, opts ...FetchOpt) (MessageBatch, error) {
	return c.fetchWithOpts(batch, true, opts...)
}

func (c *orderedConsumer) fetchWithOpts(batch int, allowMaxBytes bool, opts ...FetchOpt) (MessageBatch, error) {
	return c.fetch(func(p *pullConsumer) (MessageBatch, error) {
		return p.fetchWithOpts(batch, allowMaxBytes, opts...)
	})
}

func (c *orderedConsumer) fetch(f func(*pullConsumer) (MessageBatch, error)) (MessageBatch, error) {
	c.Lock()
	if c.consumerType == consumerTypeConsume {
		c.Unlock()
		return nil, ErrOrderConsumerUsedAsConsume
	}
	if c.fetchInProgress {
		c.Unlock()
		return nil, ErrOrderedConsumerConcurrentRequests
	}
	if c.runningFetch != nil {
		if !c.runningFetch.closed() {
			c.Unlock()
			return nil, ErrOrderedConsumerConcurrentRequests
		}
		if c.runningFetch.sseq != 0 {
			c.cursor.streamSeq = c.runningFetch.sseq
		}
	}
	if c.ctx.Err() != nil {
		c.Unlock()
		return nil, errOrderedConsumerClosed
	}
	c.consumerType = consumerTypeFetch
	sub := orderedSubscription{
		consumer: c,
		done:     make(chan struct{}),
	}
	c.subscription = &sub
	c.fetchInProgress = true
	c.Unlock()
	err := c.runRecovery()
	var msgs MessageBatch
	if err == nil {
		msgs, err = f(c.currentConsumer)
	}
	c.Lock()
	defer c.Unlock()
	c.fetchInProgress = false
	if err != nil {
		return nil, err
	}
	c.runningFetch = msgs.(*fetchResult)
	return msgs, nil
}

// FetchBytes is used to retrieve up to a provided bytes from the
// stream. This method will always send a single request and wait until
// provided number of bytes is exceeded or request times out.
//
// It is not efficient to use FetchBytes with on an ordered consumer, as it will
// reset the consumer for each subsequent Fetch call.
// Consider using [Consumer.Consume] or [Consumer.Messages] instead.
func (c *orderedConsumer) FetchBytes(maxBytes int, opts ...FetchOpt) (MessageBatch, error) {
	return c.fetch(func(p *pullConsumer) (MessageBatch, error) {
		return p.FetchBytes(maxBytes, opts...)
	})
}

// FetchNoWait is used to retrieve up to a provided number of messages
// from a stream. This method will always send a single request and
// immediately return up to a provided number of messages or wait until
// at least one message is available or request times out.
//
// It is not efficient to use FetchNoWait with on an ordered consumer, as it will
// reset the consumer for each subsequent Fetch call.
// Consider using [Consumer.Consume] or [Consumer.Messages] instead.
func (c *orderedConsumer) FetchNoWait(batch int) (MessageBatch, error) {
	return c.fetch(func(p *pullConsumer) (MessageBatch, error) {
		return p.FetchNoWait(batch)
	})
}

// Next is used to retrieve the next message from the stream. This
// method will block until the message is retrieved or timeout is
// reached.
//
// It is not efficient to use Next with on an ordered consumer, as it will
// reset the consumer for each subsequent Fetch call.
// Consider using [Consumer.Consume] or [Consumer.Messages] instead.
func (c *orderedConsumer) Next(opts ...FetchOpt) (Msg, error) {
	res, err := c.fetchWithOpts(1, false, opts...)
	if err != nil {
		return nil, err
	}
	msg := <-res.Messages()
	if msg != nil {
		return msg, nil
	}
	if res.Error() == nil {
		return nil, nats.ErrTimeout
	}
	return nil, res.Error()
}

func serialNumberFromConsumer(name string) int {
	i := strings.LastIndexByte(name, '_')
	if i < 0 {
		return 0
	}
	serial, err := strconv.Atoi(name[i+1:])
	if err != nil {
		return 0
	}
	return serial
}

// runRecovery rebuilds the server-side consumer, resuming after the last
// stream sequence already handed to the caller. It is the single entry point
// for every rebuild (Consume recovery loop, Messages resets, Fetch), which
// gives three guarantees:
//
//   - rebuilds are serialized by resetMu, so concurrent failures (a missing
//     heartbeat arriving together with a reconnect) can never run two
//     rebuilds at once;
//   - the whole rebuild happens against a snapshot taken under c's lock, and
//     the new consumer is only published once it is ready, so an older
//     rebuild cannot finish after and overwrite a newer serial/cursor;
//   - the rebuild is bound to the consumer's lifecycle context: once the
//     context is canceled, in-flight consumer creation is aborted and no new
//     rebuild starts.
func (c *orderedConsumer) runRecovery() error {
	c.resetMu.Lock()
	defer c.resetMu.Unlock()

	if err := c.ctx.Err(); err != nil {
		return errOrderedConsumerClosed
	}

	c.Lock()
	if c.subscription != nil && c.subscription.closed.Load() == 1 {
		c.Unlock()
		return errOrderedConsumerClosed
	}
	oldConsumer := c.currentConsumer
	oldSub := c.currentSub
	consumerConfig := c.getConsumerConfig()
	attempts := c.cfg.MaxResetAttempts
	sub := c.subscription
	c.Unlock()

	if oldSub != nil {
		oldSub.Stop()
	}
	if oldConsumer != nil {
		oldConsumer.Lock()
		consName := oldConsumer.CachedInfo().Name
		oldConsumer.Unlock()
		// Best-effort cleanup of the abandoned server-side consumer. It runs
		// under the lifecycle context so a canceled consumer cannot leave the
		// goroutine behind, and a timeout keeps a missing server from
		// blocking the recovery indefinitely.
		go func() {
			ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
			defer cancel()
			_ = c.js.DeleteConsumer(ctx, c.stream, consName)
		}()
	}

	// A fresh consumer always restarts its delivery sequence at 1.
	c.Lock()
	c.cursor.deliverSeq = 0
	c.Unlock()

	var (
		err  error
		cons Consumer
	)
	retryOpts := backoffOpts{
		attempts:        attempts,
		initialInterval: time.Second,
		factor:          2,
		maxInterval:     10 * time.Second,
		cancel:          c.ctx.Done(),
	}
	err = retryWithBackoff(func(attempt int) (bool, error) {
		if c.ctx.Err() != nil {
			return false, errOrderedConsumerClosed
		}
		if sub != nil && sub.closed.Load() == 1 {
			return false, errOrderedConsumerClosed
		}
		ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
		defer cancel()
		cons, err = c.js.CreateOrUpdateConsumer(ctx, c.stream, *consumerConfig)
		if err != nil {
			return true, err
		}
		return false, nil
	}, retryOpts)
	if err != nil {
		if c.ctx.Err() != nil || (sub != nil && sub.closed.Load() == 1) {
			return errOrderedConsumerClosed
		}
		return err
	}

	newConsumer := cons.(*pullConsumer)
	c.Lock()
	// Do not publish the rebuild if the consumer was stopped or canceled
	// while it was in flight: the subscription is dead and its recovery loop
	// must not pick up a new consumer generation.
	if c.ctx.Err() != nil || (sub != nil && sub.closed.Load() == 1) {
		c.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = c.js.DeleteConsumer(ctx, c.stream, newConsumer.CachedInfo().Name)
		cancel()
		return errOrderedConsumerClosed
	}
	c.currentConsumer = newConsumer
	c.currentSub = nil
	c.Unlock()
	return nil
}

func (c *orderedConsumer) getConsumerConfig() *ConsumerConfig {
	c.serial++
	var nextSeq uint64

	// if stream sequence is not initialized, no message was consumed yet
	// therefore, start from the beginning (either from 1 or from the provided sequence)
	if c.cursor.streamSeq == 0 {
		if c.cfg.OptStartSeq != 0 {
			nextSeq = c.cfg.OptStartSeq
		} else {
			nextSeq = 1
		}
	} else {
		// otherwise, start from the next sequence
		nextSeq = c.cursor.streamSeq + 1
	}

	if c.cfg.MaxResetAttempts == 0 {
		c.cfg.MaxResetAttempts = -1
	}
	name := fmt.Sprintf("%s_%d", c.namePrefix, c.serial)
	cfg := &ConsumerConfig{
		Name:              name,
		DeliverPolicy:     DeliverByStartSequencePolicy,
		OptStartSeq:       nextSeq,
		AckPolicy:         AckNonePolicy,
		InactiveThreshold: 5 * time.Minute,
		Replicas:          1,
		HeadersOnly:       c.cfg.HeadersOnly,
		MemoryStorage:     true,
		Metadata:          c.cfg.Metadata,
	}
	if len(c.cfg.FilterSubjects) == 1 {
		cfg.FilterSubject = c.cfg.FilterSubjects[0]
	} else {
		cfg.FilterSubjects = c.cfg.FilterSubjects
	}
	if c.cfg.InactiveThreshold != 0 {
		cfg.InactiveThreshold = c.cfg.InactiveThreshold
	}

	// if the cursor is not yet set, use the provided deliver policy
	if c.cursor.streamSeq != 0 {
		return cfg
	}

	// initial request, some options may be modified at that point
	cfg.DeliverPolicy = c.cfg.DeliverPolicy
	if c.cfg.DeliverPolicy == DeliverLastPerSubjectPolicy ||
		c.cfg.DeliverPolicy == DeliverLastPolicy ||
		c.cfg.DeliverPolicy == DeliverNewPolicy ||
		c.cfg.DeliverPolicy == DeliverAllPolicy {

		cfg.OptStartSeq = 0
	} else if c.cfg.DeliverPolicy == DeliverByStartTimePolicy {
		cfg.OptStartSeq = 0
		cfg.OptStartTime = c.cfg.OptStartTime
	} else {
		cfg.OptStartSeq = c.cfg.OptStartSeq
	}

	if cfg.DeliverPolicy == DeliverLastPerSubjectPolicy && len(c.cfg.FilterSubjects) == 0 {
		cfg.FilterSubjects = []string{">"}
	}

	return cfg
}

func consumeReconnectNotify() PullConsumeOpt {
	return pullOptFunc(func(opts *consumeOpts) error {
		opts.notifyOnReconnect = true
		return nil
	})
}

func messagesReconnectNotify() PullMessagesOpt {
	return pullOptFunc(func(opts *consumeOpts) error {
		opts.notifyOnReconnect = true
		return nil
	})
}

// Info returns information about the ordered consumer.
// Note that this method will fetch the latest instance of the
// consumer from the server, which can be deleted by the library at any time.
func (c *orderedConsumer) Info(ctx context.Context) (*ConsumerInfo, error) {
	c.Lock()
	defer c.Unlock()
	if c.currentConsumer == nil {
		return nil, ErrOrderedConsumerNotCreated
	}
	infoSubject := fmt.Sprintf(apiConsumerInfoT, c.stream, c.currentConsumer.name)
	var resp consumerInfoResponse

	if _, err := c.js.apiRequestJSON(ctx, infoSubject, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeConsumerNotFound {
			return nil, ErrConsumerNotFound
		}
		return nil, resp.Error
	}
	if resp.Error == nil && resp.ConsumerInfo == nil {
		return nil, ErrConsumerNotFound
	}

	c.currentConsumer.info.Store(resp.ConsumerInfo)
	return resp.ConsumerInfo, nil
}

// CachedInfo returns cached information about the consumer currently
// used by the ordered consumer. Cached info will be updated on every call
// to [Consumer.Info] or on consumer reset.
func (c *orderedConsumer) CachedInfo() *ConsumerInfo {
	c.Lock()
	defer c.Unlock()
	if c.currentConsumer == nil {
		return nil
	}
	return c.currentConsumer.info.Load()
}

type backoffOpts struct {
	// total retry attempts
	// -1 for unlimited
	attempts int
	// initial interval after which first retry will be performed
	// defaults to 1s
	initialInterval time.Duration
	// determines whether first function execution should be performed immediately
	disableInitialExecution bool
	// multiplier on each attempt
	// defaults to 2
	factor float64
	// max interval between retries
	// after reaching this value, all subsequent
	// retries will be performed with this interval
	// defaults to 1 minute
	maxInterval time.Duration
	// custom backoff intervals
	// if set, overrides all other options except attempts
	// if attempts are set, then the last interval will be used
	// for all subsequent retries after reaching the limit
	customBackoff []time.Duration
	// cancel channel
	// if set, retry will be canceled when this channel is closed
	cancel <-chan struct{}
	// cancelErr is returned when the cancel channel is closed. Defaults to
	// context.Canceled.
	cancelErr error
}

func retryWithBackoff(f func(int) (bool, error), opts backoffOpts) error {
	var err error
	var shouldContinue bool
	cancelErr := opts.cancelErr
	if cancelErr == nil {
		cancelErr = context.Canceled
	}
	// if custom backoff is set, use it instead of other options
	if len(opts.customBackoff) > 0 {
		if opts.attempts != 0 {
			return errors.New("cannot use custom backoff intervals when attempts are set")
		}
		for i, interval := range opts.customBackoff {
			select {
			case <-opts.cancel:
				return cancelErr
			case <-time.After(interval):
			}
			shouldContinue, err = f(i)
			if !shouldContinue {
				return err
			}
		}
		return err
	}

	// set default options
	if opts.initialInterval == 0 {
		opts.initialInterval = 1 * time.Second
	}
	if opts.factor == 0 {
		opts.factor = 2
	}
	if opts.maxInterval == 0 {
		opts.maxInterval = 1 * time.Minute
	}
	if opts.attempts == 0 {
		return errors.New("retry attempts have to be set when not using custom backoff intervals")
	}
	interval := opts.initialInterval
	for i := 0; ; i++ {
		if i == 0 && opts.disableInitialExecution {
			select {
			case <-opts.cancel:
				return cancelErr
			case <-time.After(interval):
			}
			continue
		}
		shouldContinue, err = f(i)
		if !shouldContinue {
			return err
		}
		if opts.attempts > 0 && i >= opts.attempts-1 {
			break
		}
		select {
		case <-opts.cancel:
			return cancelErr
		case <-time.After(interval):
		}
		interval = time.Duration(float64(interval) * opts.factor)
		if interval >= opts.maxInterval {
			interval = opts.maxInterval
		}
	}
	return err
}
