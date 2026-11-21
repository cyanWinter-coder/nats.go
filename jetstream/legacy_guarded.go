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

// Guarded (revision/CAS) update helpers bridging the legacy
// JetStreamManager API and the context-based JetStream API.
//
// The JetStream stream/consumer management endpoints do not carry a
// server-enforced revision token the way KV does, so an update request alone
// is last-writer-wins. These helpers implement an explicit read-modify-write
// guard: the caller pins the revision observed in a previous Info call, and
// the helper re-reads the server state immediately before writing and refuses
// the update with ErrLegacyRevisionConflict if anything changed in the
// meantime. The write is never retried after a conflict, so a concurrent
// update can never be silently overwritten.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
)

// LegacyResourceRevision is an opaque fingerprint of a stream's or consumer's
// server-side configuration. It is produced by [StreamConfigRevision] /
// [ConsumerConfigRevision] and consumed by the GuardedUpdate options.
//
// The revision is content-addressed from the exact configuration JSON the
// server returned, so it is stable across processes and independent of which
// API was used to read the resource. Two revisions are equal if and only if
// the server-observed configurations are equal.
type LegacyResourceRevision string

// StreamConfigRevision computes the revision of the configuration embedded in
// info. Use the result with [GuardedLegacyStreamUpdate] or
// [GuardedStreamUpdate].
func StreamConfigRevision(info *StreamInfo) (LegacyResourceRevision, error) {
	if info == nil {
		return "", fmt.Errorf("%w: nil StreamInfo", ErrLegacyRevisionConflict)
	}
	return configRevision(info.Config)
}

// LegacyStreamConfigRevision computes the revision of a legacy stream info.
// It matches [StreamConfigRevision] for equivalent configurations.
func LegacyStreamConfigRevision(info *nats.StreamInfo) (LegacyResourceRevision, error) {
	if info == nil {
		return "", fmt.Errorf("%w: nil StreamInfo", ErrLegacyRevisionConflict)
	}
	return configRevision(ToStreamConfig(info.Config))
}

// ConsumerConfigRevision computes the revision of the configuration embedded
// in info. Use the result with [GuardedLegacyConsumerUpdate] /
// [GuardedConsumerUpdate].
func ConsumerConfigRevision(info *ConsumerInfo) (LegacyResourceRevision, error) {
	if info == nil {
		return "", fmt.Errorf("%w: nil ConsumerInfo", ErrLegacyRevisionConflict)
	}
	return configRevision(info.Config)
}

// LegacyConsumerConfigRevision computes the revision of a legacy consumer
// info. It matches [ConsumerConfigRevision] for equivalent configurations.
func LegacyConsumerConfigRevision(info *nats.ConsumerInfo) (LegacyResourceRevision, error) {
	if info == nil {
		return "", fmt.Errorf("%w: nil ConsumerInfo", ErrLegacyRevisionConflict)
	}
	return configRevision(ToConsumerConfig(info.Config))
}

// configRevision hashes the canonical JSON encoding of a configuration value.
// Marshaling through the package's own JSON tags means the legacy and
// context-based configs hash identically once converted between each other.
func configRevision(cfg any) (LegacyResourceRevision, error) {
	b, err := json.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("%w: cannot marshal config: %v", ErrLegacyRevisionConflict, err)
	}
	sum := sha256.Sum256(b)
	return LegacyResourceRevision(hex.EncodeToString(sum[:])), nil
}

// GuardedStreamUpdateOpt configures [GuardedStreamUpdate].
type GuardedStreamUpdateOpt func(*guardedStreamUpdateOpts) error

type guardedStreamUpdateOpts struct {
	expected LegacyResourceRevision
	hasGuard bool
}

// GuardedStreamExpectedRevision pins the revision the update is based on.
// Without it, [GuardedStreamUpdate] performs an ordinary update.
func GuardedStreamExpectedRevision(rev LegacyResourceRevision) GuardedStreamUpdateOpt {
	return func(o *guardedStreamUpdateOpts) error {
		o.expected = rev
		o.hasGuard = true
		return nil
	}
}

// GuardedConsumerUpdateOpt configures [GuardedConsumerUpdate].
type GuardedConsumerUpdateOpt func(*guardedConsumerUpdateOpts) error

type guardedConsumerUpdateOpts struct {
	expected LegacyResourceRevision
	hasGuard bool
}

// GuardedConsumerExpectedRevision pins the revision the update is based on.
// Without it, [GuardedConsumerUpdate] performs an ordinary update.
func GuardedConsumerExpectedRevision(rev LegacyResourceRevision) GuardedConsumerUpdateOpt {
	return func(o *guardedConsumerUpdateOpts) error {
		o.expected = rev
		o.hasGuard = true
		return nil
	}
}

// GuardedStreamUpdate updates a stream through the context-based API,
// refusing the write when the stream's current server-side revision no longer
// matches expected. The check and the write run without any client-side
// retry, so an [ErrLegacyRevisionConflict] always means the caller's base
// revision is stale and the server state was left untouched by this call.
//
// When no revision option is supplied it behaves exactly as
// JetStream.UpdateStream and remains compatible with the normal path.
func (js *jetStream) GuardedStreamUpdate(ctx context.Context, cfg StreamConfig, opts ...GuardedStreamUpdateOpt) (Stream, error) {
	var o guardedStreamUpdateOpts
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	if o.hasGuard {
		current, err := js.Stream(ctx, cfg.Name)
		if err != nil {
			return nil, err
		}
		curRev, err := StreamConfigRevision(current.CachedInfo())
		if err != nil {
			return nil, err
		}
		if curRev != o.expected {
			return nil, fmt.Errorf("%w: stream %q changed since the expected revision", ErrLegacyRevisionConflict, cfg.Name)
		}
	}
	return js.UpdateStream(ctx, cfg)
}

// GuardedConsumerUpdate updates a consumer through the context-based API using
// the create-or-update server action, refusing the write when the consumer's
// current server-side revision no longer matches expected. The consumer name
// is taken from cfg.Name or cfg.Durable, matching the legacy naming rules.
//
// When no revision option is supplied it behaves exactly as
// JetStream.CreateOrUpdateConsumer and remains compatible with the normal
// path.
func (js *jetStream) GuardedConsumerUpdate(ctx context.Context, streamName string, cfg ConsumerConfig, opts ...GuardedConsumerUpdateOpt) (Consumer, error) {
	var o guardedConsumerUpdateOpts
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	if o.hasGuard {
		consumerName := cfg.Name
		if consumerName == "" {
			consumerName = cfg.Durable
		}
		if consumerName == "" {
			return nil, fmt.Errorf("%w: consumer name or durable is required for a guarded update", ErrLegacyRevisionConflict)
		}
		current, err := js.Consumer(ctx, streamName, consumerName)
		if err != nil {
			return nil, err
		}
		info, err := current.Info(ctx)
		if err != nil {
			return nil, err
		}
		curRev, err := ConsumerConfigRevision(info)
		if err != nil {
			return nil, err
		}
		if curRev != o.expected {
			return nil, fmt.Errorf("%w: consumer %q on stream %q changed since the expected revision",
				ErrLegacyRevisionConflict, consumerName, streamName)
		}
	}
	return js.CreateOrUpdateConsumer(ctx, streamName, cfg)
}

// GuardedLegacyStreamUpdate updates a stream through a legacy
// [nats.JetStreamManager], applying the same revision guard as
// GuardedStreamUpdate. The new-API config is converted with
// [LegacyStreamConfig], so fields the legacy API cannot represent fail up
// front with [ErrLegacyFieldNotSupported] and never reach the server.
//
// The manager and the context-based [JetStream] handle may be backed by the
// same or different connections; the guard always compares against fresh
// server state.
func GuardedLegacyStreamUpdate(ctx context.Context, mgr nats.JetStreamManager, cfg StreamConfig, opts ...GuardedStreamUpdateOpt) (*nats.StreamInfo, error) {
	legacyCfg, err := LegacyStreamConfig(cfg)
	if err != nil {
		return nil, err
	}
	var o guardedStreamUpdateOpts
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	if o.hasGuard {
		current, err := mgr.StreamInfo(cfg.Name, nats.Context(ctx))
		if err != nil {
			return nil, err
		}
		curRev, err := LegacyStreamConfigRevision(current)
		if err != nil {
			return nil, err
		}
		if curRev != o.expected {
			return nil, fmt.Errorf("%w: stream %q changed since the expected revision", ErrLegacyRevisionConflict, cfg.Name)
		}
	}
	return mgr.UpdateStream(&legacyCfg, nats.Context(ctx))
}

// GuardedLegacyConsumerUpdate updates a consumer through a legacy
// [nats.JetStreamManager], applying the same revision guard as
// GuardedConsumerUpdate. The new-API config is converted with
// [LegacyConsumerConfig], so fields the legacy API cannot represent fail up
// front with [ErrLegacyFieldNotSupported] and never reach the server.
func GuardedLegacyConsumerUpdate(ctx context.Context, mgr nats.JetStreamManager, streamName string, cfg ConsumerConfig, opts ...GuardedConsumerUpdateOpt) (*nats.ConsumerInfo, error) {
	legacyCfg, err := LegacyConsumerConfig(cfg)
	if err != nil {
		return nil, err
	}
	var o guardedConsumerUpdateOpts
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	if o.hasGuard {
		consumerName := cfg.Name
		if consumerName == "" {
			consumerName = cfg.Durable
		}
		if consumerName == "" {
			return nil, fmt.Errorf("%w: consumer name or durable is required for a guarded update", ErrLegacyRevisionConflict)
		}
		current, err := mgr.ConsumerInfo(streamName, consumerName, nats.Context(ctx))
		if err != nil {
			return nil, err
		}
		curRev, err := LegacyConsumerConfigRevision(current)
		if err != nil {
			return nil, err
		}
		if curRev != o.expected {
			return nil, fmt.Errorf("%w: consumer %q on stream %q changed since the expected revision",
				ErrLegacyRevisionConflict, consumerName, streamName)
		}
	}
	return mgr.UpdateConsumer(streamName, &legacyCfg, nats.Context(ctx))
}
