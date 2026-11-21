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
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// fullStreamConfig exercises every field representable in both APIs.
func fullStreamConfig(name string) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:                 name,
		Description:          "compat stream",
		Subjects:             []string{"EVENTS.>", "OTHER.*"},
		Retention:            jetstream.InterestPolicy,
		MaxConsumers:         7,
		MaxMsgs:              1000,
		MaxBytes:             1 << 20,
		Discard:              jetstream.DiscardNew,
		DiscardNewPerSubject: true,
		MaxAge:               time.Hour,
		MaxMsgsPerSubject:    42,
		MaxMsgSize:           4096,
		Storage:              jetstream.MemoryStorage,
		Replicas:             1,
		NoAck:                false,
		Duplicates:           2 * time.Minute,
		Placement:            &jetstream.Placement{Cluster: "test-cluster", Tags: []string{"t1", "t2"}},
		Sealed:               false,
		DenyDelete:           false,
		DenyPurge:            false,
		AllowRollup:          false,
		Compression:          jetstream.S2Compression,
		FirstSeq:             0,
		SubjectTransform: &jetstream.SubjectTransformConfig{
			Source:      "EVENTS.>",
			Destination: "OUT.>",
		},
		RePublish: &jetstream.RePublish{
			Source:      "EVENTS.>",
			Destination: "REP.>",
			HeadersOnly: true,
		},
		AllowDirect:            true,
		MirrorDirect:           false,
		ConsumerLimits:         jetstream.StreamConsumerLimits{InactiveThreshold: time.Minute, MaxAckPending: 123},
		Metadata:               map[string]string{"k1": "v1", "k2": "v2"},
		AllowMsgTTL:            true,
		SubjectDeleteMarkerTTL: 30 * time.Second,
	}
}

// fullConsumerConfig exercises every field representable in both APIs.
func fullConsumerConfig(durable string) jetstream.ConsumerConfig {
	start := time.Unix(1700000000, 0).UTC()
	return jetstream.ConsumerConfig{
		Durable:            durable,
		Name:               durable,
		Description:        "compat consumer",
		DeliverPolicy:      jetstream.DeliverByStartTimePolicy,
		OptStartTime:       &start,
		AckPolicy:          jetstream.AckExplicitPolicy,
		AckWait:            45 * time.Second,
		MaxDeliver:         5,
		BackOff:            []time.Duration{time.Second, 5 * time.Second},
		FilterSubject:      "EVENTS.A",
		ReplayPolicy:       jetstream.ReplayOriginalPolicy,
		SampleFrequency:    "50",
		MaxWaiting:         32,
		MaxAckPending:      64,
		FlowControl:        false,
		HeadersOnly:        true,
		MaxRequestBatch:    100,
		MaxRequestExpires:  10 * time.Second,
		MaxRequestMaxBytes: 1 << 16,
		InactiveThreshold:  30 * time.Second,
		Replicas:           1,
		MemoryStorage:      true,
		Metadata:           map[string]string{"ck": "cv"},
	}
}

// TestLegacyCompat_StreamConfigRoundTrip verifies requirement 5 for fields
// both APIs understand: a full config converts to the legacy API and back
// without any data loss, and serializes to identical JSON.
func TestLegacyCompat_StreamConfigRoundTrip(t *testing.T) {
	cfg := fullStreamConfig("ROUNDTRIP_S")
	cfg.Subjects = []string{"EVENTS.>"} // transform requires a single wildcard

	legacy, err := jetstream.LegacyStreamConfig(cfg)
	if err != nil {
		t.Fatalf("LegacyStreamConfig: %v", err)
	}
	back := jetstream.ToStreamConfig(legacy)

	newJSON, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal new cfg: %v", err)
	}
	backJSON, err := json.Marshal(back)
	if err != nil {
		t.Fatalf("marshal converted cfg: %v", err)
	}
	if string(newJSON) != string(backJSON) {
		t.Fatalf("stream config round trip changed JSON\nwant: %s\n got: %s", newJSON, backJSON)
	}
}

// TestLegacyCompat_ConsumerConfigRoundTrip does the same for consumers.
func TestLegacyCompat_ConsumerConfigRoundTrip(t *testing.T) {
	cfg := fullConsumerConfig("ROUNDTRIP_C")

	legacy, err := jetstream.LegacyConsumerConfig(cfg)
	if err != nil {
		t.Fatalf("LegacyConsumerConfig: %v", err)
	}
	back := jetstream.ToConsumerConfig(legacy)

	newJSON, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal new cfg: %v", err)
	}
	backJSON, err := json.Marshal(back)
	if err != nil {
		t.Fatalf("marshal converted cfg: %v", err)
	}
	if string(newJSON) != string(backJSON) {
		t.Fatalf("consumer config round trip changed JSON\nwant: %s\n got: %s", newJSON, backJSON)
	}
}

// TestLegacyCompat_UnsupportedStreamFieldsFail verifies requirement 5: fields
// the legacy API cannot represent are rejected explicitly rather than silently
// dropped.
func TestLegacyCompat_UnsupportedStreamFieldsFail(t *testing.T) {
	base := fullStreamConfig("UNSUP_S")
	base.Subjects = []string{"EVENTS.>"}

	cases := []struct {
		name   string
		mutate func(*jetstream.StreamConfig)
		field  string
	}{
		{name: "AllowMsgCounter", field: "AllowMsgCounter", mutate: func(c *jetstream.StreamConfig) { c.AllowMsgCounter = true }},
		{name: "AllowAtomicPublish", field: "AllowAtomicPublish", mutate: func(c *jetstream.StreamConfig) { c.AllowAtomicPublish = true }},
		{name: "AllowMsgSchedules", field: "AllowMsgSchedules", mutate: func(c *jetstream.StreamConfig) { c.AllowMsgSchedules = true }},
		{name: "PersistMode", field: "PersistMode", mutate: func(c *jetstream.StreamConfig) { c.PersistMode = jetstream.AsyncPersistMode }},
		{name: "AllowBatchPublish", field: "AllowBatchPublish", mutate: func(c *jetstream.StreamConfig) { c.AllowBatchPublish = true }},
		{name: "Mirror.Consumer", field: "Mirror.Consumer", mutate: func(c *jetstream.StreamConfig) {
			c.Mirror = &jetstream.StreamSource{Name: "OTHER", Consumer: &jetstream.StreamConsumerSource{Name: "DUR"}}
			c.Subjects = nil
		}},
		{name: "Sources.Consumer", field: "Sources[0].Consumer", mutate: func(c *jetstream.StreamConfig) {
			c.Sources = []*jetstream.StreamSource{{Name: "OTHER", Consumer: &jetstream.StreamConsumerSource{Name: "DUR"}}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mutate(&cfg)
			_, err := jetstream.LegacyStreamConfig(cfg)
			if !errors.Is(err, jetstream.ErrLegacyFieldNotSupported) {
				t.Fatalf("expected ErrLegacyFieldNotSupported for %s, got: %v", tc.field, err)
			}
			if !errors.Is(err, jetstream.ErrLegacyFieldNotSupported) ||
				jetstream.LegacyErrorCategoryOf(err) != jetstream.LegacyErrorCategoryUnsupportedField {
				t.Fatalf("wrong error category for %s: %v", tc.field, err)
			}
		})
	}
}

// TestLegacyCompat_UnsupportedConsumerFieldsFail verifies requirement 5 for
// the consumer-only fields the legacy API cannot represent.
func TestLegacyCompat_UnsupportedConsumerFieldsFail(t *testing.T) {
	pauseUntil := time.Now().Add(time.Hour).UTC()
	cases := []struct {
		name   string
		mutate func(*jetstream.ConsumerConfig)
	}{
		{name: "PauseUntil", mutate: func(c *jetstream.ConsumerConfig) { c.PauseUntil = &pauseUntil }},
		{name: "PriorityPolicy", mutate: func(c *jetstream.ConsumerConfig) { c.PriorityPolicy = jetstream.PriorityPolicyPinned }},
		{name: "PinnedTTL", mutate: func(c *jetstream.ConsumerConfig) { c.PinnedTTL = time.Minute }},
		{name: "PriorityGroups", mutate: func(c *jetstream.ConsumerConfig) { c.PriorityGroups = []string{"g1"} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := fullConsumerConfig("UNSUP_C_" + tc.name)
			tc.mutate(&cfg)
			_, err := jetstream.LegacyConsumerConfig(cfg)
			if !errors.Is(err, jetstream.ErrLegacyFieldNotSupported) {
				t.Fatalf("expected ErrLegacyFieldNotSupported, got: %v", err)
			}
		})
	}
}

// TestLegacyCompat_ConcurrentReadEquivalence verifies requirement 1:
// concurrent reads through both APIs return equivalent configs, sequences and
// error categories for the same stream/consumer.
func TestLegacyCompat_ConcurrentReadEquivalence(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
		ctx := newTesterCtx(t, 10*time.Second)
		mgr, err := nc.JetStream()
		if err != nil {
			t.Fatalf("JetStreamManager: %v", err)
		}

		sCfg := fullStreamConfig("READ_EQ_S")
		sCfg.Subjects = []string{"EVENTS.>"}
		s, err := js.CreateStream(ctx, sCfg)
		if err != nil {
			t.Fatalf("CreateStream: %v", err)
		}
		if _, err := js.CreateOrUpdateConsumer(ctx, sCfg.Name, fullConsumerConfig("READ_EQ_C")); err != nil {
			t.Fatalf("CreateOrUpdateConsumer: %v", err)
		}

		// Publish some messages so state sequences are non-trivial.
		for i := 0; i < 5; i++ {
			if _, err := js.Publish(ctx, "EVENTS.A", []byte(fmt.Sprintf("m%d", i))); err != nil {
				t.Fatalf("Publish: %v", err)
			}
		}

		const goroutines = 8
		var wg sync.WaitGroup
		errCh := make(chan error, goroutines*2)
		for i := 0; i < goroutines; i++ {
			wg.Add(2)
			go func() {
				defer wg.Done()
				info, err := s.Info(ctx)
				if err != nil {
					errCh <- fmt.Errorf("new API Stream.Info: %w", err)
					return
				}
				legacyInfo, lerr := mgr.StreamInfo("READ_EQ_S", nats.Context(ctx))
				if lerr != nil {
					errCh <- fmt.Errorf("legacy StreamInfo: %w", lerr)
					return
				}
				if info.State.LastSeq != legacyInfo.State.LastSeq ||
					info.State.Msgs != legacyInfo.State.Msgs ||
					info.State.FirstSeq != legacyInfo.State.FirstSeq {
					errCh <- fmt.Errorf("stream state mismatch: new=%+v legacy=%+v", info.State, legacyInfo.State)
					return
				}
				converted, cerr := jetstream.LegacyStreamInfo(info)
				if cerr != nil {
					errCh <- cerr
					return
				}
				if converted.Config.Name != legacyInfo.Config.Name ||
					converted.Config.Description != legacyInfo.Config.Description ||
					converted.Config.Compression != legacyInfo.Config.Compression {
					errCh <- fmt.Errorf("stream config mismatch after conversion")
				}
			}()
			go func() {
				defer wg.Done()
				cons, err := js.Consumer(ctx, "READ_EQ_S", "READ_EQ_C")
				if err != nil {
					errCh <- fmt.Errorf("new API Consumer: %w", err)
					return
				}
				info, err := cons.Info(ctx)
				if err != nil {
					errCh <- fmt.Errorf("new API Consumer.Info: %w", err)
					return
				}
				legacyInfo, lerr := mgr.ConsumerInfo("READ_EQ_S", "READ_EQ_C", nats.Context(ctx))
				if lerr != nil {
					errCh <- fmt.Errorf("legacy ConsumerInfo: %w", lerr)
					return
				}
				if info.Name != legacyInfo.Name || info.Stream != legacyInfo.Stream ||
					info.Config.Durable != legacyInfo.Config.Durable ||
					info.Config.AckWait != legacyInfo.Config.AckWait {
					errCh <- fmt.Errorf("consumer info mismatch: new=%+v legacy=%+v", info.Config, legacyInfo.Config)
				}
				converted, cerr := jetstream.LegacyConsumerInfo(info)
				if cerr != nil {
					errCh <- cerr
					return
				}
				if converted.Config.FilterSubject != legacyInfo.Config.FilterSubject {
					errCh <- fmt.Errorf("consumer config mismatch after conversion")
				}
			}()
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			t.Error(err)
		}
	})
}

// TestLegacyCompat_ErrorCategoryEquivalence verifies requirement 1 for error
// categories: the same server-side failure surfaces an equivalent category
// regardless of which API triggered it.
func TestLegacyCompat_ErrorCategoryEquivalence(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
		ctx := newTesterCtx(t, 10*time.Second)
		mgr, err := nc.JetStream()
		if err != nil {
			t.Fatalf("JetStreamManager: %v", err)
		}

		// Stream not found through both APIs.
		_, newErr := js.Stream(ctx, "MISSING_S")
		_, legacyErr := mgr.StreamInfo("MISSING_S", nats.Context(ctx))
		if newErr == nil || legacyErr == nil {
			t.Fatalf("expected not-found errors, got new=%v legacy=%v", newErr, legacyErr)
		}
		if !errors.Is(newErr, jetstream.ErrStreamNotFound) || !errors.Is(legacyErr, nats.ErrStreamNotFound) {
			t.Fatalf("expected stream-not-found sentinels, got new=%v legacy=%v", newErr, legacyErr)
		}
		if cat := jetstream.LegacyErrorCategoryOf(newErr); cat != jetstream.LegacyErrorCategoryStreamNotFound {
			t.Fatalf("new API category: %s", cat)
		}
		if cat := jetstream.LegacyErrorCategoryOf(legacyErr); cat != jetstream.LegacyErrorCategoryStreamNotFound {
			t.Fatalf("legacy API category: %s", cat)
		}
		if !jetstream.EquivalentLegacyError(newErr, legacyErr) {
			t.Fatalf("errors should be equivalent: %v vs %v", newErr, legacyErr)
		}

		// Consumer not found through both APIs.
		_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: "CAT_S", Subjects: []string{"CAT.>"}})
		if err != nil {
			t.Fatalf("CreateStream: %v", err)
		}
		_, newCErr := js.Consumer(ctx, "CAT_S", "MISSING_C")
		_, legacyCErr := mgr.ConsumerInfo("CAT_S", "MISSING_C", nats.Context(ctx))
		if !jetstream.EquivalentLegacyError(newCErr, legacyCErr) {
			t.Fatalf("consumer errors should be equivalent: %v vs %v", newCErr, legacyCErr)
		}
		if jetstream.LegacyErrorCategoryOf(newCErr) != jetstream.LegacyErrorCategoryConsumerNotFound {
			t.Fatalf("wrong consumer category: %v", newCErr)
		}

		// Nil handling and non-matching categories.
		if !jetstream.EquivalentLegacyError(nil, nil) {
			t.Fatal("nil errors should be equivalent")
		}
		if jetstream.EquivalentLegacyError(jetstream.ErrStreamNotFound, jetstream.ErrConsumerNotFound) {
			t.Fatal("different categories must not be equivalent")
		}
		if jetstream.EquivalentLegacyError(errors.New("network"), errors.New("network")) {
			t.Fatal("plain errors without a JetStream category must not be equivalent")
		}
	})
}

// TestLegacyCompat_LegacyUpdateRefreshesNewAPICache verifies requirement 2:
// after the legacy API updates a stream/consumer, the context-based API's
// cached handle cannot keep observing the old snapshot.
func TestLegacyCompat_LegacyUpdateRefreshesNewAPICache(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
		ctx := newTesterCtx(t, 15*time.Second)
		mgr, err := nc.JetStream()
		if err != nil {
			t.Fatalf("JetStreamManager: %v", err)
		}
		if err := jetstream.BindLegacyManager(js, mgr); err != nil {
			t.Fatalf("BindLegacyManager: %v", err)
		}

		s, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name:        "CACHE_S",
			Subjects:    []string{"CACHE.>"},
			Description: "before",
		})
		if err != nil {
			t.Fatalf("CreateStream: %v", err)
		}
		cons, err := js.CreateOrUpdateConsumer(ctx, "CACHE_S", jetstream.ConsumerConfig{
			Name:        "CACHE_C",
			Durable:     "CACHE_C",
			Description: "before",
			AckPolicy:   jetstream.AckExplicitPolicy,
		})
		if err != nil {
			t.Fatalf("CreateOrUpdateConsumer: %v", err)
		}

		// Prime the caches.
		if got := s.CachedInfo().Config.Description; got != "before" {
			t.Fatalf("unexpected initial stream cache: %q", got)
		}
		if got := cons.CachedInfo().Config.Description; got != "before" {
			t.Fatalf("unexpected initial consumer cache: %q", got)
		}

		// Update through the legacy API.
		sInfo, err := mgr.StreamInfo("CACHE_S", nats.Context(ctx))
		if err != nil {
			t.Fatalf("StreamInfo: %v", err)
		}
		sInfo.Config.Description = "after-legacy"
		if _, err := mgr.UpdateStream(&sInfo.Config, nats.Context(ctx)); err != nil {
			t.Fatalf("legacy UpdateStream: %v", err)
		}
		cInfo, err := mgr.ConsumerInfo("CACHE_S", "CACHE_C", nats.Context(ctx))
		if err != nil {
			t.Fatalf("ConsumerInfo: %v", err)
		}
		cInfo.Config.Description = "after-legacy"
		if _, err := mgr.UpdateConsumer("CACHE_S", &cInfo.Config, nats.Context(ctx)); err != nil {
			t.Fatalf("legacy UpdateConsumer: %v", err)
		}

		// The guarded cache must transparently reflect the legacy updates.
		checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
			if got := s.CachedInfo().Config.Description; got != "after-legacy" {
				return fmt.Errorf("stale stream cache: %q", got)
			}
			return nil
		})
		if got := cons.CachedInfo().Config.Description; got != "after-legacy" {
			t.Fatalf("stale consumer cache: %q", got)
		}

		// A fresh handle bound to the same JetStream instance inherits the guard.
		s2, err := js.Stream(ctx, "CACHE_S")
		if err != nil {
			t.Fatalf("Stream: %v", err)
		}
		if got := s2.CachedInfo().Config.Description; got != "after-legacy" {
			t.Fatalf("fresh handle did not inherit guard: %q", got)
		}
	})
}

// TestLegacyCompat_NewAPIUpdateVisibleToLegacy verifies the other direction of
// requirement 2: the legacy API always reads server state and therefore sees
// updates made through the context-based API immediately.
func TestLegacyCompat_NewAPIUpdateVisibleToLegacy(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
		ctx := newTesterCtx(t, 10*time.Second)
		mgr, err := nc.JetStream()
		if err != nil {
			t.Fatalf("JetStreamManager: %v", err)
		}

		s, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name: "VIS_S", Subjects: []string{"VIS.>"}, Description: "v1",
		})
		if err != nil {
			t.Fatalf("CreateStream: %v", err)
		}
		cfg := s.CachedInfo().Config
		cfg.Description = "v2-new-api"
		if _, err := js.UpdateStream(ctx, cfg); err != nil {
			t.Fatalf("UpdateStream: %v", err)
		}
		legacyInfo, err := mgr.StreamInfo("VIS_S", nats.Context(ctx))
		if err != nil {
			t.Fatalf("legacy StreamInfo: %v", err)
		}
		if legacyInfo.Config.Description != "v2-new-api" {
			t.Fatalf("legacy API observed stale description: %q", legacyInfo.Config.Description)
		}
	})
}

// TestLegacyCompat_GuardedUpdateConflict verifies requirement 3: a guarded
// update based on a stale revision is rejected without writing, while a
// current revision updates successfully.
func TestLegacyCompat_GuardedUpdateConflict(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
		ctx := newTesterCtx(t, 15*time.Second)
		mgr, err := nc.JetStream()
		if err != nil {
			t.Fatalf("JetStreamManager: %v", err)
		}

		s, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name: "GUARD_S", Subjects: []string{"GUARD.>"}, Description: "base", MaxMsgs: 10,
		})
		if err != nil {
			t.Fatalf("CreateStream: %v", err)
		}
		baseInfo, err := s.Info(ctx)
		if err != nil {
			t.Fatalf("Info: %v", err)
		}
		rev, err := jetstream.StreamConfigRevision(baseInfo)
		if err != nil {
			t.Fatalf("StreamConfigRevision: %v", err)
		}

		// Concurrent change through the legacy API.
		legacyInfo, err := mgr.StreamInfo("GUARD_S", nats.Context(ctx))
		if err != nil {
			t.Fatalf("StreamInfo: %v", err)
		}
		legacyInfo.Config.MaxMsgs = 20
		if _, err := mgr.UpdateStream(&legacyInfo.Config, nats.Context(ctx)); err != nil {
			t.Fatalf("legacy UpdateStream: %v", err)
		}

		// Stale revision must be rejected and must not overwrite the change.
		staleCfg := baseInfo.Config
		staleCfg.Description = "should-not-land"
		if _, err := js.GuardedStreamUpdate(ctx, staleCfg, jetstream.GuardedStreamExpectedRevision(rev)); !errors.Is(err, jetstream.ErrLegacyRevisionConflict) {
			t.Fatalf("expected ErrLegacyRevisionConflict, got: %v", err)
		}
		checkInfo, err := mgr.StreamInfo("GUARD_S", nats.Context(ctx))
		if err != nil {
			t.Fatalf("StreamInfo: %v", err)
		}
		if checkInfo.Config.MaxMsgs != 20 {
			t.Fatalf("guarded update overwrote concurrent change: %+v", checkInfo.Config)
		}
		if checkInfo.Config.Description != "base" {
			t.Fatalf("rejected update leaked a write: description=%q", checkInfo.Config.Description)
		}

		// Re-basing on the fresh revision succeeds.
		freshRev, err := jetstream.LegacyStreamConfigRevision(checkInfo)
		if err != nil {
			t.Fatalf("LegacyStreamConfigRevision: %v", err)
		}
		freshCfg := jetstream.ToStreamConfig(checkInfo.Config)
		freshCfg.Description = "guarded-ok"
		if _, err := jetstream.GuardedLegacyStreamUpdate(ctx, mgr, freshCfg,
			jetstream.GuardedStreamExpectedRevision(freshRev)); err != nil {
			t.Fatalf("GuardedLegacyStreamUpdate: %v", err)
		}
		after, err := mgr.StreamInfo("GUARD_S", nats.Context(ctx))
		if err != nil {
			t.Fatalf("StreamInfo: %v", err)
		}
		if after.Config.Description != "guarded-ok" || after.Config.MaxMsgs != 20 {
			t.Fatalf("guarded update did not preserve all fields: %+v", after.Config)
		}
	})
}

// TestLegacyCompat_GuardedConsumerConflict exercises the same revision/CAS
// guarantee on consumers, across both APIs.
func TestLegacyCompat_GuardedConsumerConflict(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
		ctx := newTesterCtx(t, 15*time.Second)
		mgr, err := nc.JetStream()
		if err != nil {
			t.Fatalf("JetStreamManager: %v", err)
		}
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "GC_S", Subjects: []string{"GC.>"}}); err != nil {
			t.Fatalf("CreateStream: %v", err)
		}
		cons, err := js.CreateOrUpdateConsumer(ctx, "GC_S", jetstream.ConsumerConfig{
			Name: "GC_C", Durable: "GC_C", AckPolicy: jetstream.AckExplicitPolicy, MaxAckPending: 100,
		})
		if err != nil {
			t.Fatalf("CreateOrUpdateConsumer: %v", err)
		}
		info, err := cons.Info(ctx)
		if err != nil {
			t.Fatalf("Info: %v", err)
		}
		rev, err := jetstream.ConsumerConfigRevision(info)
		if err != nil {
			t.Fatalf("ConsumerConfigRevision: %v", err)
		}

		// Concurrent legacy update.
		legacyInfo, err := mgr.ConsumerInfo("GC_S", "GC_C", nats.Context(ctx))
		if err != nil {
			t.Fatalf("ConsumerInfo: %v", err)
		}
		legacyInfo.Config.MaxAckPending = 200
		if _, err := mgr.UpdateConsumer("GC_S", &legacyInfo.Config, nats.Context(ctx)); err != nil {
			t.Fatalf("legacy UpdateConsumer: %v", err)
		}

		// Stale guard through the new API.
		stale := info.Config
		stale.Description = "nope"
		_, err = js.GuardedConsumerUpdate(ctx, "GC_S", stale, jetstream.GuardedConsumerExpectedRevision(rev))
		if !errors.Is(err, jetstream.ErrLegacyRevisionConflict) {
			t.Fatalf("expected conflict, got: %v", err)
		}

		// Fresh guard through the legacy bridge.
		cur, err := mgr.ConsumerInfo("GC_S", "GC_C", nats.Context(ctx))
		if err != nil {
			t.Fatalf("ConsumerInfo: %v", err)
		}
		if cur.Config.MaxAckPending != 200 {
			t.Fatalf("concurrent change lost: %+v", cur.Config)
		}
		freshRev, err := jetstream.LegacyConsumerConfigRevision(cur)
		if err != nil {
			t.Fatalf("LegacyConsumerConfigRevision: %v", err)
		}
		freshCfg := jetstream.ToConsumerConfig(cur.Config)
		freshCfg.Description = "guarded-consumer"
		if _, err := jetstream.GuardedLegacyConsumerUpdate(ctx, mgr, "GC_S", freshCfg,
			jetstream.GuardedConsumerExpectedRevision(freshRev)); err != nil {
			t.Fatalf("GuardedLegacyConsumerUpdate: %v", err)
		}
		after, err := mgr.ConsumerInfo("GC_S", "GC_C", nats.Context(ctx))
		if err != nil {
			t.Fatalf("ConsumerInfo: %v", err)
		}
		if after.Config.Description != "guarded-consumer" || after.Config.MaxAckPending != 200 {
			t.Fatalf("guarded consumer update lost fields: %+v", after.Config)
		}
	})
}

// TestLegacyCompat_PreFlightCancellation verifies the first half of
// requirement 4: a context canceled before the request is sent returns the
// plain context error, not the post-dispatch sentinel.
func TestLegacyCompat_PreFlightCancellation(t *testing.T) {
	withJSServer(t, func(t *testing.T, _ *nats.Conn, js jetstream.JetStream) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // canceled before any call

		_, err := js.Stream(ctx, "PRE_S")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got: %v", err)
		}
		if errors.Is(err, jetstream.ErrRequestCanceledAfterDispatch) {
			t.Fatalf("pre-flight cancellation must not look like post-dispatch: %v", err)
		}
	})
}

// TestLegacyCompat_PostDispatchCancellation verifies the second half of
// requirement 4: when the context is canceled after the request left the
// client, the caller gets a distinguishable ErrRequestCanceledAfterDispatch
// (while context.Canceled still matches for backwards compatibility) and can
// re-fetch the resource to determine whether the write landed.
func TestLegacyCompat_PostDispatchCancellation(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
		ctx := newTesterCtx(t, 15*time.Second)
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name: "CANCEL_S", Subjects: []string{"CANCEL.>"}, Description: "orig",
		}); err != nil {
			t.Fatalf("CreateStream: %v", err)
		}

		// A JetStream handle whose API prefix points at a sink that accepts
		// requests but never answers. Every request is fully dispatched onto the
		// connection (the sink subscription confirms receipt), so canceling the
		// wait exercises the post-dispatch path; the missing response also makes
		// the server-side outcome deterministic (the write cannot reach the
		// real JetStream API).
		received := make(chan struct{}, 4)
		sink, err := nc.Subscribe("$JS.NO_RESPONDER.API.>", func(*nats.Msg) {
			select {
			case received <- struct{}{}:
			default:
			}
		})
		if err != nil {
			t.Fatalf("sink subscribe: %v", err)
		}
		defer sink.Unsubscribe()
		deadJS, err := jetstream.NewWithAPIPrefix(nc, "$JS.NO_RESPONDER.API.")
		if err != nil {
			t.Fatalf("NewWithAPIPrefix: %v", err)
		}

		updateCtx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, uerr := deadJS.UpdateStream(updateCtx, jetstream.StreamConfig{
				Name: "CANCEL_S", Subjects: []string{"CANCEL.>"}, Description: "canceled-write",
			})
			done <- uerr
		}()

		// Wait until the request actually left the client, then cancel.
		select {
		case <-received:
		case <-time.After(5 * time.Second):
			t.Fatal("update request was never dispatched")
		}
		cancel()

		select {
		case uerr := <-done:
			if !errors.Is(uerr, jetstream.ErrRequestCanceledAfterDispatch) {
				t.Fatalf("expected ErrRequestCanceledAfterDispatch, got: %v", uerr)
			}
			// Backwards-compatible context matching.
			if !errors.Is(uerr, context.Canceled) {
				t.Fatalf("error must still match context.Canceled, got: %v", uerr)
			}
			if jetstream.LegacyErrorCategoryOf(uerr) != jetstream.LegacyErrorCategoryCanceledAfterDispatch {
				t.Fatalf("wrong category for %v", uerr)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("update did not return after cancellation")
		}

		// The distinguishable state lets the caller re-fetch through the working
		// handle to learn the actual outcome: the inert-prefix write never
		// reached the JS API, so the server state is unchanged.
		info, err := js.Stream(newTesterCtx(t, 5*time.Second), "CANCEL_S")
		if err != nil {
			t.Fatalf("re-fetch after cancel: %v", err)
		}
		if got := info.CachedInfo().Config.Description; got != "orig" {
			t.Fatalf("server state unexpectedly changed after canceled request: %q", got)
		}

		// DeadlineExceeded must be classified the same way once a request is in
		// flight.
		deadlineCtx, deadlineCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer deadlineCancel()
		_, derr := deadJS.Stream(deadlineCtx, "CANCEL_S")
		if !errors.Is(derr, jetstream.ErrRequestCanceledAfterDispatch) {
			t.Fatalf("expected post-dispatch sentinel on deadline, got: %v", derr)
		}
		if !errors.Is(derr, context.DeadlineExceeded) {
			t.Fatalf("error must still match context.DeadlineExceeded, got: %v", derr)
		}
	})
}

// TestLegacyCompat_ConcurrentCrossAPIUpdates verifies requirement 6's
// concurrency scenario: many updates from both APIs serialize through the
// server, every observed revision is one of the writes actually committed, and
// guarded updates never clobber a committed concurrent write.
func TestLegacyCompat_ConcurrentCrossAPIUpdates(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
		ctx := newTesterCtx(t, 30*time.Second)
		mgr, err := nc.JetStream()
		if err != nil {
			t.Fatalf("JetStreamManager: %v", err)
		}
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name: "RACE_S", Subjects: []string{"RACE.>"}, Description: "d0",
		}); err != nil {
			t.Fatalf("CreateStream: %v", err)
		}

		const writers = 6
		var wg sync.WaitGroup
		errCh := make(chan error, writers)
		for w := 0; w < writers; w++ {
			wg.Add(2)
			go func(id int) {
				defer wg.Done()
				for i := 0; i < 5; i++ {
					info, err := js.Stream(ctx, "RACE_S")
					if err != nil {
						errCh <- err
						return
					}
					cfg := info.CachedInfo().Config
					cfg.Description = fmt.Sprintf("new-%d-%d", id, i)
					if _, err := js.UpdateStream(ctx, cfg); err != nil {
						errCh <- err
						return
					}
				}
			}(w)
			go func(id int) {
				defer wg.Done()
				for i := 0; i < 5; i++ {
					info, err := mgr.StreamInfo("RACE_S", nats.Context(ctx))
					if err != nil {
						errCh <- err
						return
					}
					info.Config.Description = fmt.Sprintf("legacy-%d-%d", id, i)
					if _, err := mgr.UpdateStream(&info.Config, nats.Context(ctx)); err != nil {
						errCh <- err
						return
					}
				}
			}(w)
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			t.Error(err)
		}

		final, err := mgr.StreamInfo("RACE_S", nats.Context(ctx))
		if err != nil {
			t.Fatalf("final StreamInfo: %v", err)
		}
		// Whatever won the race, both APIs must agree on the final snapshot.
		newFinal, err := js.Stream(ctx, "RACE_S")
		if err != nil {
			t.Fatalf("final Stream: %v", err)
		}
		if newFinal.CachedInfo().Config.Description != final.Config.Description {
			t.Fatalf("final descriptions disagree: new=%q legacy=%q",
				newFinal.CachedInfo().Config.Description, final.Config.Description)
		}
		// A revision computed from either API must match for the same state.
		r1, err := jetstream.StreamConfigRevision(newFinal.CachedInfo())
		if err != nil {
			t.Fatal(err)
		}
		r2, err := jetstream.LegacyStreamConfigRevision(final)
		if err != nil {
			t.Fatal(err)
		}
		if r1 != r2 {
			t.Fatal("cross-API revisions of the same server state must match")
		}
	})
}

// TestLegacyCompat_InfoConversion verifies requirement 1 at the full-info
// level: StreamInfo/ConsumerInfo converted between the APIs carry equivalent
// state sequences and configs.
func TestLegacyCompat_InfoConversion(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
		ctx := newTesterCtx(t, 10*time.Second)
		mgr, err := nc.JetStream()
		if err != nil {
			t.Fatalf("JetStreamManager: %v", err)
		}
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name: "INFO_S", Subjects: []string{"INFO.>"}, Description: "info",
		}); err != nil {
			t.Fatalf("CreateStream: %v", err)
		}
		if _, err := js.CreateOrUpdateConsumer(ctx, "INFO_S", jetstream.ConsumerConfig{
			Name: "INFO_C", Durable: "INFO_C", AckPolicy: jetstream.AckExplicitPolicy,
		}); err != nil {
			t.Fatalf("CreateOrUpdateConsumer: %v", err)
		}
		for i := 0; i < 3; i++ {
			if _, err := js.Publish(ctx, "INFO.A", []byte("x")); err != nil {
				t.Fatalf("Publish: %v", err)
			}
		}

		newStream, err := js.Stream(ctx, "INFO_S")
		if err != nil {
			t.Fatal(err)
		}
		newSInfo := newStream.CachedInfo()
		legacySInfo, err := mgr.StreamInfo("INFO_S", nats.Context(ctx))
		if err != nil {
			t.Fatal(err)
		}
		convSInfo, err := jetstream.LegacyStreamInfo(newSInfo)
		if err != nil {
			t.Fatalf("LegacyStreamInfo: %v", err)
		}
		if convSInfo.State.Msgs != legacySInfo.State.Msgs ||
			convSInfo.State.LastSeq != legacySInfo.State.LastSeq ||
			convSInfo.Config.Subjects[0] != legacySInfo.Config.Subjects[0] {
			t.Fatalf("converted stream info diverged: %+v vs %+v", convSInfo, legacySInfo)
		}
		// Reverse direction.
		back := jetstream.ToStreamInfo(legacySInfo)
		if back.State.Msgs != newSInfo.State.Msgs || back.Config.Name != newSInfo.Config.Name {
			t.Fatalf("ToStreamInfo diverged: %+v vs %+v", back, newSInfo)
		}

		cons, err := js.Consumer(ctx, "INFO_S", "INFO_C")
		if err != nil {
			t.Fatal(err)
		}
		newCInfo, err := cons.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		legacyCInfo, err := mgr.ConsumerInfo("INFO_S", "INFO_C", nats.Context(ctx))
		if err != nil {
			t.Fatal(err)
		}
		convCInfo, err := jetstream.LegacyConsumerInfo(newCInfo)
		if err != nil {
			t.Fatalf("LegacyConsumerInfo: %v", err)
		}
		if convCInfo.Stream != legacyCInfo.Stream || convCInfo.Name != legacyCInfo.Name ||
			convCInfo.Config.AckPolicy != legacyCInfo.Config.AckPolicy {
			t.Fatalf("converted consumer info diverged: %+v vs %+v", convCInfo, legacyCInfo)
		}
		if backC := jetstream.ToConsumerInfo(legacyCInfo); backC.Name != newCInfo.Name ||
			backC.NumPending != newCInfo.NumPending {
			t.Fatalf("ToConsumerInfo diverged: %+v vs %+v", backC, newCInfo)
		}
	})
}

// TestLegacyCompat_UnboundCacheKeepsLegacyBehavior verifies the compatibility
// promise that, without BindLegacyManager, CachedInfo stays a pure local
// cache: it performs no server validation and keeps serving the snapshot it
// was populated with, exactly as before this feature existed.
func TestLegacyCompat_UnboundCacheKeepsLegacyBehavior(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
		ctx := newTesterCtx(t, 10*time.Second)
		mgr, err := nc.JetStream()
		if err != nil {
			t.Fatalf("JetStreamManager: %v", err)
		}
		// Deliberately do not call BindLegacyManager.
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name: "UNBOUND_S", Subjects: []string{"UNBOUND.>"}, Description: "cached",
		})
		if err != nil {
			t.Fatalf("CreateStream: %v", err)
		}
		if _, err := js.CreateOrUpdateConsumer(ctx, "UNBOUND_S", jetstream.ConsumerConfig{
			Name: "UNBOUND_C", Durable: "UNBOUND_C", AckPolicy: jetstream.AckExplicitPolicy,
		}); err != nil {
			t.Fatalf("CreateOrUpdateConsumer: %v", err)
		}
		cons, err := js.Consumer(ctx, "UNBOUND_S", "UNBOUND_C")
		if err != nil {
			t.Fatal(err)
		}

		info, err := mgr.StreamInfo("UNBOUND_S", nats.Context(ctx))
		if err != nil {
			t.Fatal(err)
		}
		info.Config.Description = "changed-out-of-band"
		if _, err := mgr.UpdateStream(&info.Config, nats.Context(ctx)); err != nil {
			t.Fatalf("legacy UpdateStream: %v", err)
		}

		// Without a guard the cache is not consulted against the server, so the
		// original snapshot is still served locally.
		if got := s.CachedInfo().Config.Description; got != "cached" {
			t.Fatalf("unbound stream cache changed behavior: %q", got)
		}
		if got := cons.CachedInfo(); got == nil || got.Name != "UNBOUND_C" {
			t.Fatalf("unbound consumer cache changed behavior: %+v", got)
		}
	})
}

// TestLegacyCompat_RevisionStableAndDistinct verifies revision semantics:
// equivalent configs hash to the same revision across both APIs, and different
// configs hash differently.
func TestLegacyCompat_RevisionStableAndDistinct(t *testing.T) {
	cfg := jetstream.StreamConfig{Name: "REV_S", Subjects: []string{"REV.>"}, MaxMsgs: 11}
	legacyCfg, err := jetstream.LegacyStreamConfig(cfg)
	if err != nil {
		t.Fatalf("LegacyStreamConfig: %v", err)
	}

	// The new-API view and the legacy view of the same configuration must carry
	// identical revisions.
	rNew, err := jetstream.StreamConfigRevision(&jetstream.StreamInfo{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	rLegacy, err := jetstream.LegacyStreamConfigRevision(&nats.StreamInfo{Config: legacyCfg})
	if err != nil {
		t.Fatal(err)
	}
	if rNew == "" || rNew != rLegacy {
		t.Fatalf("cross-API revision mismatch: new=%q legacy=%q", rNew, rLegacy)
	}
	// Deterministic across repeated calls.
	rNewAgain, err := jetstream.StreamConfigRevision(&jetstream.StreamInfo{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if rNew != rNewAgain {
		t.Fatal("revision is not deterministic")
	}

	// A changed configuration produces a distinct revision.
	changed := cfg
	changed.MaxMsgs = 12
	rChanged, err := jetstream.StreamConfigRevision(&jetstream.StreamInfo{Config: changed})
	if err != nil {
		t.Fatal(err)
	}
	if rChanged == rNew {
		t.Fatal("different configs produced the same revision")
	}

	// Consumer revisions follow the same rules and honor the AckPolicy mapping.
	ccfg := jetstream.ConsumerConfig{
		Name: "REV_C", Durable: "REV_C", AckPolicy: jetstream.AckExplicitPolicy,
	}
	legacyCCfg, err := jetstream.LegacyConsumerConfig(ccfg)
	if err != nil {
		t.Fatalf("LegacyConsumerConfig: %v", err)
	}
	crNew, err := jetstream.ConsumerConfigRevision(&jetstream.ConsumerInfo{Config: ccfg})
	if err != nil {
		t.Fatal(err)
	}
	crLegacy, err := jetstream.LegacyConsumerConfigRevision(&nats.ConsumerInfo{Config: legacyCCfg})
	if err != nil {
		t.Fatal(err)
	}
	if crNew != crLegacy {
		t.Fatalf("cross-API consumer revision mismatch: new=%q legacy=%q", crNew, crLegacy)
	}
}
