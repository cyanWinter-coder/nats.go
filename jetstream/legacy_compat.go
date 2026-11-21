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

// This file contains the compatibility layer between the legacy
// JetStream management API (JetStreamManager/JetStreamContext in the root
// nats package) and the context-based JetStream API in this package.
//
// The layer exists so the two APIs can be used against the same server-side
// assets with explicit, testable guarantees:
//
//   - configurations and info payloads convert both ways without silently
//     dropping fields (fields the legacy API cannot represent fail with
//     ErrLegacyFieldNotSupported),
//   - errors returned for the same server-side condition share an error
//     category (see LegacyErrorCategory / EquivalentLegacyError),
//   - updates can be guarded against a previously observed revision so that
//     concurrent updates from either API cannot clobber each other with last
//     writer wins semantics (see GuardedUpdateOpt),
//   - context cancellation after the request left the client is reported as
//     an ambiguous outcome (ErrRequestCanceledAfterDispatch) rather than as a
//     plain pre-flight cancellation, letting callers distinguish "not sent"
//     from "may have been applied".
//
// The layer only adds new API surface; the existing methods keep their
// signatures and normal-path behavior.

import (
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// LegacyErrorCategory classifies errors by the underlying JetStream condition
// independently of which API (legacy or context-based) surfaced them. Both
// nats.APIError and jetstream.APIError report the same server err_code, so
// the category is derived from that code; client-side errors produced by this
// package are mapped to their semantic equivalent.
type LegacyErrorCategory string

const (
	// LegacyErrorCategoryNone means the error does not map to a known
	// JetStream API condition (e.g. a plain network or context error).
	LegacyErrorCategoryNone LegacyErrorCategory = ""

	LegacyErrorCategoryStreamNotFound          LegacyErrorCategory = "stream_not_found"
	LegacyErrorCategoryStreamNameInUse         LegacyErrorCategory = "stream_name_in_use"
	LegacyErrorCategoryConsumerNotFound        LegacyErrorCategory = "consumer_not_found"
	LegacyErrorCategoryConsumerExists          LegacyErrorCategory = "consumer_exists"
	LegacyErrorCategoryConsumerDoesNotExist    LegacyErrorCategory = "consumer_does_not_exist"
	LegacyErrorCategoryBadRequest              LegacyErrorCategory = "bad_request"
	LegacyErrorCategoryMessageNotFound         LegacyErrorCategory = "message_not_found"
	LegacyErrorCategoryJetStreamNotEnabled     LegacyErrorCategory = "jetstream_not_enabled"
	LegacyErrorCategoryJetStreamNotEnabledAcct LegacyErrorCategory = "jetstream_not_enabled_for_account"
	LegacyErrorCategoryMaxConsumers            LegacyErrorCategory = "maximum_consumers_limit"
	LegacyErrorCategoryWrongLastSequence       LegacyErrorCategory = "wrong_last_sequence"
	LegacyErrorCategoryUnsupportedField        LegacyErrorCategory = "unsupported_legacy_field"
	LegacyErrorCategoryRevisionConflict        LegacyErrorCategory = "revision_conflict"
	LegacyErrorCategoryCanceledAfterDispatch   LegacyErrorCategory = "canceled_after_dispatch"
)

// LegacyErrorCategoryOf returns the server-defined category of err. Errors
// produced by either the legacy package (nats.APIError) or this package
// (jetstream.APIError) map to the same category when they carry the same
// err_code; purely client-side errors from the compatibility layer map to
// their own categories.
func LegacyErrorCategoryOf(err error) LegacyErrorCategory {
	if err == nil {
		return LegacyErrorCategoryNone
	}
	switch {
	case errors.Is(err, ErrLegacyFieldNotSupported):
		return LegacyErrorCategoryUnsupportedField
	case errors.Is(err, ErrLegacyRevisionConflict):
		return LegacyErrorCategoryRevisionConflict
	case errors.Is(err, ErrRequestCanceledAfterDispatch):
		return LegacyErrorCategoryCanceledAfterDispatch
	}

	// Context-based API error: jetstream.APIError.Is matches on ErrorCode and
	// jsError unwraps to it.
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode {
		case JSErrCodeStreamNotFound:
			return LegacyErrorCategoryStreamNotFound
		case JSErrCodeStreamNameInUse:
			return LegacyErrorCategoryStreamNameInUse
		case JSErrCodeConsumerNotFound:
			return LegacyErrorCategoryConsumerNotFound
		case JSErrCodeConsumerExists:
			return LegacyErrorCategoryConsumerExists
		case JSErrCodeConsumerDoesNotExist:
			return LegacyErrorCategoryConsumerDoesNotExist
		case JSErrCodeBadRequest:
			return LegacyErrorCategoryBadRequest
		case JSErrCodeMessageNotFound:
			return LegacyErrorCategoryMessageNotFound
		case JSErrCodeJetStreamNotEnabled:
			return LegacyErrorCategoryJetStreamNotEnabled
		case JSErrCodeJetStreamNotEnabledForAccount:
			return LegacyErrorCategoryJetStreamNotEnabledAcct
		case JSErrCodeMaximumConsumersLimit:
			return LegacyErrorCategoryMaxConsumers
		case JSErrCodeStreamWrongLastSequence, JSErrCodeStreamWrongLastSequenceConstant:
			return LegacyErrorCategoryWrongLastSequence
		}
	}

	// Legacy package API error. The two APIError types are distinct, so match
	// the nats package one explicitly via its err_code.
	var legacyAPIErr *nats.APIError
	if errors.As(err, &legacyAPIErr) {
		switch nats.ErrorCode(legacyAPIErr.ErrorCode) {
		case nats.JSErrCodeStreamNotFound:
			return LegacyErrorCategoryStreamNotFound
		case nats.JSErrCodeStreamNameInUse:
			return LegacyErrorCategoryStreamNameInUse
		case nats.JSErrCodeConsumerNotFound:
			return LegacyErrorCategoryConsumerNotFound
		case nats.JSErrCodeConsumerNameExists, nats.JSErrCodeConsumerAlreadyExists:
			return LegacyErrorCategoryConsumerExists
		case nats.JSErrCodeBadRequest:
			return LegacyErrorCategoryBadRequest
		case nats.JSErrCodeMessageNotFound:
			return LegacyErrorCategoryMessageNotFound
		case nats.JSErrCodeJetStreamNotEnabled:
			return LegacyErrorCategoryJetStreamNotEnabled
		case nats.JSErrCodeJetStreamNotEnabledForAccount:
			return LegacyErrorCategoryJetStreamNotEnabledAcct
		case nats.JSErrCodeStreamWrongLastSequence, nats.JSErrCodeStreamWrongLastSequenceConstant:
			return LegacyErrorCategoryWrongLastSequence
		}
	}
	return LegacyErrorCategoryNone
}

// EquivalentLegacyError reports whether two errors returned by the two
// JetStream APIs represent the same server-side condition. A nil error is
// equivalent only to another nil error. Errors without a recognizable
// JetStream category are never equivalent to each other, even when their
// messages happen to match.
func EquivalentLegacyError(a, b error) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	catA := LegacyErrorCategoryOf(a)
	if catA == LegacyErrorCategoryNone {
		return false
	}
	return catA == LegacyErrorCategoryOf(b)
}

// unsupportedLegacyFieldf builds an ErrLegacyFieldNotSupported naming the
// configuration field that cannot cross the API boundary.
func unsupportedLegacyFieldf(field string) error {
	return fmt.Errorf("%w: %s", ErrLegacyFieldNotSupported, field)
}

// ---------------------------------------------------------------------------
// Stream configuration conversion
// ---------------------------------------------------------------------------

// LegacyStreamConfig converts a context API [StreamConfig] to a legacy
// nats.StreamConfig. Fields that have no representation in the legacy API
// (AllowMsgCounter, AllowAtomicPublish, AllowMsgSchedules, PersistMode,
// AllowBatchPublish and the durable stream-source Consumer) fail with
// ErrLegacyFieldNotSupported instead of being silently dropped.
func LegacyStreamConfig(cfg StreamConfig) (nats.StreamConfig, error) {
	if cfg.AllowMsgCounter {
		return nats.StreamConfig{}, unsupportedLegacyFieldf("StreamConfig.AllowMsgCounter")
	}
	if cfg.AllowAtomicPublish {
		return nats.StreamConfig{}, unsupportedLegacyFieldf("StreamConfig.AllowAtomicPublish")
	}
	if cfg.AllowMsgSchedules {
		return nats.StreamConfig{}, unsupportedLegacyFieldf("StreamConfig.AllowMsgSchedules")
	}
	if cfg.PersistMode != DefaultPersistMode {
		return nats.StreamConfig{}, unsupportedLegacyFieldf("StreamConfig.PersistMode")
	}
	if cfg.AllowBatchPublish {
		return nats.StreamConfig{}, unsupportedLegacyFieldf("StreamConfig.AllowBatchPublish")
	}
	for i, src := range cfg.Sources {
		if src != nil && src.Consumer != nil {
			return nats.StreamConfig{}, unsupportedLegacyFieldf(
				fmt.Sprintf("StreamConfig.Sources[%d].Consumer", i))
		}
	}
	if cfg.Mirror != nil && cfg.Mirror.Consumer != nil {
		return nats.StreamConfig{}, unsupportedLegacyFieldf("StreamConfig.Mirror.Consumer")
	}

	return nats.StreamConfig{
		Name:                   cfg.Name,
		Description:            cfg.Description,
		Subjects:               cloneStrings(cfg.Subjects),
		Retention:              nats.RetentionPolicy(cfg.Retention),
		MaxConsumers:           cfg.MaxConsumers,
		MaxMsgs:                cfg.MaxMsgs,
		MaxBytes:               cfg.MaxBytes,
		Discard:                nats.DiscardPolicy(cfg.Discard),
		DiscardNewPerSubject:   cfg.DiscardNewPerSubject,
		MaxAge:                 cfg.MaxAge,
		MaxMsgsPerSubject:      cfg.MaxMsgsPerSubject,
		MaxMsgSize:             cfg.MaxMsgSize,
		Storage:                nats.StorageType(cfg.Storage),
		Replicas:               cfg.Replicas,
		NoAck:                  cfg.NoAck,
		Duplicates:             cfg.Duplicates,
		Placement:              legacyPlacement(cfg.Placement),
		Mirror:                 legacyStreamSource(cfg.Mirror),
		Sources:                legacyStreamSources(cfg.Sources),
		Sealed:                 cfg.Sealed,
		DenyDelete:             cfg.DenyDelete,
		DenyPurge:              cfg.DenyPurge,
		AllowRollup:            cfg.AllowRollup,
		Compression:            nats.StoreCompression(cfg.Compression),
		FirstSeq:               cfg.FirstSeq,
		SubjectTransform:       legacySubjectTransform(cfg.SubjectTransform),
		RePublish:              legacyRePublish(cfg.RePublish),
		AllowDirect:            cfg.AllowDirect,
		MirrorDirect:           cfg.MirrorDirect,
		ConsumerLimits:         nats.StreamConsumerLimits(cfg.ConsumerLimits),
		Metadata:               cloneStringMap(cfg.Metadata),
		Template:               cfg.Template,
		AllowMsgTTL:            cfg.AllowMsgTTL,
		SubjectDeleteMarkerTTL: cfg.SubjectDeleteMarkerTTL,
	}, nil
}

// ToStreamConfig converts a legacy nats.StreamConfig to a context API
// [StreamConfig]. Every field of the legacy config is representable in the
// newer API, so this conversion never fails or loses data.
func ToStreamConfig(cfg nats.StreamConfig) StreamConfig {
	return StreamConfig{
		Name:                   cfg.Name,
		Description:            cfg.Description,
		Subjects:               cloneStrings(cfg.Subjects),
		Retention:              RetentionPolicy(cfg.Retention),
		MaxConsumers:           cfg.MaxConsumers,
		MaxMsgs:                cfg.MaxMsgs,
		MaxBytes:               cfg.MaxBytes,
		Discard:                DiscardPolicy(cfg.Discard),
		DiscardNewPerSubject:   cfg.DiscardNewPerSubject,
		MaxAge:                 cfg.MaxAge,
		MaxMsgsPerSubject:      cfg.MaxMsgsPerSubject,
		MaxMsgSize:             cfg.MaxMsgSize,
		Storage:                StorageType(cfg.Storage),
		Replicas:               cfg.Replicas,
		NoAck:                  cfg.NoAck,
		Duplicates:             cfg.Duplicates,
		Placement:              toPlacement(cfg.Placement),
		Mirror:                 toStreamSource(cfg.Mirror),
		Sources:                toStreamSources(cfg.Sources),
		Sealed:                 cfg.Sealed,
		DenyDelete:             cfg.DenyDelete,
		DenyPurge:              cfg.DenyPurge,
		AllowRollup:            cfg.AllowRollup,
		Compression:            StoreCompression(cfg.Compression),
		FirstSeq:               cfg.FirstSeq,
		SubjectTransform:       toSubjectTransform(cfg.SubjectTransform),
		RePublish:              toRePublish(cfg.RePublish),
		AllowDirect:            cfg.AllowDirect,
		MirrorDirect:           cfg.MirrorDirect,
		ConsumerLimits:         StreamConsumerLimits(cfg.ConsumerLimits),
		Metadata:               cloneStringMap(cfg.Metadata),
		Template:               cfg.Template,
		AllowMsgTTL:            cfg.AllowMsgTTL,
		SubjectDeleteMarkerTTL: cfg.SubjectDeleteMarkerTTL,
	}
}

// LegacyStreamInfo converts a context API [StreamInfo] to a legacy
// nats.StreamInfo. It returns the same error as [LegacyStreamConfig] if the
// embedded config uses fields the legacy API cannot represent.
func LegacyStreamInfo(info *StreamInfo) (*nats.StreamInfo, error) {
	if info == nil {
		return nil, nil
	}
	cfg, err := LegacyStreamConfig(info.Config)
	if err != nil {
		return nil, err
	}
	return &nats.StreamInfo{
		Config:     cfg,
		Created:    info.Created,
		State:      legacyStreamState(info.State),
		Cluster:    legacyClusterInfo(info.Cluster),
		Mirror:     legacyStreamSourceInfo(info.Mirror),
		Sources:    legacyStreamSourceInfos(info.Sources),
		Alternates: legacyAlternates(info.Alternates),
	}, nil
}

// ToStreamInfo converts a legacy nats.StreamInfo to a context API [StreamInfo].
func ToStreamInfo(info *nats.StreamInfo) *StreamInfo {
	if info == nil {
		return nil
	}
	return &StreamInfo{
		Config:     ToStreamConfig(info.Config),
		Created:    info.Created,
		State:      toStreamState(info.State),
		Cluster:    toClusterInfo(info.Cluster),
		Mirror:     toStreamSourceInfo(info.Mirror),
		Sources:    toStreamSourceInfos(info.Sources),
		Alternates: toAlternates(info.Alternates),
		TimeStamp:  time.Time{},
	}
}

// ---------------------------------------------------------------------------
// Consumer configuration conversion
// ---------------------------------------------------------------------------

// LegacyConsumerConfig converts a context API [ConsumerConfig] to a legacy
// nats.ConsumerConfig. Fields that have no representation in the legacy API
// (PauseUntil, PriorityPolicy, PinnedTTL and PriorityGroups) fail with
// ErrLegacyFieldNotSupported instead of being silently dropped.
func LegacyConsumerConfig(cfg ConsumerConfig) (nats.ConsumerConfig, error) {
	if cfg.PauseUntil != nil {
		return nats.ConsumerConfig{}, unsupportedLegacyFieldf("ConsumerConfig.PauseUntil")
	}
	if cfg.PriorityPolicy != PriorityPolicyNone {
		return nats.ConsumerConfig{}, unsupportedLegacyFieldf("ConsumerConfig.PriorityPolicy")
	}
	if cfg.PinnedTTL != 0 {
		return nats.ConsumerConfig{}, unsupportedLegacyFieldf("ConsumerConfig.PinnedTTL")
	}
	if len(cfg.PriorityGroups) != 0 {
		return nats.ConsumerConfig{}, unsupportedLegacyFieldf("ConsumerConfig.PriorityGroups")
	}
	return nats.ConsumerConfig{
		Durable:            cfg.Durable,
		Name:               cfg.Name,
		Description:        cfg.Description,
		DeliverPolicy:      legacyDeliverPolicy(cfg.DeliverPolicy),
		OptStartSeq:        cfg.OptStartSeq,
		OptStartTime:       cloneTime(cfg.OptStartTime),
		AckPolicy:          legacyAckPolicy(cfg.AckPolicy),
		AckWait:            cfg.AckWait,
		MaxDeliver:         cfg.MaxDeliver,
		BackOff:            cloneDurations(cfg.BackOff),
		FilterSubject:      cfg.FilterSubject,
		FilterSubjects:     cloneStrings(cfg.FilterSubjects),
		ReplayPolicy:       nats.ReplayPolicy(cfg.ReplayPolicy),
		RateLimit:          cfg.RateLimit,
		SampleFrequency:    cfg.SampleFrequency,
		MaxWaiting:         cfg.MaxWaiting,
		MaxAckPending:      cfg.MaxAckPending,
		FlowControl:        cfg.FlowControl,
		Heartbeat:          cfg.IdleHeartbeat,
		HeadersOnly:        cfg.HeadersOnly,
		MaxRequestBatch:    cfg.MaxRequestBatch,
		MaxRequestExpires:  cfg.MaxRequestExpires,
		MaxRequestMaxBytes: cfg.MaxRequestMaxBytes,
		DeliverSubject:     cfg.DeliverSubject,
		DeliverGroup:       cfg.DeliverGroup,
		InactiveThreshold:  cfg.InactiveThreshold,
		Replicas:           cfg.Replicas,
		MemoryStorage:      cfg.MemoryStorage,
		Metadata:           cloneStringMap(cfg.Metadata),
	}, nil
}

// ToConsumerConfig converts a legacy nats.ConsumerConfig to a context API
// [ConsumerConfig]. Every field of the legacy config is representable in the
// newer API, so this conversion never fails or loses data.
func ToConsumerConfig(cfg nats.ConsumerConfig) ConsumerConfig {
	return ConsumerConfig{
		Durable:            cfg.Durable,
		Name:               cfg.Name,
		Description:        cfg.Description,
		DeliverPolicy:      toDeliverPolicy(cfg.DeliverPolicy),
		OptStartSeq:        cfg.OptStartSeq,
		OptStartTime:       cloneTime(cfg.OptStartTime),
		AckPolicy:          toAckPolicy(cfg.AckPolicy),
		AckWait:            cfg.AckWait,
		MaxDeliver:         cfg.MaxDeliver,
		BackOff:            cloneDurations(cfg.BackOff),
		FilterSubject:      cfg.FilterSubject,
		FilterSubjects:     cloneStrings(cfg.FilterSubjects),
		ReplayPolicy:       ReplayPolicy(cfg.ReplayPolicy),
		RateLimit:          cfg.RateLimit,
		SampleFrequency:    cfg.SampleFrequency,
		MaxWaiting:         cfg.MaxWaiting,
		MaxAckPending:      cfg.MaxAckPending,
		FlowControl:        cfg.FlowControl,
		IdleHeartbeat:      cfg.Heartbeat,
		HeadersOnly:        cfg.HeadersOnly,
		MaxRequestBatch:    cfg.MaxRequestBatch,
		MaxRequestExpires:  cfg.MaxRequestExpires,
		MaxRequestMaxBytes: cfg.MaxRequestMaxBytes,
		DeliverSubject:     cfg.DeliverSubject,
		DeliverGroup:       cfg.DeliverGroup,
		InactiveThreshold:  cfg.InactiveThreshold,
		Replicas:           cfg.Replicas,
		MemoryStorage:      cfg.MemoryStorage,
		Metadata:           cloneStringMap(cfg.Metadata),
	}
}

// LegacyConsumerInfo converts a context API [ConsumerInfo] to a legacy
// nats.ConsumerInfo. It returns the same error as [LegacyConsumerConfig] if
// the embedded config uses fields the legacy API cannot represent.
func LegacyConsumerInfo(info *ConsumerInfo) (*nats.ConsumerInfo, error) {
	if info == nil {
		return nil, nil
	}
	cfg, err := LegacyConsumerConfig(info.Config)
	if err != nil {
		return nil, err
	}
	return &nats.ConsumerInfo{
		Stream:         info.Stream,
		Name:           info.Name,
		Created:        info.Created,
		Config:         cfg,
		Delivered:      nats.SequenceInfo(info.Delivered),
		AckFloor:       nats.SequenceInfo(info.AckFloor),
		NumAckPending:  info.NumAckPending,
		NumRedelivered: info.NumRedelivered,
		NumWaiting:     info.NumWaiting,
		NumPending:     info.NumPending,
		Cluster:        legacyClusterInfo(info.Cluster),
		PushBound:      info.PushBound,
	}, nil
}

// ToConsumerInfo converts a legacy nats.ConsumerInfo to a context API
// [ConsumerInfo].
func ToConsumerInfo(info *nats.ConsumerInfo) *ConsumerInfo {
	if info == nil {
		return nil
	}
	return &ConsumerInfo{
		Stream:         info.Stream,
		Name:           info.Name,
		Created:        info.Created,
		Config:         ToConsumerConfig(info.Config),
		Delivered:      SequenceInfo(info.Delivered),
		AckFloor:       SequenceInfo(info.AckFloor),
		NumAckPending:  info.NumAckPending,
		NumRedelivered: info.NumRedelivered,
		NumWaiting:     info.NumWaiting,
		NumPending:     info.NumPending,
		Cluster:        toClusterInfo(info.Cluster),
		PushBound:      info.PushBound,
	}
}

// ---------------------------------------------------------------------------
// Enum mapping
//
// Most policy enums share the same iota order across the two packages, but
// AckPolicy does not: the legacy package orders it None/All/Explicit while the
// context API orders it Explicit/All/None. The JSON wire values ("none",
// "all", "explicit", ...) are identical, but numeric conversion would silently
// corrupt in-memory configs, so every enum crossing the boundary is mapped
// explicitly.
// ---------------------------------------------------------------------------

// legacyAckPolicy maps a context API AckPolicy to its legacy equivalent.
func legacyAckPolicy(p AckPolicy) nats.AckPolicy {
	switch p {
	case AckNonePolicy:
		return nats.AckNonePolicy
	case AckAllPolicy:
		return nats.AckAllPolicy
	case AckExplicitPolicy:
		return nats.AckExplicitPolicy
	case AckFlowControlPolicy:
		return nats.AckFlowControlPolicy
	default:
		return nats.AckPolicy(p)
	}
}

// toAckPolicy maps a legacy AckPolicy to the context API equivalent.
func toAckPolicy(p nats.AckPolicy) AckPolicy {
	switch p {
	case nats.AckNonePolicy:
		return AckNonePolicy
	case nats.AckAllPolicy:
		return AckAllPolicy
	case nats.AckExplicitPolicy:
		return AckExplicitPolicy
	case nats.AckFlowControlPolicy:
		return AckFlowControlPolicy
	default:
		return AckPolicy(p)
	}
}

// legacyDeliverPolicy maps a context API DeliverPolicy to its legacy
// equivalent. The numeric values currently coincide, but the explicit mapping
// keeps the boundary safe if either package reorders the enum later.
func legacyDeliverPolicy(p DeliverPolicy) nats.DeliverPolicy {
	switch p {
	case DeliverAllPolicy:
		return nats.DeliverAllPolicy
	case DeliverLastPolicy:
		return nats.DeliverLastPolicy
	case DeliverNewPolicy:
		return nats.DeliverNewPolicy
	case DeliverByStartSequencePolicy:
		return nats.DeliverByStartSequencePolicy
	case DeliverByStartTimePolicy:
		return nats.DeliverByStartTimePolicy
	case DeliverLastPerSubjectPolicy:
		return nats.DeliverLastPerSubjectPolicy
	default:
		return nats.DeliverPolicy(p)
	}
}

// toDeliverPolicy maps a legacy DeliverPolicy to the context API equivalent.
func toDeliverPolicy(p nats.DeliverPolicy) DeliverPolicy {
	switch p {
	case nats.DeliverAllPolicy:
		return DeliverAllPolicy
	case nats.DeliverLastPolicy:
		return DeliverLastPolicy
	case nats.DeliverNewPolicy:
		return DeliverNewPolicy
	case nats.DeliverByStartSequencePolicy:
		return DeliverByStartSequencePolicy
	case nats.DeliverByStartTimePolicy:
		return DeliverByStartTimePolicy
	case nats.DeliverLastPerSubjectPolicy:
		return DeliverLastPerSubjectPolicy
	default:
		return DeliverPolicy(p)
	}
}

// ---------------------------------------------------------------------------
// Nested type conversions
// ---------------------------------------------------------------------------

func legacyPlacement(p *Placement) *nats.Placement {
	if p == nil {
		return nil
	}
	return &nats.Placement{Cluster: p.Cluster, Tags: cloneStrings(p.Tags)}
}

func toPlacement(p *nats.Placement) *Placement {
	if p == nil {
		return nil
	}
	return &Placement{Cluster: p.Cluster, Tags: cloneStrings(p.Tags)}
}

func legacySubjectTransform(t *SubjectTransformConfig) *nats.SubjectTransformConfig {
	if t == nil {
		return nil
	}
	return &nats.SubjectTransformConfig{Source: t.Source, Destination: t.Destination}
}

func toSubjectTransform(t *nats.SubjectTransformConfig) *SubjectTransformConfig {
	if t == nil {
		return nil
	}
	return &SubjectTransformConfig{Source: t.Source, Destination: t.Destination}
}

func legacyRePublish(r *RePublish) *nats.RePublish {
	if r == nil {
		return nil
	}
	return &nats.RePublish{Source: r.Source, Destination: r.Destination, HeadersOnly: r.HeadersOnly}
}

func toRePublish(r *nats.RePublish) *RePublish {
	if r == nil {
		return nil
	}
	return &RePublish{Source: r.Source, Destination: r.Destination, HeadersOnly: r.HeadersOnly}
}

func legacyStreamSource(s *StreamSource) *nats.StreamSource {
	if s == nil {
		return nil
	}
	// Consumer is guarded in LegacyStreamConfig; anything reaching here has no
	// durable-source consumer to convert.
	out := &nats.StreamSource{
		Name:              s.Name,
		OptStartSeq:       s.OptStartSeq,
		OptStartTime:      cloneTime(s.OptStartTime),
		FilterSubject:     s.FilterSubject,
		SubjectTransforms: legacySubjectTransforms(s.SubjectTransforms),
		External:          legacyExternalStream(s.External),
		Domain:            s.Domain,
	}
	return out
}

func toStreamSource(s *nats.StreamSource) *StreamSource {
	if s == nil {
		return nil
	}
	return &StreamSource{
		Name:              s.Name,
		OptStartSeq:       s.OptStartSeq,
		OptStartTime:      cloneTime(s.OptStartTime),
		FilterSubject:     s.FilterSubject,
		SubjectTransforms: toSubjectTransforms(s.SubjectTransforms),
		External:          toExternalStream(s.External),
		Domain:            s.Domain,
	}
}

func legacyStreamSources(in []*StreamSource) []*nats.StreamSource {
	if in == nil {
		return nil
	}
	out := make([]*nats.StreamSource, len(in))
	for i, s := range in {
		out[i] = legacyStreamSource(s)
	}
	return out
}

func toStreamSources(in []*nats.StreamSource) []*StreamSource {
	if in == nil {
		return nil
	}
	out := make([]*StreamSource, len(in))
	for i, s := range in {
		out[i] = toStreamSource(s)
	}
	return out
}

func legacySubjectTransforms(in []SubjectTransformConfig) []nats.SubjectTransformConfig {
	if in == nil {
		return nil
	}
	out := make([]nats.SubjectTransformConfig, len(in))
	for i, t := range in {
		out[i] = nats.SubjectTransformConfig{Source: t.Source, Destination: t.Destination}
	}
	return out
}

func toSubjectTransforms(in []nats.SubjectTransformConfig) []SubjectTransformConfig {
	if in == nil {
		return nil
	}
	out := make([]SubjectTransformConfig, len(in))
	for i, t := range in {
		out[i] = SubjectTransformConfig{Source: t.Source, Destination: t.Destination}
	}
	return out
}

func legacyExternalStream(e *ExternalStream) *nats.ExternalStream {
	if e == nil {
		return nil
	}
	return &nats.ExternalStream{APIPrefix: e.APIPrefix, DeliverPrefix: e.DeliverPrefix}
}

func toExternalStream(e *nats.ExternalStream) *ExternalStream {
	if e == nil {
		return nil
	}
	return &ExternalStream{APIPrefix: e.APIPrefix, DeliverPrefix: e.DeliverPrefix}
}

func legacyStreamState(s StreamState) nats.StreamState {
	return nats.StreamState{
		Msgs:        s.Msgs,
		Bytes:       s.Bytes,
		FirstSeq:    s.FirstSeq,
		FirstTime:   s.FirstTime,
		LastSeq:     s.LastSeq,
		LastTime:    s.LastTime,
		Consumers:   s.Consumers,
		Deleted:     cloneU64(s.Deleted),
		NumDeleted:  s.NumDeleted,
		NumSubjects: s.NumSubjects,
		Subjects:    cloneU64Map(s.Subjects),
	}
}

func toStreamState(s nats.StreamState) StreamState {
	return StreamState{
		Msgs:        s.Msgs,
		Bytes:       s.Bytes,
		FirstSeq:    s.FirstSeq,
		FirstTime:   s.FirstTime,
		LastSeq:     s.LastSeq,
		LastTime:    s.LastTime,
		Consumers:   s.Consumers,
		Deleted:     cloneU64(s.Deleted),
		NumDeleted:  s.NumDeleted,
		NumSubjects: s.NumSubjects,
		Subjects:    cloneU64Map(s.Subjects),
	}
}

func legacyClusterInfo(c *ClusterInfo) *nats.ClusterInfo {
	if c == nil {
		return nil
	}
	out := &nats.ClusterInfo{
		Name:        c.Name,
		RaftGroup:   c.RaftGroup,
		Leader:      c.Leader,
		LeaderSince: cloneTime(c.LeaderSince),
		SystemAcc:   c.SystemAcc,
		TrafficAcc:  c.TrafficAcc,
	}
	if len(c.Replicas) != 0 {
		out.Replicas = make([]*nats.PeerInfo, len(c.Replicas))
		for i, p := range c.Replicas {
			if p == nil {
				continue
			}
			out.Replicas[i] = &nats.PeerInfo{
				Name: p.Name, Current: p.Current, Offline: p.Offline,
				Active: p.Active, Lag: p.Lag, Peer: p.Peer, Pending: p.Pending,
			}
		}
	}
	return out
}

func toClusterInfo(c *nats.ClusterInfo) *ClusterInfo {
	if c == nil {
		return nil
	}
	out := &ClusterInfo{
		Name:        c.Name,
		RaftGroup:   c.RaftGroup,
		Leader:      c.Leader,
		LeaderSince: cloneTime(c.LeaderSince),
		SystemAcc:   c.SystemAcc,
		TrafficAcc:  c.TrafficAcc,
	}
	if len(c.Replicas) != 0 {
		out.Replicas = make([]*PeerInfo, len(c.Replicas))
		for i, p := range c.Replicas {
			if p == nil {
				continue
			}
			out.Replicas[i] = &PeerInfo{
				Name: p.Name, Current: p.Current, Offline: p.Offline,
				Active: p.Active, Lag: p.Lag, Peer: p.Peer, Pending: p.Pending,
			}
		}
	}
	return out
}

func legacyStreamSourceInfo(s *StreamSourceInfo) *nats.StreamSourceInfo {
	if s == nil {
		return nil
	}
	return &nats.StreamSourceInfo{
		Name:              s.Name,
		Lag:               s.Lag,
		Seq:               s.Seq,
		Active:            s.Active,
		External:          legacyExternalStream(s.External),
		Error:             legacyAPIError(s.Error),
		FilterSubject:     s.FilterSubject,
		SubjectTransforms: legacySubjectTransforms(s.SubjectTransforms),
	}
}

func toStreamSourceInfo(s *nats.StreamSourceInfo) *StreamSourceInfo {
	if s == nil {
		return nil
	}
	return &StreamSourceInfo{
		Name:              s.Name,
		Lag:               s.Lag,
		Seq:               s.Seq,
		Active:            s.Active,
		External:          toExternalStream(s.External),
		Error:             toAPIError(s.Error),
		FilterSubject:     s.FilterSubject,
		SubjectTransforms: toSubjectTransforms(s.SubjectTransforms),
	}
}

func legacyStreamSourceInfos(in []*StreamSourceInfo) []*nats.StreamSourceInfo {
	if in == nil {
		return nil
	}
	out := make([]*nats.StreamSourceInfo, len(in))
	for i, s := range in {
		out[i] = legacyStreamSourceInfo(s)
	}
	return out
}

func toStreamSourceInfos(in []*nats.StreamSourceInfo) []*StreamSourceInfo {
	if in == nil {
		return nil
	}
	out := make([]*StreamSourceInfo, len(in))
	for i, s := range in {
		out[i] = toStreamSourceInfo(s)
	}
	return out
}

func legacyAlternates(in []StreamAlternate) []*nats.StreamAlternate {
	if in == nil {
		return nil
	}
	out := make([]*nats.StreamAlternate, len(in))
	for i, a := range in {
		out[i] = &nats.StreamAlternate{Name: a.Name, Domain: a.Domain, Cluster: a.Cluster}
	}
	return out
}

func toAlternates(in []*nats.StreamAlternate) []StreamAlternate {
	if in == nil {
		return nil
	}
	out := make([]StreamAlternate, len(in))
	for i, a := range in {
		if a == nil {
			continue
		}
		out[i] = StreamAlternate{Name: a.Name, Domain: a.Domain, Cluster: a.Cluster}
	}
	return out
}

// legacyAPIError converts a jetstream.APIError as carried inside info
// payloads (e.g. source replication errors) to the legacy APIError type.
func legacyAPIError(e *APIError) *nats.APIError {
	if e == nil {
		return nil
	}
	return &nats.APIError{
		Code:        e.Code,
		ErrorCode:   nats.ErrorCode(e.ErrorCode),
		Description: e.Description,
	}
}

func toAPIError(e *nats.APIError) *APIError {
	if e == nil {
		return nil
	}
	return &APIError{
		Code:        e.Code,
		ErrorCode:   ErrorCode(e.ErrorCode),
		Description: e.Description,
	}
}

// ---------------------------------------------------------------------------
// Small cloning helpers: conversions never alias caller-owned slices/maps.
// ---------------------------------------------------------------------------

func cloneStrings(in []string) []string {
	if in == nil {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func cloneDurations(in []time.Duration) []time.Duration {
	if in == nil {
		return nil
	}
	out := make([]time.Duration, len(in))
	copy(out, in)
	return out
}

func cloneU64(in []uint64) []uint64 {
	if in == nil {
		return nil
	}
	out := make([]uint64, len(in))
	copy(out, in)
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneU64Map(in map[string]uint64) map[string]uint64 {
	if in == nil {
		return nil
	}
	out := make(map[string]uint64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	ct := *t
	return &ct
}
