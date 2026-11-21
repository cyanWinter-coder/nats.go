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

	"github.com/nats-io/nats.go"
)

// CacheGuard lets a [Stream] or [Consumer] verify that its locally cached
// info still matches the server when another API may have changed the
// resource. The canonical implementation is backed by a legacy
// nats.JetStreamManager: whenever the legacy API (or any other client)
// updates a stream or consumer, the guard's revision check fails and the
// cache is refreshed before being observed, so an update made through one
// API can never be read as a stale snapshot through the other.
type CacheGuard interface {
	// IsFresh reports whether the given cached info still matches the
	// server-side revision. A false result triggers a transparent refresh.
	IsFresh(cached any) bool
}

// BindLegacyManager attaches a legacy-backed [CacheGuard] to js. Afterwards,
// [Stream.CachedInfo] and [Consumer.CachedInfo] on handles returned by js
// validate their cache against the server through mgr whenever the cache may
// have been invalidated by an update issued through the legacy
// JetStreamManager API.
//
// Binding is optional and additive: without it the package behaves exactly as
// before (cache updated only by its own Info/Update calls). The manager should
// use the same API prefix/domain as js.
func BindLegacyManager(js JetStream, mgr nats.JetStreamManager) error {
	j, ok := js.(*jetStream)
	if !ok {
		return errors.New("jetstream: unsupported JetStream implementation passed to BindLegacyManager")
	}
	j.cacheGuard = &legacyCacheGuard{mgr: mgr}
	return nil
}

// legacyCacheGuard implements CacheGuard by fetching the current info through
// the legacy manager and comparing content-addressed revisions.
type legacyCacheGuard struct {
	mgr nats.JetStreamManager
}

// IsFresh accepts *StreamInfo or *ConsumerInfo snapshots and compares their
// configuration revision against a fresh legacy API read. Unknown snapshot
// types and transient fetch errors are treated as fresh so a cache read never
// hard-fails because of the guard.
func (g *legacyCacheGuard) IsFresh(cached any) bool {
	switch info := cached.(type) {
	case *StreamInfo:
		cur, err := g.mgr.StreamInfo(info.Config.Name)
		if err != nil {
			return true
		}
		curRev, err := LegacyStreamConfigRevision(cur)
		if err != nil {
			return true
		}
		oldRev, err := StreamConfigRevision(info)
		if err != nil {
			return true
		}
		return curRev == oldRev
	case *ConsumerInfo:
		cur, err := g.mgr.ConsumerInfo(info.Stream, info.Name)
		if err != nil {
			return true
		}
		curRev, err := LegacyConsumerConfigRevision(cur)
		if err != nil {
			return true
		}
		oldRev, err := ConsumerConfigRevision(info)
		if err != nil {
			return true
		}
		return curRev == oldRev
	default:
		return true
	}
}
