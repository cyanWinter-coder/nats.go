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
	"context"
	"errors"
)

// canceledAfterDispatchError wraps a context cancellation/deadline error that
// occurred after the request had already been handed to the connection. It
// preserves two ways to inspect the outcome:
//
//   - errors.Is(err, ErrRequestCanceledAfterDispatch) identifies the
//     ambiguous post-dispatch state (the write may have been applied by the
//     server), and
//   - errors.Is(err, context.Canceled) / errors.Is(err,
//     context.DeadlineExceeded) still matches the underlying context error,
//     keeping the pre-existing contract used by callers and existing tests.
type canceledAfterDispatchError struct {
	cause error
}

func (e *canceledAfterDispatchError) Error() string {
	return ErrRequestCanceledAfterDispatch.Error() + ": " + e.cause.Error()
}

// Is matches the compatibility sentinel and both standard context errors,
// delegating every other target to the wrapped cause.
func (e *canceledAfterDispatchError) Is(target error) bool {
	switch target {
	case ErrRequestCanceledAfterDispatch:
		return true
	case context.Canceled, context.DeadlineExceeded:
		return errors.Is(e.cause, target)
	default:
		return errors.Is(e.cause, target)
	}
}

// Unwrap exposes the original context error so generic unwrapping and
// error-code inspection keep working.
func (e *canceledAfterDispatchError) Unwrap() error {
	return e.cause
}

// newCanceledAfterDispatchError wraps a context cancellation observed after a
// request was dispatched. Non-context errors are returned unchanged.
func newCanceledAfterDispatchError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &canceledAfterDispatchError{cause: err}
	}
	return err
}
