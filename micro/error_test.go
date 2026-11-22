// Copyright 2025 The NATS Authors
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

package micro

import (
	"errors"
	"sync"
	"testing"
)

func TestNATSErrorUnwrap(t *testing.T) {
	baseErr := errors.New("base error")
	natsErr := &NATSError{
		Subject:     "test.subject",
		Description: "test description",
		err:         baseErr,
	}

	unwrapped := errors.Unwrap(natsErr)
	if unwrapped != baseErr {
		t.Errorf("Unwrap() = %v, want %v", unwrapped, baseErr)
	}

	// Test with nil underlying error
	natsErr2 := &NATSError{
		Subject:     "test.subject",
		Description: "test description",
	}
	if errors.Unwrap(natsErr2) != nil {
		t.Errorf("Unwrap() with nil err should return nil")
	}
}

func TestNATSErrorIs(t *testing.T) {
	natsErr1 := &NATSError{
		Subject:     "test.subject",
		Description: "test description",
	}

	natsErr2 := &NATSError{
		Subject:     "test.subject",
		Description: "test description",
	}

	natsErr3 := &NATSError{
		Subject:     "different.subject",
		Description: "test description",
	}

	natsErr4 := &NATSError{
		Subject:     "test.subject",
		Description: "different description",
	}

	// Test equality
	if !errors.Is(natsErr1, natsErr2) {
		t.Errorf("Is() = false, want true for equal errors")
	}

	// different subject
	if errors.Is(natsErr1, natsErr3) {
		t.Errorf("Is() = true, want false for different subjects")
	}

	// different description
	if errors.Is(natsErr1, natsErr4) {
		t.Errorf("Is() = true, want false for different descriptions")
	}

	// non NATSError
	otherErr := errors.New("other error")
	if errors.Is(natsErr1, otherErr) {
		t.Errorf("Is() = true, want false for non-NATSError")
	}
}

func TestNATSErrorWrapping(t *testing.T) {
	// Test error chain
	baseErr := errors.New("connection failed")
	natsErr := &NATSError{
		Subject:     "test.subject",
		Description: "service error: connection failed",
		err:         baseErr,
	}

	// Check that we can find the base error in the chain
	if !errors.Is(natsErr, baseErr) {
		t.Errorf("errors.Is() should find base error in chain")
	}

	// Test error message
	expectedMsg := `"test.subject": service error: connection failed`
	if natsErr.Error() != expectedMsg {
		t.Errorf("Error() = %q, want %q", natsErr.Error(), expectedMsg)
	}
}

// TestRequestResponseClaim verifies the response ownership boundary without
// a server: only the first claim succeeds and every later Respond, RespondJSON
// or Error attempt is rejected with ErrRequestAlreadyResponded.
func TestRequestResponseClaim(t *testing.T) {
	t.Run("claim once", func(t *testing.T) {
		r := &request{}
		if err := r.claimResponse(); err != nil {
			t.Fatalf("first claim failed: %v", err)
		}
		for i := 0; i < 3; i++ {
			if err := r.claimResponse(); !errors.Is(err, ErrRequestAlreadyResponded) {
				t.Fatalf("claim %d: want %v; got %v", i+2, ErrRequestAlreadyResponded, err)
			}
		}
	})

	t.Run("concurrent claims", func(t *testing.T) {
		r := &request{}
		var won, lost int64
		var mu sync.Mutex
		var wg sync.WaitGroup
		start := make(chan struct{})
		for range 50 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				err := r.claimResponse()
				mu.Lock()
				if err == nil {
					won++
				} else if errors.Is(err, ErrRequestAlreadyResponded) {
					lost++
				} else {
					t.Errorf("unexpected error: %v", err)
				}
				mu.Unlock()
			}()
		}
		close(start)
		wg.Wait()
		if won != 1 || lost != 49 {
			t.Fatalf("want exactly 1 winning claim and 49 rejections; got %d won, %d lost", won, lost)
		}
	})

	t.Run("validation errors do not claim", func(t *testing.T) {
		r := &request{}
		if err := r.Error("", "desc", nil); !errors.Is(err, ErrArgRequired) {
			t.Fatalf("want %v; got %v", ErrArgRequired, err)
		}
		if err := r.Error("500", "", nil); !errors.Is(err, ErrArgRequired) {
			t.Fatalf("want %v; got %v", ErrArgRequired, err)
		}
		// The request was never claimed, so a later claim still succeeds.
		if err := r.claimResponse(); err != nil {
			t.Fatalf("claim after failed validation should succeed; got %v", err)
		}
	})
}
