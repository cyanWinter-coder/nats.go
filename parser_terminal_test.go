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
	"errors"
	"fmt"
	"strings"
	"testing"
)

// parserTestConn wires a bare Conn to a fresh parser generation and one
// channel subscription. The CLOSED status keeps server-side control frames
// (-ERR) from attempting a full connection teardown, which needs machinery
// these in-memory tests do not set up: the parser is the system under test.
func parserTestConn() (*Conn, chan *Msg) {
	c := &Conn{status: CLOSED}
	c.newReaderWriter()
	c.bw.switchToPending()
	c.subsMu.Lock()
	c.subs = make(map[int64]*Subscription)
	ch := make(chan *Msg, 1<<16)
	c.subs[1] = &Subscription{sid: 1, Subject: "a", mch: ch, typ: ChanSubscription}
	c.subsMu.Unlock()
	c.mu.Lock()
	c.resetParserLocked()
	c.mu.Unlock()
	return c, ch
}

// feed parses stream in fixed-size chunks, stopping at the first error.
func feed(c *Conn, stream []byte, chunk int) error {
	if chunk <= 0 {
		chunk = len(stream)
	}
	for off := 0; off < len(stream); off += chunk {
		end := off + chunk
		if end > len(stream) {
			end = len(stream)
		}
		if err := c.parse(stream[off:end]); err != nil {
			return err
		}
	}
	return nil
}

// collect drains delivered messages without blocking.
func collect(ch chan *Msg) []string {
	var out []string
	for {
		select {
		case m := <-ch:
			out = append(out, string(m.Data))
		default:
			return out
		}
	}
}

func protoMsg(topic string, sid int, payload string) string {
	return fmt.Sprintf("MSG %s %d %d\r\n%s\r\n", topic, sid, len(payload), payload)
}

// TestParserStickyErrorIsTerminal verifies that a malformed frame puts the
// parser in a single terminal state that later calls can not escape, and that
// the input after the failure is never interpreted.
func TestParserStickyErrorIsTerminal(t *testing.T) {
	bad := []struct {
		name   string
		stream string
		class  error
	}{
		{"bad opcode", "ZOO\r\n", ErrParserProtocol},
		{"bad msg arity", "MSG a\r\n", ErrParserProtocol},
		{"bad sid", "MSG a xyz 3\r\n", ErrParserProtocol},
		{"negative size", "MSG a 1 -1\r\n", ErrParserBadLength},
		{"size overflow", "MSG a 1 9223372036854775808\r\n", ErrParserBadLength},
		{"header larger than payload", "HMSG a 1 9 3\r\n", ErrParserBadLength},
		{"empty argument line", "MSG \r\n", ErrParserProtocol},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := parserTestConn()
			first := c.parse([]byte(tc.stream))
			if first == nil {
				t.Fatalf("%q should have failed", tc.stream)
			}
			if !errors.Is(first, tc.class) {
				t.Fatalf("expected class %v, got %v", tc.class, first)
			}
			// A fully valid message after the failure must not be parsed.
			frozen := c.ps.consumed
			for _, follow := range [][]byte{
				[]byte(protoMsg("a", 1, "should-not-arrive")),
				nil,
				[]byte("PING\r\n"),
			} {
				if err := c.parse(follow); err != first {
					t.Fatalf("expected the same sticky error %v, got %v", first, err)
				}
			}
			if got := c.ps.consumed; got != frozen {
				t.Fatalf("parser consumed %d bytes after a terminal error (was %d)", got, frozen)
			}
		})
	}
}

// TestParserDeliversCompletedMessagesBeforeError verifies that whole messages
// preceding a bad frame are still delivered, and that a partial message never
// reaches the callback.
func TestParserDeliversCompletedMessagesBeforeError(t *testing.T) {
	c, ch := parserTestConn()
	// Two complete messages, then a third frame cut off mid-payload. The
	// partial third message must never be delivered.
	stream := protoMsg("a", 1, "one") + protoMsg("a", 1, "two") + "MSG a 1 5\r\nab"
	if err := c.parse([]byte(stream)); err != nil {
		t.Fatalf("partial payload should not fail on its own: %v", err)
	}
	if got, want := collect(ch), []string{"one", "two"}; !equalStrings(got, want) {
		t.Fatalf("delivered %q, want %q", got, want)
	}
	if c.InMsgs != 2 {
		t.Fatalf("InMsgs = %d, want 2", c.InMsgs)
	}
	// Ending the input now is a truncation of the third frame.
	if err := c.parserInputError(errors.New("EOF")); !errors.Is(err, ErrParserTruncated) {
		t.Fatalf("expected ErrParserTruncated, got %v", err)
	}
	if got := collect(ch); len(got) != 0 {
		t.Fatalf("the half message leaked into the callback: %q", got)
	}
}

// TestParserMalformedFrameAfterCompleteOnes verifies that a bad opcode after
// valid frames keeps the earlier deliveries and fails terminally.
func TestParserMalformedFrameAfterCompleteOnes(t *testing.T) {
	c, ch := parserTestConn()
	stream := protoMsg("a", 1, "one") + protoMsg("a", 1, "two") + "ZOO\r\n"
	err := c.parse([]byte(stream))
	if !errors.Is(err, ErrParserProtocol) {
		t.Fatalf("expected ErrParserProtocol, got %v", err)
	}
	if got, want := collect(ch), []string{"one", "two"}; !equalStrings(got, want) {
		t.Fatalf("delivered %q, want %q", got, want)
	}
}

// TestParserTruncatedInputIsTerminal verifies that a read error mid-frame is
// classified as a truncation and becomes the sticky terminal error, and that
// the half-received message is never delivered.
func TestParserTruncatedInputIsTerminal(t *testing.T) {
	c, ch := parserTestConn()
	if err := c.parse([]byte("MSG a 1 10\r\nabc")); err != nil {
		t.Fatalf("partial payload should not fail on its own: %v", err)
	}
	truncErr := c.parserInputError(errors.New("EOF"))
	if !errors.Is(truncErr, ErrParserTruncated) {
		t.Fatalf("expected ErrParserTruncated, got %v", truncErr)
	}
	if again := c.parse([]byte("defghij\r\n")); again != truncErr {
		t.Fatalf("expected sticky truncation error, got %v", again)
	}
	if got := collect(ch); len(got) != 0 {
		t.Fatalf("unexpected delivery: %q", got)
	}
}

// TestParserCleanBoundaryReadErrorPassesThrough verifies that a read error
// between frames is not turned into a truncation.
func TestParserCleanBoundaryReadErrorPassesThrough(t *testing.T) {
	c, _ := parserTestConn()
	if err := c.parse([]byte(protoMsg("a", 1, "x"))); err != nil {
		t.Fatalf("parse: %v", err)
	}
	ioErr := errors.New("boom")
	if got := c.parserInputError(ioErr); got != ioErr {
		t.Fatalf("expected the original read error, got %v", got)
	}
	if c.ps.err != nil {
		t.Fatalf("a clean boundary must not enter a terminal state: %v", c.ps.err)
	}
}

// TestParserTerminatorBoundary verifies that a bad payload terminator fails at
// the boundary instead of consuming the next frame's header.
func TestParserTerminatorBoundary(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{"stray byte after payload", "MSG a 1 3\r\nfooX" + protoMsg("a", 1, "next")},
		{"CR then non-LF", "MSG a 1 3\r\nfoo\rX" + protoMsg("a", 1, "next")},
		{"two trailing CRs", "MSG a 1 3\r\nfoo\r\r\n" + protoMsg("a", 1, "next")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, ch := parserTestConn()
			err := c.parse([]byte(tc.input))
			if !errors.Is(err, ErrParserProtocol) {
				t.Fatalf("expected ErrParserProtocol, got %v", err)
			}
			if got, want := collect(ch), []string{"foo"}; !equalStrings(got, want) {
				t.Fatalf("delivered %q, want %q", got, want)
			}
			// The next frame must not have been consumed as part of the bad
			// one, so the fan-out is exactly one delivered message.
			if c.InMsgs != 1 {
				t.Fatalf("InMsgs = %d, want 1", c.InMsgs)
			}
		})
	}
}

// TestParserChunkInvariance verifies that the same stream produces identical
// deliveries regardless of how it is split across reads, including the exact
// control-line/payload boundaries.
func TestParserChunkInvariance(t *testing.T) {
	big := strings.Repeat("q", MAX_CONTROL_LINE_SIZE+37)
	streams := []string{
		protoMsg("a", 1, "hello") + protoMsg("a", 1, "world"),
		protoMsg("a", 1, "") + protoMsg("a", 1, "after-empty"),
		protoMsg("a", 1, "") + protoMsg("a", 1, "x") + protoMsg("a", 1, "") + protoMsg("a", 1, "y"),
		"PING\r\n" + protoMsg("a", 1, "x") + "PONG\r\n",
		protoMsg("a", 1, big) + protoMsg("a", 1, "tail"),
		protoMsg("a", 1, "head") + protoMsg("a", 1, big),
		"HMSG a 1 12 17\r\nNATS/1.0\r\n\r\nhello\r\n" + protoMsg("a", 1, "after-hdr"),
	}
	for si, stream := range streams {
		wantConn, wantCh := parserTestConn()
		if err := feed(wantConn, []byte(stream), 0); err != nil {
			t.Fatalf("stream %d baseline: %v", si, err)
		}
		wantMsgs := collect(wantCh)
		wantIn, wantBytes := wantConn.InMsgs, wantConn.InBytes

		for _, chunk := range []int{1, 2, 3, 7, 4095, 4096, 4097, len(stream)} {
			if chunk > len(stream) {
				continue
			}
			c, ch := parserTestConn()
			if err := feed(c, []byte(stream), chunk); err != nil {
				t.Fatalf("stream %d chunk %d: %v", si, chunk, err)
			}
			got := collect(ch)
			if !equalStrings(got, wantMsgs) || c.InMsgs != wantIn || c.InBytes != wantBytes {
				t.Fatalf("stream %d chunk %d: got %d msgs/%d in/%d bytes, want %d/%d/%d",
					si, chunk, len(got), c.InMsgs, c.InBytes, len(wantMsgs), wantIn, wantBytes)
			}
		}
	}
}

// TestParserTerminatorCRLFSplit verifies the payload terminator is not
// half-consumed when a read ends between its CR and its LF, for both an
// in-buffer payload and one large enough to spill into the scratch buffer. A
// parser that advanced past the first terminator byte would either lose the
// following frame or misread it.
func TestParserTerminatorCRLFSplit(t *testing.T) {
	for _, size := range []int{3, MAX_CONTROL_LINE_SIZE - 1, MAX_CONTROL_LINE_SIZE + 1, 5000} {
		payload := strings.Repeat("L", size)
		stream := fmt.Sprintf("MSG a 1 %d\r\n", size) + payload + "\r\n" + protoMsg("a", 1, "end")
		cr := len(fmt.Sprintf("MSG a 1 %d\r\n", size)) + size // index of the terminator CR
		// Deliver up to and including CR, then the rest (LF + next frame).
		c, ch := parserTestConn()
		if err := c.parse([]byte(stream[:cr+1])); err != nil {
			t.Fatalf("size %d CR chunk: %v", size, err)
		}
		if err := c.parse([]byte(stream[cr+1:])); err != nil {
			t.Fatalf("size %d LF chunk: %v", size, err)
		}
		if got := collect(ch); !equalStrings(got, []string{payload, "end"}) {
			t.Fatalf("size %d: delivered %d msgs", size, len(got))
		}
	}
}

// TestParserOversizedPayloadIntoSplitControlLine feeds a maximum-sized payload
// whose terminator is followed immediately by a control line split at every
// position, which is where a boundary lookup can run past the read.
func TestParserOversizedPayloadIntoSplitControlLine(t *testing.T) {
	payload := strings.Repeat("p", MAX_CONTROL_LINE_SIZE*2)
	stream := protoMsg("a", 1, payload) + protoMsg("a", 1, "next")
	nextMsg := protoMsg("a", 1, "next")
	for cut := len(stream) - len(nextMsg); cut < len(stream); cut++ {
		c, ch := parserTestConn()
		if err := c.parse([]byte(stream[:cut])); err != nil {
			t.Fatalf("cut %d first chunk: %v", cut, err)
		}
		if err := c.parse([]byte(stream[cut:])); err != nil {
			t.Fatalf("cut %d second chunk: %v", cut, err)
		}
		if got := collect(ch); !equalStrings(got, []string{payload, "next"}) {
			t.Fatalf("cut %d: delivered %d msgs", cut, len(got))
		}
	}
}

// TestParserSplitControlLineThenOversizedPayload reverses the order: the MSG
// argument line itself arrives across reads, then a payload large enough that
// it is copied into the scratch buffer. The two split paths share as/drop/argBuf
// bookkeeping, so a stale index in one corrupts the other.
func TestParserSplitControlLineThenOversizedPayload(t *testing.T) {
	payload := strings.Repeat("L", MAX_CONTROL_LINE_SIZE+123)
	control := fmt.Sprintf("MSG a 1 %d\r\n", len(payload))
	stream := control + payload + "\r\n" + protoMsg("a", 1, "tail")
	for cut := 1; cut < len(control); cut++ {
		// Split inside the control line, then deliver the rest including the
		// whole oversized payload in one read.
		c, ch := parserTestConn()
		if err := c.parse([]byte(stream[:cut])); err != nil {
			t.Fatalf("cut %d control chunk: %v", cut, err)
		}
		if err := c.parse([]byte(stream[cut:])); err != nil {
			t.Fatalf("cut %d remainder: %v", cut, err)
		}
		if got := collect(ch); !equalStrings(got, []string{payload, "tail"}) {
			t.Fatalf("cut %d: delivered %d msgs", cut, len(got))
		}
	}
}

// TestParserControlLineTooLong verifies that an unterminated control line is
// rejected at the bound rather than growing without limit.
func TestParserControlLineTooLong(t *testing.T) {
	c, _ := parserTestConn()
	var err error
	// A valid MSG prefix, then an argument line that never terminates.
	if err = c.parse([]byte("MSG ")); err != nil {
		t.Fatalf("prefix: %v", err)
	}
	chunk := []byte(strings.Repeat("x", 4096))
	for i := 0; i < maxControlLineArgSize/len(chunk)+3 && err == nil; i++ {
		err = c.parse(chunk)
	}
	if !errors.Is(err, ErrParserControlLineTooLong) {
		t.Fatalf("expected ErrParserControlLineTooLong, got %v", err)
	}
	if c.ps.argBuf == nil || len(c.ps.argBuf) > maxControlLineArgSize {
		t.Fatalf("argBuf grew to %d", len(c.ps.argBuf))
	}
}

// TestParserResetClearsAllState verifies that a reset returns every buffer and
// counter to its zero state, and that it is driven by a new generation.
func TestParserResetClearsAllState(t *testing.T) {
	c, ch := parserTestConn()
	// Accumulate a partial payload and a partial control line so both the
	// message buffer and the argument buffer hold residual bytes.
	if err := c.parse([]byte("MSG a 1 10\r\nabc")); err != nil {
		t.Fatalf("parse partial payload: %v", err)
	}
	if c.ps.msgBuf == nil {
		t.Fatal("expected a partially received payload")
	}
	// Drive the parser into a terminal error while it still holds the
	// partial payload, so the reset has both buffers to clear.
	if err := c.parserInputError(errors.New("EOF")); !errors.Is(err, ErrParserTruncated) {
		t.Fatalf("expected ErrParserTruncated, got %v", err)
	}
	gen := c.ps.gen

	c.mu.Lock()
	c.resetParserLocked()
	c.mu.Unlock()

	ps := c.ps
	if ps.gen == gen || ps.gen != c.parserGen {
		t.Fatalf("generation not advanced: ps.gen=%d conn.gen=%d old=%d", ps.gen, c.parserGen, gen)
	}
	if ps.err != nil || ps.state != OP_START || ps.as != 0 || ps.drop != 0 {
		t.Fatalf("state not cleared: %+v", ps)
	}
	if ps.argBuf != nil || ps.msgBuf != nil || ps.msgCopied || ps.consumed != 0 || ps.payloadLen != 0 {
		t.Fatalf("buffers/counters not cleared: argBuf=%v msgBuf=%v consumed=%d",
			ps.argBuf, ps.msgBuf, ps.consumed)
	}
	for _, b := range ps.scratch {
		if b != 0 {
			t.Fatal("scratch buffer retained a residual byte")
		}
	}
	// The new generation must parse cleanly.
	if err := c.parse([]byte(protoMsg("a", 1, "fresh"))); err != nil {
		t.Fatalf("parse after reset: %v", err)
	}
	if got := collect(ch); !equalStrings(got, []string{"fresh"}) {
		t.Fatalf("delivered %q", got)
	}
}

// TestParserStateAccounting verifies the diagnostics recorded for a frame.
func TestParserStateAccounting(t *testing.T) {
	c, _ := parserTestConn()
	// A complete frame, then a frame whose declared payload is longer than
	// what arrives: the parser records the declared length and consumed bytes.
	if err := c.parse([]byte(protoMsg("a", 1, "ok"))); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if c.ps.consumed != int64(len(protoMsg("a", 1, "ok"))) {
		t.Fatalf("consumed = %d", c.ps.consumed)
	}
	if c.ps.state != OP_START {
		t.Fatalf("state = %d", c.ps.state)
	}
	if err := c.parse([]byte("MSG a 1 50\r\nabc")); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if c.ps.payloadLen != 50 {
		t.Fatalf("payloadLen = %d, want 50", c.ps.payloadLen)
	}
	if c.ps.state != MSG_PAYLOAD {
		t.Fatalf("state = %d, want MSG_PAYLOAD", c.ps.state)
	}
	// Ending the input mid-frame is a truncation and freezes consumption.
	truncErr := c.parserInputError(errors.New("EOF"))
	if !errors.Is(truncErr, ErrParserTruncated) {
		t.Fatalf("expected truncation, got %v", truncErr)
	}
	frozen := c.ps.consumed
	if err := c.parse([]byte("def")); err != truncErr {
		t.Fatalf("expected sticky error, got %v", err)
	}
	if c.ps.consumed != frozen {
		t.Fatalf("consumed advanced after terminal failure: %d -> %d", frozen, c.ps.consumed)
	}
}

// TestParserEmptyPayloadAndHeader covers zero-length frames, which have no
// payload byte before the terminator.
func TestParserEmptyPayloadAndHeader(t *testing.T) {
	c, ch := parserTestConn()
	// MSG with an empty payload, then HMSG whose whole size is the header
	// block (empty body), then a normal message.
	stream := "MSG a 1 0\r\n\r\n" +
		"HMSG a 1 12 12\r\nNATS/1.0\r\n\r\n" + "\r\n" +
		protoMsg("a", 1, "z")
	if err := feed(c, []byte(stream), 1); err != nil {
		t.Fatalf("parse: %v", err)
	}
	// The header block is decoded out of the payload, leaving it empty.
	if got := collect(ch); !equalStrings(got, []string{"", "", "z"}) {
		t.Fatalf("delivered %q", got)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
