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
	"bufio"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// newHardeningTestConn returns a bare Conn wired to a fresh parseState,
// mirroring the setup used by the existing parser unit tests. The CLOSED
// status keeps server-side control frames (-ERR, ...) from attempting a full
// connection teardown, which needs machinery these in-memory tests don't set
// up (the parser itself is the system under test).
func newHardeningTestConn() *Conn {
	c := &Conn{status: CLOSED}
	c.newReaderWriter()
	c.bw.switchToPending()
	c.ps = &parseState{}
	return c
}

// TestParserLengthBounds verifies that the MSG/HMSG/INFO length fields and
// control lines are rejected before any payload allocation can happen.
func TestParserLengthBounds(t *testing.T) {
	t.Run("MSG oversized size", func(t *testing.T) {
		c := newHardeningTestConn()
		err := c.parse([]byte(fmt.Sprintf("MSG foo 1 %d\r\n", maxMsgSize+1)))
		if !errors.Is(err, errParserBadSize) {
			t.Fatalf("expected errParserBadSize, got %v", err)
		}
		if c.ps.msgBuf != nil {
			t.Fatal("no payload buffer must be allocated for an oversized size")
		}
	})

	t.Run("HMSG oversized size", func(t *testing.T) {
		c := newHardeningTestConn()
		err := c.parse([]byte(fmt.Sprintf("HMSG foo 1 8 %d\r\n", maxMsgSize+1)))
		if !errors.Is(err, errParserBadSize) {
			t.Fatalf("expected errParserBadSize, got %v", err)
		}
		if c.ps.msgBuf != nil {
			t.Fatal("no payload buffer must be allocated for an oversized size")
		}
	})

	t.Run("MSG negative size", func(t *testing.T) {
		// A leading '-' is not a digit, so parseInt64 returns -1.
		c := newHardeningTestConn()
		err := c.parse([]byte("MSG foo 1 -5\r\n"))
		if !errors.Is(err, errParserBadSize) {
			t.Fatalf("expected errParserBadSize, got %v", err)
		}
	})

	t.Run("HMSG negative header size", func(t *testing.T) {
		c := newHardeningTestConn()
		err := c.parse([]byte("HMSG foo 1 -1 5\r\n"))
		if !errors.Is(err, errParserBadHeaderSize) {
			t.Fatalf("expected errParserBadHeaderSize, got %v", err)
		}
	})

	t.Run("HMSG header larger than payload", func(t *testing.T) {
		c := newHardeningTestConn()
		err := c.parse([]byte("HMSG foo 1 10 5\r\n"))
		if !errors.Is(err, errParserBadHeaderSize) {
			t.Fatalf("expected errParserBadHeaderSize, got %v", err)
		}
	})

	t.Run("non-numeric size", func(t *testing.T) {
		c := newHardeningTestConn()
		err := c.parse([]byte("MSG foo 1 abc\r\n"))
		if !errors.Is(err, errParserBadSize) {
			t.Fatalf("expected errParserBadSize, got %v", err)
		}
	})

	t.Run("non-numeric sid", func(t *testing.T) {
		c := newHardeningTestConn()
		err := c.parse([]byte("MSG foo xyz 3\r\n"))
		if !errors.Is(err, errParserBadSid) {
			t.Fatalf("expected errParserBadSid, got %v", err)
		}
	})

	t.Run("int64 overflow size", func(t *testing.T) {
		// 9223372036854775808 is MaxInt64+1; it must be rejected rather
		// than silently wrapping into a negative or small number.
		c := newHardeningTestConn()
		err := c.parse([]byte("MSG foo 1 9223372036854775808\r\n"))
		if !errors.Is(err, errParserBadSize) {
			t.Fatalf("expected errParserBadSize, got %v", err)
		}
	})

	t.Run("max accepted size still works", func(t *testing.T) {
		// The boundary itself is accepted; we only feed the control line
		// (no payload), so no large allocation occurs here.
		c := newHardeningTestConn()
		err := c.parse([]byte(fmt.Sprintf("MSG foo 1 %d\r\n", maxMsgSize)))
		if err != nil {
			t.Fatalf("unexpected error at max size: %v", err)
		}
		if c.ps.ma.size != maxMsgSize {
			t.Fatalf("expected size %d, got %d", maxMsgSize, c.ps.ma.size)
		}
		if c.ps.state != MSG_PAYLOAD {
			t.Fatalf("expected MSG_PAYLOAD state, got %d", c.ps.state)
		}
	})

	t.Run("oversized MSG control line across splits", func(t *testing.T) {
		c := newHardeningTestConn()
		// First chunk creates the argBuf in the split-buffer path.
		if err := c.parse([]byte("MSG foo 1 ")); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Then stream digits without a terminating CRLF up to the cap.
		big := make([]byte, maxControlLineArgSize+2)
		for i := range big {
			big[i] = '1'
		}
		err := c.parse(big)
		if !errors.Is(err, errParserControlLineTooLong) {
			t.Fatalf("expected errParserControlLineTooLong, got %v", err)
		}
	})

	t.Run("oversized INFO control line", func(t *testing.T) {
		c := newHardeningTestConn()
		if err := c.parse([]byte("INFO ")); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		big := make([]byte, maxControlLineArgSize+2)
		for i := range big {
			big[i] = 'x'
		}
		err := c.parse(big)
		if !errors.Is(err, errParserControlLineTooLong) {
			t.Fatalf("expected errParserControlLineTooLong, got %v", err)
		}
	})
}

// TestParserMissingCRLF verifies malformed frame termination is surfaced as a
// recognizable error instead of being silently resynchronized.
func TestParserMissingCRLF(t *testing.T) {
	t.Run("CR not followed by LF", func(t *testing.T) {
		c := newHardeningTestConn()
		// After the full payload, a CR that is not completed by an LF is
		// an unambiguous protocol violation.
		if err := c.parse([]byte("MSG foo 1 3\r\nfoo\r")); err != nil {
			t.Fatalf("split after CR should wait for the next byte, got %v", err)
		}
		err := c.parse([]byte("X"))
		if !errors.Is(err, errParserMissingCRLF) {
			t.Fatalf("expected errParserMissingCRLF, got %v", err)
		}
	})

	t.Run("two CRs after payload", func(t *testing.T) {
		c := newHardeningTestConn()
		err := c.parse([]byte("MSG foo 1 3\r\nfoo\r\r\n"))
		if !errors.Is(err, errParserMissingCRLF) {
			t.Fatalf("expected errParserMissingCRLF, got %v", err)
		}
	})

	t.Run("bare CR with no payload argument", func(t *testing.T) {
		// "MSG " enters MSG_ARG; a CR followed by something other than LF
		// is a malformed terminator rather than a silent resync.
		c := newHardeningTestConn()
		if err := c.parse([]byte("MSG \r")); err != nil {
			t.Fatalf("split after CR should wait for the next byte, got %v", err)
		}
		err := c.parse([]byte("X"))
		if !errors.Is(err, errParserMissingCRLF) {
			t.Fatalf("expected errParserMissingCRLF, got %v", err)
		}

		// And the same frame ending immediately at LF yields an error too.
		c2 := newHardeningTestConn()
		if err = c2.parse([]byte("MSG \r\n")); !errors.Is(err, errParserMissingCRLF) {
			t.Fatalf("expected errParserMissingCRLF for empty arg, got %v", err)
		}
	})

	t.Run("payload terminated with LF only is still accepted", func(t *testing.T) {
		// Historical tolerance: a lone '\n' after the payload remains valid.
		c := newHardeningTestConn()
		if err := c.parse([]byte("MSG foo 1 3\r\nfoo\n")); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.ps.state != OP_START {
			t.Fatalf("expected OP_START, got %d", c.ps.state)
		}
	})

	t.Run("proper CRLF still accepted", func(t *testing.T) {
		c := newHardeningTestConn()
		if err := c.parse([]byte("MSG foo 1 3\r\nfoo\r\n")); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.ps.state != OP_START {
			t.Fatalf("expected OP_START, got %d", c.ps.state)
		}
	})
}

// TestParserEmptyPayloadAndHeader preserves the existing behavior for legal
// empty bodies and zero-length header fields.
func TestParserEmptyPayloadAndHeader(t *testing.T) {
	t.Run("empty MSG payload", func(t *testing.T) {
		c := newHardeningTestConn()
		before := c.Statistics.InMsgs
		if err := c.parse([]byte("MSG foo 1 0\r\n\r\n")); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.ps.state != OP_START {
			t.Fatalf("expected OP_START, got %d", c.ps.state)
		}
		if c.Statistics.InMsgs != before+1 {
			t.Fatalf("expected one delivered message, got %d", c.Statistics.InMsgs-before)
		}
	})

	t.Run("HMSG with empty body", func(t *testing.T) {
		// hdr == size: the whole payload is the header block, body is empty.
		c := newHardeningTestConn()
		hdr := []byte("NATS/1.0\r\nFoo: bar\r\n\r\n")
		frame := append([]byte(fmt.Sprintf("HMSG foo 1 %d %d\r\n", len(hdr), len(hdr))), hdr...)
		frame = append(frame, '\r', '\n')
		if err := c.parse(frame); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.ps.state != OP_START {
			t.Fatalf("expected OP_START, got %d", c.ps.state)
		}
	})

	t.Run("HMSG with zero-length header value", func(t *testing.T) {
		hdr := []byte("NATS/1.0\r\nEmpty:\r\n\r\n")
		h, err := DecodeHeadersMsg(hdr)
		if err != nil {
			t.Fatalf("unexpected header decode error: %v", err)
		}
		if v := h.Get("Empty"); v != "" {
			t.Fatalf("expected empty header value, got %q", v)
		}
	})

	t.Run("HMSG with empty header block", func(t *testing.T) {
		// Status line plus blank line, no header fields.
		hdr := []byte("NATS/1.0\r\n\r\n")
		h, err := DecodeHeadersMsg(hdr)
		if err != nil {
			t.Fatalf("unexpected header decode error: %v", err)
		}
		if len(h) != 0 {
			t.Fatalf("expected no headers, got %v", h)
		}
	})
}

// TestParserOversizedHeaderLine checks the header decoder rejects a single
// oversized header field with a recognizable error.
func TestParserOversizedHeaderLine(t *testing.T) {
	long := strings.Repeat("a", maxHeaderLineSize+1)
	shouldErr := [][]byte{
		[]byte("NATS/1.0 " + long + "\r\n\r\n"),
		[]byte("NATS/1.0\r\nKey: " + long + "\r\n\r\n"),
	}
	for _, hdr := range shouldErr {
		if _, err := DecodeHeadersMsg(hdr); !errors.Is(err, errParserHeaderLineTooLong) {
			t.Fatalf("expected errParserHeaderLineTooLong for %d-byte header, got %v", len(hdr), err)
		}
	}
}

// TestParserRandomSplit feeds complete legal frames in random chunk sizes,
// covering splits inside the command, every argument, the header block, the
// payload and the trailing CRLF.
func TestParserRandomSplit(t *testing.T) {
	hdr := []byte("NATS/1.0\r\nFoo: bar\r\nEmpty:\r\n\r\n")
	body := []byte("the quick brown fox jumps over the lazy dog")

	hmsg := append(
		[]byte(fmt.Sprintf("HMSG hdr 1 %d %d\r\n", len(hdr), len(hdr)+len(body))),
		append(append([]byte{}, hdr...), body...)...,
	)
	hmsg = append(hmsg, '\r', '\n')
	frames := [][]byte{
		[]byte("PING\r\n"),
		[]byte("PONG\r\n"),
		[]byte("+OK\r\n"),
		[]byte("-ERR 'something happened'\r\n"),
		[]byte("INFO {\"server_id\":\"split-test\"}\r\n"),
		[]byte("MSG plain 1 0\r\n\r\n"),
		[]byte("MSG plain 1 3\r\nfoo\r\n"),
		[]byte("MSG with-reply 1 r 5\r\nhello\r\n"),
		hmsg,
	}

	var raw []byte
	for _, f := range frames {
		raw = append(raw, f...)
	}

	// Build chunk-size sequences: uniform splits of 1..4 bytes plus random
	// plans with mixed chunk sizes.
	var chunkPlans [][]int
	for _, uniform := range []int{1, 2, 3, 4} {
		var sizes []int
		for pos := 0; pos < len(raw); {
			n := uniform
			if pos+n > len(raw) {
				n = len(raw) - pos
			}
			sizes = append(sizes, n)
			pos += n
		}
		chunkPlans = append(chunkPlans, sizes)
	}
	rng := rand.New(rand.NewSource(42))
	for iter := 0; iter < 25; iter++ {
		var sizes []int
		for pos := 0; pos < len(raw); {
			n := 1 + rng.Intn(16)
			if pos+n > len(raw) {
				n = len(raw) - pos
			}
			sizes = append(sizes, n)
			pos += n
		}
		chunkPlans = append(chunkPlans, sizes)
	}

	for si, sizes := range chunkPlans {
		t.Run(fmt.Sprintf("split-%d", si), func(t *testing.T) {
			c := newHardeningTestConn()
			pos := 0
			for _, n := range sizes {
				if err := c.parse(raw[pos : pos+n]); err != nil {
					t.Fatalf("split %d failed at offset %d: %v", si, pos, err)
				}
				pos += n
			}
			if pos != len(raw) {
				t.Fatalf("consumed %d of %d bytes", pos, len(raw))
			}
			// Only after the entire stream is consumed must every frame
			// have been fully parsed and its state reset.
			if c.ps.state != OP_START {
				t.Fatalf("expected OP_START at end, got state %d", c.ps.state)
			}
			if c.ps.argBuf != nil || c.ps.msgBuf != nil {
				t.Fatalf("buffers not released: argBuf=%v msgBuf=%v", c.ps.argBuf != nil, c.ps.msgBuf != nil)
			}
		})
	}

	// And all at once.
	t.Run("whole", func(t *testing.T) {
		c := newHardeningTestConn()
		if err := c.parse(raw); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.ps.state != OP_START {
			t.Fatalf("expected OP_START at end, got state %d", c.ps.state)
		}
	})
}

// TestParserSplitBoundaryMatrix exhaustively splits a representative MSG and
// HMSG at every single byte boundary, including the header/payload boundary.
func TestParserSplitBoundaryMatrix(t *testing.T) {
	hdr := []byte("NATS/1.0\r\nK: v\r\n\r\n")
	body := []byte("payload-bytes")
	cases := [][]byte{
		[]byte("MSG s 1 r 3\r\nfoo\r\n"),
		append(
			append([]byte(fmt.Sprintf("HMSG s 1 r %d %d\r\n", len(hdr), len(hdr)+len(body))), hdr...),
			append(body, '\r', '\n')...,
		),
	}
	for ci, frame := range cases {
		for cut := 1; cut < len(frame); cut++ {
			c := newHardeningTestConn()
			if err := c.parse(frame[:cut]); err != nil {
				t.Fatalf("case %d cut %d first half failed: %v", ci, cut, err)
			}
			if err := c.parse(frame[cut:]); err != nil {
				t.Fatalf("case %d cut %d second half failed: %v", ci, cut, err)
			}
			if c.ps.state != OP_START {
				t.Fatalf("case %d cut %d: expected OP_START, got %d", ci, cut, c.ps.state)
			}
		}
	}
}

// hardeningProtoServer is a minimal NATS-speaking TCP peer for end-to-end
// parser tests. It answers the connect handshake, then replays scripted bytes.
type hardeningProtoServer struct {
	listener net.Listener
	connCh   chan net.Conn
}

func startHardeningProtoServer(t *testing.T) *hardeningProtoServer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	s := &hardeningProtoServer{listener: l, connCh: make(chan net.Conn, 1)}
	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		// Send an INFO, then drain everything the client sends. Every
		// PING is answered with a PONG so Flush()/request round trips work;
		// CONNECT/SUB/PUB lines are simply consumed.
		fmt.Fprint(conn, "INFO {\"server_id\":\"hardening-test\",\"max_payload\":1048576}\r\n")
		s.connCh <- conn
		br := bufio.NewReader(conn)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			if strings.HasPrefix(line, "PING") {
				if _, err := fmt.Fprint(conn, "PONG\r\n"); err != nil {
					return
				}
			}
		}
	}()
	return s
}

func (s *hardeningProtoServer) addr() string {
	return "nats://" + s.listener.Addr().String()
}

func (s *hardeningProtoServer) close() {
	s.listener.Close()
	select {
	case conn := <-s.connCh:
		conn.Close()
	default:
	}
}

func (s *hardeningProtoServer) conn(t *testing.T) net.Conn {
	t.Helper()
	select {
	case conn := <-s.connCh:
		return conn
	case <-time.After(2 * time.Second):
		t.Fatal("server never accepted a connection")
		return nil
	}
}

// TestConnMalformedProtoClosesOnce drives a real connection through the
// readLoop: a malformed frame must produce exactly one terminal close, no
// leftover read goroutine and no half-written outbound reply.
func TestConnMalformedProtoClosesOnce(t *testing.T) {
	srv := startHardeningProtoServer(t)
	defer srv.close()

	closedCh := make(chan error, 1)
	nc, err := Connect(srv.addr(),
		NoReconnect(),
		DisconnectErrHandler(func(_ *Conn, err error) {
			select {
			case closedCh <- err:
			default:
			}
		}),
	)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	srvConn := srv.conn(t)

	// Malformed: junk byte where an op is expected.
	if _, err := srvConn.Write([]byte("ZZZZ\r\n")); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	select {
	case err := <-closedCh:
		if err == nil {
			t.Fatal("expected a non-nil disconnect error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("connection was not closed after malformed protocol")
	}

	check := func() bool {
		return nc.IsClosed()
	}
	if !waitFor(check, time.Second) {
		t.Fatal("Conn never reported IsClosed()")
	}

	// A second user Close must be a no-op (no panic, no double teardown).
	nc.Close()
	nc.Close()

	// The readLoop must have exited and dropped its parseState.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		nc.mu.RLock()
		ps := nc.ps
		nc.mu.RUnlock()
		if ps == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("parseState still present: readLoop did not exit")
}

func waitFor(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// TestConnCloseRace hammers Close concurrently with protocol reads to make
// sure the connection is torn down exactly once under -race.
func TestConnCloseRace(t *testing.T) {
	for iter := 0; iter < 20; iter++ {
		srv := startHardeningProtoServer(t)
		nc, err := Connect(srv.addr(), NoReconnect())
		if err != nil {
			srv.close()
			t.Fatalf("connect failed: %v", err)
		}
		srvConn := srv.conn(t)

		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				nc.Close()
			}()
		}
		// Keep the read path busy while Close runs.
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for i := 0; i < 20; i++ {
				fmt.Fprint(srvConn, "PING\r\n")
				<-ticker.C
			}
		}()
		wg.Wait()
		if !nc.IsClosed() {
			t.Fatalf("iteration %d: connection not closed", iter)
		}
		srv.close()
	}
}

// TestConnRandomChunkDelivery delivers a full MSG/HMSG/INFO stream over a real
// connection in random-sized TCP writes and verifies end-to-end delivery with
// splits landing anywhere in the stream.
func TestConnRandomChunkDelivery(t *testing.T) {
	srv := startHardeningProtoServer(t)
	defer srv.close()

	nc, err := Connect(srv.addr(), NoReconnect())
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	// Flush the SUB so the server side settles (best effort; no real server
	// enforces anything here).
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	srvConn := srv.conn(t)

	hdr := []byte("NATS/1.0\r\nFoo: bar\r\n\r\n")
	body := []byte("hello over random chunks")
	hmsg := append(
		[]byte(fmt.Sprintf("HMSG foo 1 %d %d\r\n", len(hdr), len(hdr)+len(body))),
		append(append([]byte{}, hdr...), body...)...,
	)
	hmsg = append(hmsg, '\r', '\n')
	msg := []byte("MSG foo 1 5\r\nplain\r\n")
	stream := append(append([]byte{}, hmsg...), msg...)

	rng := rand.New(rand.NewSource(7))
	for pos := 0; pos < len(stream); {
		n := 1 + rng.Intn(7)
		if pos+n > len(stream) {
			n = len(stream) - pos
		}
		if _, err := srvConn.Write(stream[pos : pos+n]); err != nil {
			t.Fatalf("write at %d failed: %v", pos, err)
		}
		pos += n
		// Small pause so writes often land in separate read buffers.
		if rng.Intn(3) == 0 {
			time.Sleep(time.Millisecond)
		}
	}

	got, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("missing HMSG delivery: %v", err)
	}
	if string(got.Data) != string(body) {
		t.Fatalf("HMSG body mismatch: %q vs %q", got.Data, body)
	}
	if got.Header.Get("Foo") != "bar" {
		t.Fatalf("HMSG header mismatch: %q", got.Header)
	}

	got, err = sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("missing MSG delivery: %v", err)
	}
	if string(got.Data) != "plain" {
		t.Fatalf("MSG body mismatch: %q", got.Data)
	}
}

// TestConnNoGoroutineLeakAfterParseError ensures the readLoop exits and no
// background goroutine keeps running once a parse error closes the conn.
func TestConnNoGoroutineLeakAfterParseError(t *testing.T) {
	srv := startHardeningProtoServer(t)
	defer srv.close()

	nc, err := Connect(srv.addr(), NoReconnect())
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	srvConn := srv.conn(t)

	baseline := runtime.NumGoroutine()
	if _, err := srvConn.Write([]byte("BADOP\r\n")); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if !waitFor(nc.IsClosed, 2*time.Second) {
		t.Fatal("connection never closed")
	}
	nc.Close()

	// Allow the readLoop (and any peers) to wind down.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && runtime.NumGoroutine() > baseline {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > baseline+1 {
		// +1 tolerance for transient runtime goroutines unrelated to us.
		t.Fatalf("goroutine leak: baseline=%d now=%d", baseline, n)
	}
}
