// Copyright 2012-2023 The NATS Authors
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
	"fmt"
	"math"
)

type msgArg struct {
	subject []byte
	reply   []byte
	sid     int64
	hdr     int
	size    int
}

const MAX_CONTROL_LINE_SIZE = 4096

const (
	// maxControlLineArgSize bounds the argument portion of one control line.
	// A peer that never terminates a line must not be able to grow argBuf
	// without bound, and an unbounded line makes truncation indistinguishable
	// from a slow writer.
	maxControlLineArgSize = 64 * 1024

	// maxMsgSize bounds a declared MSG/HMSG payload length. It is validated
	// before any payload allocation, so a forged length can only fail the
	// frame. It is deliberately above the largest payload a conformant server
	// negotiates (MaxPayload, 64MiB).
	maxMsgSize = 128 * 1024 * 1024

	// maxParserErrorSnippet limits how much unparsed input a parse error
	// quotes, so a bad frame can not turn into a huge error string.
	maxParserErrorSnippet = 32
)

// parseState is the protocol parser state. Besides the state machine it keeps
// the accounting needed to describe where a failure happened (current control
// line, declared payload length and bytes consumed) and, once a frame fails,
// the sticky terminal error.
type parseState struct {
	state     int
	as        int
	drop      int
	hdr       int
	ma        msgArg
	argBuf    []byte
	msgBuf    []byte
	msgCopied bool

	// lineLen is the number of argument bytes accumulated for the control
	// line currently being parsed.
	lineLen int
	// payloadLen is the declared payload length of the current frame.
	payloadLen int
	// consumed is the number of bytes the parser has consumed since the
	// generation was established. It stops advancing once err is set.
	consumed int64

	// err, when non-nil, is the sticky terminal error. The parser does not
	// consume any more input until a new connection generation resets it.
	err error
	// gen is the connection generation this state belongs to.
	gen uint64

	scratch [MAX_CONTROL_LINE_SIZE]byte
}

// reset clears every residual byte and all frame accounting. It is used when
// the parser is adopted by a new connection generation, and nowhere else.
func (ps *parseState) reset() {
	ps.state = OP_START
	ps.as = 0
	ps.drop = 0
	ps.hdr = 0
	ps.ma = msgArg{}
	ps.argBuf = nil
	ps.msgBuf = nil
	ps.msgCopied = false
	ps.lineLen = 0
	ps.payloadLen = 0
	ps.consumed = 0
	ps.err = nil
	// Drop any bytes left in the scratch buffer so they can not be mistaken
	// for input belonging to the new generation.
	clear(ps.scratch[:])
}

const (
	OP_START = iota
	OP_PLUS
	OP_PLUS_O
	OP_PLUS_OK
	OP_MINUS
	OP_MINUS_E
	OP_MINUS_ER
	OP_MINUS_ERR
	OP_MINUS_ERR_SPC
	MINUS_ERR_ARG
	OP_M
	OP_MS
	OP_MSG
	OP_MSG_SPC
	MSG_ARG
	MSG_PAYLOAD
	MSG_END
	MSG_END_CR
	OP_H
	OP_P
	OP_PI
	OP_PIN
	OP_PING
	OP_PO
	OP_PON
	OP_PONG
	OP_I
	OP_IN
	OP_INF
	OP_INFO
	OP_INFO_SPC
	INFO_ARG
)

// parserFail records err as the sticky terminal state and returns it. Every
// later parse call returns this exact error until the connection generation
// changes.
func (nc *Conn) parserFail(err error) error {
	if nc.ps != nil {
		nc.ps.err = err
	}
	return err
}

// parse is the fast protocol parser engine.
//
// A parse error is terminal for the current connection generation: the state
// is not rewound and later calls return the same error without consuming
// input. Complete messages parsed before the failing frame have already been
// handed to processMsg; a partial frame is never delivered.
func (nc *Conn) parse(buf []byte) error {
	if nc.ps == nil {
		nc.ps = &parseState{}
	}
	ps := nc.ps
	// A terminal failure is sticky: do not guess at a boundary.
	if ps.err != nil {
		return ps.err
	}

	var i int
	var b byte

	// Move to loop instead of range syntax to allow jumping of i
	for i = 0; i < len(buf); i++ {
		b = buf[i]

		switch ps.state {
		case OP_START:
			switch b {
			case 'M', 'm':
				ps.state = OP_M
				ps.hdr = -1
				ps.ma.hdr = -1
			case 'H', 'h':
				ps.state = OP_H
				ps.hdr = 0
				ps.ma.hdr = 0
			case 'P', 'p':
				ps.state = OP_P
			case '+':
				ps.state = OP_PLUS
			case '-':
				ps.state = OP_MINUS
			case 'I', 'i':
				ps.state = OP_I
			default:
				goto parseErr
			}
		case OP_H:
			switch b {
			case 'M', 'm':
				ps.state = OP_M
			default:
				goto parseErr
			}
		case OP_M:
			switch b {
			case 'S', 's':
				ps.state = OP_MS
			default:
				goto parseErr
			}
		case OP_MS:
			switch b {
			case 'G', 'g':
				ps.state = OP_MSG
			default:
				goto parseErr
			}
		case OP_MSG:
			switch b {
			case ' ', '\t':
				ps.state = OP_MSG_SPC
			default:
				goto parseErr
			}
		case OP_MSG_SPC:
			switch b {
			case ' ', '\t':
				continue
			case '\r', '\n':
				// The argument line ended before it began. Do not scan on:
				// the next frame would be consumed as argument bytes.
				ps.consumed += int64(i)
				return nc.parserFail(fmt.Errorf("%w: empty message argument line", ErrParserProtocol))
			default:
				ps.state = MSG_ARG
				ps.as = i
				ps.lineLen = 0
			}
		case MSG_ARG:
			switch b {
			case '\r':
				ps.drop = 1
			case '\n':
				arg, err := msgArgBytes(ps, buf, i)
				if err != nil {
					ps.consumed += int64(i)
					return nc.parserFail(err)
				}
				if err := nc.processMsgArgs(arg); err != nil {
					ps.consumed += int64(i)
					return nc.parserFail(err)
				}
				ps.drop, ps.as, ps.state = 0, i+1, MSG_PAYLOAD
				ps.payloadLen = ps.ma.size
				ps.lineLen = 0

				// Jump ahead with the index. If this overruns
				// what is left we fall out and process a split buffer.
				// A zero-size payload has no final byte, so enter the
				// payload state at the terminator instead.
				if ps.ma.size > 0 {
					i = ps.as + ps.ma.size - 1
				} else {
					i = ps.as - 1
				}
			default:
				// A CR already ended the line; anything but LF after it is
				// a protocol error, not an argument byte.
				if ps.drop > 0 {
					goto parseErr
				}
				if ps.argBuf != nil {
					if len(ps.argBuf) >= maxControlLineArgSize {
						ps.consumed += int64(i)
						return nc.parserFail(fmt.Errorf("%w: %d bytes in control line",
							ErrParserControlLineTooLong, len(ps.argBuf)))
					}
					ps.argBuf = append(ps.argBuf, b)
					ps.lineLen = len(ps.argBuf)
				}
			}
		case MSG_PAYLOAD:
			if ps.msgBuf != nil {
				if len(ps.msgBuf) >= ps.ma.size {
					nc.processMsg(ps.msgBuf)
					ps.argBuf, ps.msgBuf, ps.msgCopied = nil, nil, false
					ps.lineLen = 0
					// buf[i] is the first terminator byte. Reprocess it in
					// MSG_END instead of skipping it: skipping it is what
					// let a malformed terminator consume the next frame.
					ps.state = MSG_END
					i--
				} else {
					// copy as much as we can to the buffer and skip ahead.
					toCopy := ps.ma.size - len(ps.msgBuf)
					avail := len(buf) - i

					if avail < toCopy {
						toCopy = avail
					}

					if toCopy > 0 {
						start := len(ps.msgBuf)
						// This is needed for copy to work.
						ps.msgBuf = ps.msgBuf[:start+toCopy]
						copy(ps.msgBuf[start:], buf[i:i+toCopy])
						// Update our index
						i = (i + toCopy) - 1
					} else {
						ps.msgBuf = append(ps.msgBuf, b)
					}
				}
			} else if i-ps.as >= ps.ma.size {
				// buf[as:i] is exactly the payload; buf[i] is the first
				// terminator byte. Reprocess it in MSG_END so a bad
				// terminator fails at the boundary instead of scanning
				// into the following frame.
				nc.processMsg(buf[ps.as:i])
				ps.argBuf, ps.msgBuf, ps.msgCopied = nil, nil, false
				ps.state = MSG_END
				i--
			}
		case MSG_END:
			switch b {
			case '\r':
				ps.state = MSG_END_CR
			case '\n':
				// Historical tolerance for a lone LF terminator.
				ps.finishFrame(i)
			default:
				ps.consumed += int64(i)
				return nc.parserFail(fmt.Errorf("%w: expected CRLF after payload, got %q",
					ErrParserProtocol, b))
			}
		case MSG_END_CR:
			if b != '\n' {
				ps.consumed += int64(i)
				return nc.parserFail(fmt.Errorf("%w: expected LF after CR, got %q",
					ErrParserProtocol, b))
			}
			ps.finishFrame(i)
		case OP_PLUS:
			switch b {
			case 'O', 'o':
				ps.state = OP_PLUS_O
			default:
				goto parseErr
			}
		case OP_PLUS_O:
			switch b {
			case 'K', 'k':
				ps.state = OP_PLUS_OK
			default:
				goto parseErr
			}
		case OP_PLUS_OK:
			switch b {
			case '\n':
				nc.processOK()
				ps.drop, ps.state = 0, OP_START
			}
		case OP_MINUS:
			switch b {
			case 'E', 'e':
				ps.state = OP_MINUS_E
			default:
				goto parseErr
			}
		case OP_MINUS_E:
			switch b {
			case 'R', 'r':
				ps.state = OP_MINUS_ER
			default:
				goto parseErr
			}
		case OP_MINUS_ER:
			switch b {
			case 'R', 'r':
				ps.state = OP_MINUS_ERR
			default:
				goto parseErr
			}
		case OP_MINUS_ERR:
			switch b {
			case ' ', '\t':
				ps.state = OP_MINUS_ERR_SPC
			default:
				goto parseErr
			}
		case OP_MINUS_ERR_SPC:
			switch b {
			case ' ', '\t':
				continue
			default:
				ps.state = MINUS_ERR_ARG
				ps.as = i
				ps.lineLen = 0
			}
		case MINUS_ERR_ARG:
			switch b {
			case '\r':
				ps.drop = 1
			case '\n':
				arg, err := controlLineBytes(ps, buf, i)
				if err != nil {
					ps.consumed += int64(i)
					return nc.parserFail(err)
				}
				ps.argBuf = nil
				nc.processErr(string(arg))
				ps.drop, ps.as, ps.state = 0, i+1, OP_START
				ps.lineLen = 0
			default:
				if ps.drop > 0 {
					goto parseErr
				}
				if ps.argBuf != nil {
					if len(ps.argBuf) >= maxControlLineArgSize {
						ps.consumed += int64(i)
						return nc.parserFail(fmt.Errorf("%w: %d bytes in control line",
							ErrParserControlLineTooLong, len(ps.argBuf)))
					}
					ps.argBuf = append(ps.argBuf, b)
					ps.lineLen = len(ps.argBuf)
				}
			}
		case OP_P:
			switch b {
			case 'I', 'i':
				ps.state = OP_PI
			case 'O', 'o':
				ps.state = OP_PO
			default:
				goto parseErr
			}
		case OP_PO:
			switch b {
			case 'N', 'n':
				ps.state = OP_PON
			default:
				goto parseErr
			}
		case OP_PON:
			switch b {
			case 'G', 'g':
				ps.state = OP_PONG
			default:
				goto parseErr
			}
		case OP_PONG:
			switch b {
			case '\n':
				nc.processPong()
				ps.drop, ps.state = 0, OP_START
			}
		case OP_PI:
			switch b {
			case 'N', 'n':
				ps.state = OP_PIN
			default:
				goto parseErr
			}
		case OP_PIN:
			switch b {
			case 'G', 'g':
				ps.state = OP_PING
			default:
				goto parseErr
			}
		case OP_PING:
			switch b {
			case '\n':
				nc.processPing()
				ps.drop, ps.state = 0, OP_START
			}
		case OP_I:
			switch b {
			case 'N', 'n':
				ps.state = OP_IN
			default:
				goto parseErr
			}
		case OP_IN:
			switch b {
			case 'F', 'f':
				ps.state = OP_INF
			default:
				goto parseErr
			}
		case OP_INF:
			switch b {
			case 'O', 'o':
				ps.state = OP_INFO
			default:
				goto parseErr
			}
		case OP_INFO:
			switch b {
			case ' ', '\t':
				ps.state = OP_INFO_SPC
			default:
				goto parseErr
			}
		case OP_INFO_SPC:
			switch b {
			case ' ', '\t':
				continue
			default:
				ps.state = INFO_ARG
				ps.as = i
				ps.lineLen = 0
			}
		case INFO_ARG:
			switch b {
			case '\r':
				ps.drop = 1
			case '\n':
				arg, err := controlLineBytes(ps, buf, i)
				if err != nil {
					ps.consumed += int64(i)
					return nc.parserFail(err)
				}
				ps.argBuf = nil
				nc.processAsyncInfo(arg)
				ps.drop, ps.as, ps.state = 0, i+1, OP_START
				ps.lineLen = 0
			default:
				if ps.drop > 0 {
					goto parseErr
				}
				if ps.argBuf != nil {
					if len(ps.argBuf) >= maxControlLineArgSize {
						ps.consumed += int64(i)
						return nc.parserFail(fmt.Errorf("%w: %d bytes in control line",
							ErrParserControlLineTooLong, len(ps.argBuf)))
					}
					ps.argBuf = append(ps.argBuf, b)
					ps.lineLen = len(ps.argBuf)
				}
			}
		default:
			goto parseErr
		}
	}
	// Check for split buffer scenarios
	if (ps.state == MSG_ARG || ps.state == MINUS_ERR_ARG || ps.state == INFO_ARG) && ps.argBuf == nil {
		// i is len(buf) here. A trailing CR (drop == 1) must be excluded
		// from the captured argument bytes.
		end := i - ps.drop
		if end < ps.as {
			end = ps.as
		}
		if end-ps.as > maxControlLineArgSize {
			ps.consumed += int64(i)
			return nc.parserFail(fmt.Errorf("%w: %d bytes in control line",
				ErrParserControlLineTooLong, end-ps.as))
		}
		ps.argBuf = ps.scratch[:0]
		ps.argBuf = append(ps.argBuf, buf[ps.as:end]...)
		ps.lineLen = len(ps.argBuf)
	}
	// Check for split msg
	if ps.state == MSG_PAYLOAD && ps.msgBuf == nil {
		// We need to clone the msgArg if it is still referencing the
		// read buffer and we are not able to process the msg.
		if ps.argBuf == nil {
			nc.cloneMsgArg()
		}

		// If we will overflow the scratch buffer, just create a
		// new buffer to hold the split message.
		if ps.ma.size > cap(ps.scratch)-len(ps.argBuf) {
			lrem := len(buf[ps.as:])

			ps.msgBuf = make([]byte, lrem, ps.ma.size)
			copy(ps.msgBuf, buf[ps.as:])
			ps.msgCopied = true
		} else {
			ps.msgBuf = ps.scratch[len(ps.argBuf):len(ps.argBuf)]
			ps.msgBuf = append(ps.msgBuf, (buf[ps.as:])...)
		}
	}

	ps.consumed += int64(len(buf))
	return nil

parseErr:
	snippet := buf[i:]
	if len(snippet) > maxParserErrorSnippet {
		snippet = snippet[:maxParserErrorSnippet]
	}
	// The offending byte is not consumed, but everything before it was.
	ps.consumed += int64(i)
	return nc.parserFail(fmt.Errorf("%w [%d]: '%s'", ErrParserProtocol, ps.state, snippet))
}

// msgArgBytes returns the bytes of a MSG/HMSG argument line. It enforces the
// control line bound and reports an empty line as a protocol error rather than
// letting it fall through to the argument parser.
func msgArgBytes(ps *parseState, buf []byte, i int) ([]byte, error) {
	arg, err := controlLineBytes(ps, buf, i)
	if err != nil {
		return nil, err
	}
	if len(arg) == 0 {
		return nil, fmt.Errorf("%w: empty message argument line", ErrParserProtocol)
	}
	return arg, nil
}

// controlLineBytes returns the payload argument of a control line ending at i.
// It uses the accumulated argument buffer when the line was split across
// reads, and otherwise slices the input. A trailing CR (drop == 1) is always
// excluded.
func controlLineBytes(ps *parseState, buf []byte, i int) ([]byte, error) {
	if ps.argBuf != nil {
		return ps.argBuf, nil
	}
	end := i - ps.drop
	if end < ps.as {
		end = ps.as
	}
	if end-ps.as > maxControlLineArgSize {
		return nil, fmt.Errorf("%w: %d bytes in control line",
			ErrParserControlLineTooLong, end-ps.as)
	}
	ps.lineLen = end - ps.as
	return buf[ps.as:end], nil
}

// finishFrame consumes the frame terminator and returns to OP_START.
func (ps *parseState) finishFrame(i int) {
	ps.drop, ps.as, ps.state = 0, i+1, OP_START
	ps.lineLen, ps.payloadLen = 0, 0
}

// parserInputError classifies a read error against the frame boundary. A read
// error while the parser is between frames is passed through unchanged; one
// observed in the middle of a frame means the frame was truncated, so it enters
// the same sticky terminal state as a protocol error.
func (nc *Conn) parserInputError(readErr error) error {
	if nc.ps == nil {
		return readErr
	}
	if nc.ps.err != nil {
		return nc.ps.err
	}
	if nc.ps.state != OP_START {
		return nc.parserFail(fmt.Errorf("%w: %v", ErrParserTruncated, readErr))
	}
	return readErr
}

// resetParserLocked establishes a new parser generation, clearing every
// residual buffer byte and any sticky error. The connection lock must be held.
// It is the only way the parser is reset.
func (nc *Conn) resetParserLocked() {
	nc.parserGen++
	if nc.ps == nil {
		nc.ps = &parseState{}
	} else {
		nc.ps.reset()
	}
	nc.ps.gen = nc.parserGen
}

// cloneMsgArg is used when the split buffer scenario has the pubArg in the existing read buffer, but
// we need to hold onto it into the next read.
func (nc *Conn) cloneMsgArg() {
	nc.ps.argBuf = nc.ps.scratch[:0]
	nc.ps.argBuf = append(nc.ps.argBuf, nc.ps.ma.subject...)
	nc.ps.argBuf = append(nc.ps.argBuf, nc.ps.ma.reply...)
	nc.ps.ma.subject = nc.ps.argBuf[:len(nc.ps.ma.subject)]
	if nc.ps.ma.reply != nil {
		nc.ps.ma.reply = nc.ps.argBuf[len(nc.ps.ma.subject):]
	}
}

const argsLenMax = 4
const hdrArgsLenMax = 5

func (nc *Conn) processMsgArgs(arg []byte) error {
	// Use separate function for header based messages.
	if nc.ps.hdr >= 0 {
		return nc.processHeaderMsgArgs(arg)
	}

	// Unroll splitArgs to avoid runtime/heap issues
	a := [argsLenMax][]byte{}
	args := a[:0]
	start := -1
	for i, b := range arg {
		switch b {
		case ' ', '\t', '\r', '\n':
			if start >= 0 {
				args = append(args, arg[start:i])
				start = -1
			}
		default:
			if start < 0 {
				start = i
			}
		}
	}
	if start >= 0 {
		args = append(args, arg[start:])
	}

	switch len(args) {
	case 3:
		nc.ps.ma.subject = args[0]
		nc.ps.ma.sid = parseInt64(args[1])
		nc.ps.ma.reply = nil
		size := parseInt64(args[2])
		if err := validateMsgSize(size, arg); err != nil {
			return err
		}
		nc.ps.ma.size = int(size)
	case 4:
		nc.ps.ma.subject = args[0]
		nc.ps.ma.sid = parseInt64(args[1])
		nc.ps.ma.reply = args[2]
		size := parseInt64(args[3])
		if err := validateMsgSize(size, arg); err != nil {
			return err
		}
		nc.ps.ma.size = int(size)
	default:
		return fmt.Errorf("%w: processMsgArgs Parse Error: '%s'", ErrParserProtocol, snippet(arg))
	}
	if nc.ps.ma.sid < 0 {
		return fmt.Errorf("%w: processMsgArgs Bad or Missing Sid: '%s'", ErrParserProtocol, snippet(arg))
	}
	return nil
}

// validateMsgSize rejects missing, negative or implausibly large length fields
// before any payload memory is allocated. It takes the untruncated int64 value
// so an over-large field is caught even on 32-bit platforms, where converting
// it to int could otherwise wrap into a small or negative number.
func validateMsgSize(size int64, arg []byte) error {
	if size < 0 || size > maxMsgSize {
		return fmt.Errorf("%w: processMsgArgs Bad or Missing Size: '%s'", ErrParserBadLength, snippet(arg))
	}
	return nil
}

// processHeaderMsgArgs is for a header based message.
func (nc *Conn) processHeaderMsgArgs(arg []byte) error {
	// Unroll splitArgs to avoid runtime/heap issues
	a := [hdrArgsLenMax][]byte{}
	args := a[:0]
	start := -1
	for i, b := range arg {
		switch b {
		case ' ', '\t', '\r', '\n':
			if start >= 0 {
				args = append(args, arg[start:i])
				start = -1
			}
		default:
			if start < 0 {
				start = i
			}
		}
	}
	if start >= 0 {
		args = append(args, arg[start:])
	}

	switch len(args) {
	case 4:
		nc.ps.ma.subject = args[0]
		nc.ps.ma.sid = parseInt64(args[1])
		nc.ps.ma.reply = nil
		hdr := parseInt64(args[2])
		size := parseInt64(args[3])
		if err := validateMsgSize(size, arg); err != nil {
			return err
		}
		if hdr < 0 || hdr > size {
			return fmt.Errorf("%w: processHeaderMsgArgs Bad or Missing Header Size: '%s'",
				ErrParserBadLength, snippet(arg))
		}
		nc.ps.ma.hdr = int(hdr)
		nc.ps.ma.size = int(size)
	case 5:
		nc.ps.ma.subject = args[0]
		nc.ps.ma.sid = parseInt64(args[1])
		nc.ps.ma.reply = args[2]
		hdr := parseInt64(args[3])
		size := parseInt64(args[4])
		if err := validateMsgSize(size, arg); err != nil {
			return err
		}
		if hdr < 0 || hdr > size {
			return fmt.Errorf("%w: processHeaderMsgArgs Bad or Missing Header Size: '%s'",
				ErrParserBadLength, snippet(arg))
		}
		nc.ps.ma.hdr = int(hdr)
		nc.ps.ma.size = int(size)
	default:
		return fmt.Errorf("%w: processHeaderMsgArgs Parse Error: '%s'", ErrParserProtocol, snippet(arg))
	}
	if nc.ps.ma.sid < 0 {
		return fmt.Errorf("%w: processHeaderMsgArgs Bad or Missing Sid: '%s'", ErrParserProtocol, snippet(arg))
	}
	return nil
}

// snippet bounds an argument echoed in an error message.
func snippet(b []byte) string {
	if len(b) > maxParserErrorSnippet {
		b = b[:maxParserErrorSnippet]
	}
	return string(b)
}

// ASCII numbers 0-9
const (
	ascii_0 = 48
	ascii_9 = 57
)

// parseInt64 expects decimal positive numbers. We return -1 to signal error,
// including empty input, non-digit characters and int64 overflow. Overflow
// must be detected here rather than left to wrap into an accepted value.
func parseInt64(d []byte) (n int64) {
	if len(d) == 0 {
		return -1
	}
	for _, dec := range d {
		if dec < ascii_0 || dec > ascii_9 {
			return -1
		}
		digit := int64(dec - ascii_0)
		if n > (math.MaxInt64-digit)/10 {
			return -1
		}
		n = n*10 + digit
	}
	return n
}
