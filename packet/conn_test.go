package packet

import (
	"bufio"
	"bytes"
	goErrors "errors"
	"io"
	"net"
	"testing"

	"github.com/go-mysql-org/go-mysql/compress"
	"github.com/go-mysql-org/go-mysql/mysql"
)

// newReadTestConn builds a Conn whose read side is fed from the given raw bytes,
// simulating frames as they would arrive from a MySQL server. No net.Conn is
// needed because readTimeout is 0, so SetReadDeadline is never called.
func newReadTestConn(stream []byte, compression uint8) *Conn {
	c, _ := newReadTestConnReader(stream, compression)
	return c
}

// newReadTestConnReader is newReadTestConn with the underlying reader exposed,
// so a test can assert how much of the stream was actually consumed.
func newReadTestConnReader(stream []byte, compression uint8) (*Conn, *bytes.Reader) {
	r := bytes.NewReader(stream)
	c := new(Conn)
	c.reader = r
	c.copyNBuf = make([]byte, DefaultBufferSize)
	c.Compression = compression
	return c, r
}

// mysqlPacket builds a single MySQL protocol packet (4-byte header + payload).
func mysqlPacket(seq byte, payload []byte) []byte {
	n := len(payload)
	b := make([]byte, 4+n)
	b[0] = byte(n)
	b[1] = byte(n >> 8)
	b[2] = byte(n >> 16)
	b[3] = seq
	copy(b[4:], payload)
	return b
}

// uncompressedFrame builds a compressed-protocol frame whose payload is stored
// verbatim (length-before-compression == 0), as a server does for small or
// incompressible chunks.
func uncompressedFrame(seq byte, body []byte) []byte {
	cl := len(body)
	f := make([]byte, 7+cl)
	f[0] = byte(cl)
	f[1] = byte(cl >> 8)
	f[2] = byte(cl >> 16)
	f[3] = seq
	// bytes 4..6 (uncompressed length) stay 0
	copy(f[7:], body)
	return f
}

// zlibFrame builds a compressed-protocol frame whose payload is zlib-compressed.
func zlibFrame(t *testing.T, seq byte, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := compress.GetPooledZlibWriter(&buf)
	if err != nil {
		t.Fatalf("GetPooledZlibWriter: %v", err)
	}
	if _, err := w.Write(payload); err != nil {
		t.Fatalf("zlib write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("zlib close: %v", err)
	}
	body := buf.Bytes()
	cl := len(body)
	ul := len(payload)
	f := make([]byte, 7+cl)
	f[0] = byte(cl)
	f[1] = byte(cl >> 8)
	f[2] = byte(cl >> 16)
	f[3] = seq
	f[4] = byte(ul)
	f[5] = byte(ul >> 8)
	f[6] = byte(ul >> 16)
	copy(f[7:], body)
	return f
}

// TestReadPacketPacketSpanningUncompressedFrames reproduces the desync where a
// single MySQL packet spans out of an uncompressed (length-before-compression 0)
// frame into the following frame. Before the fix the uncompressed frame was read
// from the raw, unbounded connection, so copyN read straight through the next
// frame's 7-byte header and corrupted the payload.
func TestReadPacketPacketSpanningUncompressedFrames(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789abcdef"), 16) // 256 bytes
	pkt := mysqlPacket(0, payload)

	// Split the packet's bytes mid-payload across two uncompressed frames.
	split := 100
	stream := append(uncompressedFrame(0, pkt[:split]), uncompressedFrame(1, pkt[split:])...)

	c := newReadTestConn(stream, mysql.MYSQL_COMPRESS_ZLIB)
	got, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch:\n got  %q\n want %q", got, payload)
	}
}

// TestReadPacketPacketSpanningZlibFrames guards the already-working compressed
// path: a packet split across two zlib frames must still reassemble correctly.
func TestReadPacketPacketSpanningZlibFrames(t *testing.T) {
	payload := bytes.Repeat([]byte("the quick brown fox "), 32) // 640 bytes
	pkt := mysqlPacket(0, payload)

	split := 200
	stream := append(zlibFrame(t, 0, pkt[:split]), zlibFrame(t, 1, pkt[split:])...)

	c := newReadTestConn(stream, mysql.MYSQL_COMPRESS_ZLIB)
	got, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch:\n got  %q\n want %q", got, payload)
	}
}

// TestReadPacketMultiplePacketsInOneZlibFrame guards reader reuse across reads: a
// server may pack several MySQL packets into a single compressed frame, and each is
// retrieved by a separate ReadPacket call. The first read consumes the frame header
// and decompresses; subsequent reads must continue from the same compressedReader
// rather than consuming another frame header. Recreating the reader on every read
// (e.g. dropping the nil guard) reads garbage as the next frame's header and desyncs.
func TestReadPacketMultiplePacketsInOneZlibFrame(t *testing.T) {
	payloadA := bytes.Repeat([]byte("alpha "), 8)
	payloadB := bytes.Repeat([]byte("bravo "), 8)

	// Two complete MySQL packets (sequence 0 and 1) inside one zlib frame.
	combined := append(mysqlPacket(0, payloadA), mysqlPacket(1, payloadB)...)
	stream := zlibFrame(t, 0, combined)

	c := newReadTestConn(stream, mysql.MYSQL_COMPRESS_ZLIB)

	got, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket #1: %v", err)
	}
	if !bytes.Equal(got, payloadA) {
		t.Fatalf("packet #1 mismatch:\n got  %q\n want %q", got, payloadA)
	}

	got, err = c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket #2: %v", err)
	}
	if !bytes.Equal(got, payloadB) {
		t.Fatalf("packet #2 mismatch:\n got  %q\n want %q", got, payloadB)
	}
}

// TestReadPacketSingleUncompressedFrame guards the common small-response case:
// a whole packet inside one uncompressed frame.
func TestReadPacketSingleUncompressedFrame(t *testing.T) {
	payload := []byte("small uncompressed response")
	stream := uncompressedFrame(0, mysqlPacket(0, payload))

	c := newReadTestConn(stream, mysql.MYSQL_COMPRESS_ZLIB)
	got, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch:\n got  %q\n want %q", got, payload)
	}
}

// benchmarkReadPacket measures the read path over loopback TCP: unbuffered is
// what a conn from NewTLSConn pays without EnableReadBuffering (two read(2)
// calls per packet), buffered is the same conn after EnableReadBuffering.
func benchmarkReadPacket(b *testing.B, buffered bool) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer ln.Close()

	const payloadSize = 64
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		w := bufio.NewWriterSize(conn, DefaultBufferSize)
		frame := make([]byte, 4+payloadSize)
		frame[0] = payloadSize
		for seq := byte(0); ; seq++ {
			frame[3] = seq
			if _, err := w.Write(frame); err != nil {
				return
			}
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	c := NewTLSConn(conn)
	if buffered {
		c.EnableReadBuffering(DefaultReadBufferSize)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.ReadPacket(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadPacketUnbuffered(b *testing.B) { benchmarkReadPacket(b, false) }

func BenchmarkReadPacketBuffered(b *testing.B) { benchmarkReadPacket(b, true) }

// benchmarkWriteResultset measures writing a point-SELECT-shaped response (22
// packets: column count, 17 column definitions, row, EOF/OK framing) followed
// by one Flush. Unbuffered issues one write(2) per packet, buffered one per
// response.
func benchmarkWriteResultset(b *testing.B, buffered bool) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(io.Discard, conn)
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	c := NewTLSConn(conn)
	if buffered {
		c.EnableWriteBuffering(DefaultBufferSize)
	}

	const packetsPerResponse = 22
	frame := make([]byte, 4+64)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for range packetsPerResponse {
			if err := c.WritePacket(frame); err != nil {
				b.Fatal(err)
			}
		}
		if err := c.Flush(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteResultsetUnbuffered(b *testing.B) { benchmarkWriteResultset(b, false) }

func BenchmarkWriteResultsetBuffered(b *testing.B) { benchmarkWriteResultset(b, true) }

// TestReadPacketSequenceMismatchIsBadConn covers a desynced stream: once a packet
// arrives with an unexpected sequence, nothing after it can be interpreted, so the
// connection must be reported as bad rather than merely as a failed read. A server
// closing an idle connection queues its ERR packet outside any command cycle, so it
// carries sequence 0 and the next command reads it while expecting 1.
func TestReadPacketSequenceMismatchIsBadConn(t *testing.T) {
	c := newReadTestConn(mysqlPacket(0, []byte("out of band")), mysql.MYSQL_COMPRESS_NONE)
	c.Sequence = 1

	_, err := c.ReadPacket()
	if err == nil {
		t.Fatal("ReadPacket succeeded on a mismatched sequence, want an error")
	}
	if !mysql.ErrorEqual(err, mysql.ErrBadConn) {
		t.Errorf("err = %v, want an error equal to mysql.ErrBadConn", err)
	}
}

// fullLengthPacketPlus builds the wire bytes for one logical packet whose
// payload is MaxPayloadLen bytes followed by tail: a full-length packet plus the
// shorter continuation that terminates it. This is the only shape that exercises
// reassembly, since only a packet of exactly MaxPayloadLen is continued.
func fullLengthPacketPlus(tail []byte) []byte {
	stream := make([]byte, 0, 4+mysql.MaxPayloadLen+4+len(tail))
	stream = append(stream, 0xff, 0xff, 0xff, 0) // MaxPayloadLen, sequence 0
	stream = append(stream, bytes.Repeat([]byte("a"), mysql.MaxPayloadLen)...)
	stream = append(stream, mysqlPacket(1, tail)...)
	return stream
}

// TestReadPacketRefusesOversizedPacketWithoutReadingIt pins the ordering that
// makes the limit worth having: the declared length is checked before any of the
// payload is read and before the destination buffer is grown to hold it, so
// refusing a packet costs its 4-byte header and nothing else. A limit enforced
// after reassembly would still let a peer decide how much we allocate.
func TestReadPacketRefusesOversizedPacketWithoutReadingIt(t *testing.T) {
	const payloadLen = 4096
	c, r := newReadTestConnReader(mysqlPacket(0, bytes.Repeat([]byte("x"), payloadLen)), mysql.MYSQL_COMPRESS_NONE)
	c.SetMaxAllowedPacket(64)

	_, err := c.ReadPacket()
	var tooLarge *PacketTooLargeError
	if !goErrors.As(err, &tooLarge) {
		t.Fatalf("err = %v, want a *PacketTooLargeError", err)
	}
	if tooLarge.Limit != 64 {
		t.Errorf("Limit = %d, want 64", tooLarge.Limit)
	}
	if tooLarge.Size != payloadLen {
		t.Errorf("Size = %d, want %d", tooLarge.Size, payloadLen)
	}
	if r.Len() != payloadLen {
		t.Errorf("%d payload bytes consumed; want the payload left unread", payloadLen-r.Len())
	}
}

// TestReadPacketRefusesOversizedContinuation covers the case the per-packet
// header cannot catch on its own: every continuation is individually legal (the
// 3-byte length field cannot express more than MaxPayloadLen), so only their sum
// bounds the payload. Streaming continuations is exactly how a peer makes a
// server buffer without bound.
func TestReadPacketRefusesOversizedContinuation(t *testing.T) {
	const tailLen = 512
	stream := fullLengthPacketPlus(bytes.Repeat([]byte("b"), tailLen))

	c, r := newReadTestConnReader(stream, mysql.MYSQL_COMPRESS_NONE)
	// The first packet fits on its own; the pair does not.
	c.SetMaxAllowedPacket(mysql.MaxPayloadLen + 128)

	_, err := c.ReadPacket()
	var tooLarge *PacketTooLargeError
	if !goErrors.As(err, &tooLarge) {
		t.Fatalf("err = %v, want a *PacketTooLargeError", err)
	}
	if want := int64(mysql.MaxPayloadLen + tailLen); tooLarge.Size != want {
		t.Errorf("Size = %d, want %d", tooLarge.Size, want)
	}
	if r.Len() != tailLen {
		t.Errorf("%d continuation payload bytes consumed; want them left unread", tailLen-r.Len())
	}
}

// TestReadPacketMultiPacketPayloadWithinLimit guards the loop rewrite: a payload
// spanning a continuation still reassembles, and a payload of exactly
// MaxAllowedPacket is accepted (the limit is a maximum, not a strict bound).
func TestReadPacketMultiPacketPayloadWithinLimit(t *testing.T) {
	tail := []byte("tail")
	c := newReadTestConn(fullLengthPacketPlus(tail), mysql.MYSQL_COMPRESS_NONE)
	c.SetMaxAllowedPacket(mysql.MaxPayloadLen + len(tail))

	got, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	if len(got) != mysql.MaxPayloadLen+len(tail) {
		t.Fatalf("payload length = %d, want %d", len(got), mysql.MaxPayloadLen+len(tail))
	}
	if !bytes.Equal(got[mysql.MaxPayloadLen:], tail) {
		t.Errorf("continuation payload = %q, want %q", got[mysql.MaxPayloadLen:], tail)
	}
	if c.Sequence != 2 {
		t.Errorf("Sequence = %d, want 2 (both packets accounted for)", c.Sequence)
	}
}

// TestReadPacketUnlimitedByDefault keeps the limit opt-in at the packet layer, so
// introducing it does not change what an existing Conn accepts.
func TestReadPacketUnlimitedByDefault(t *testing.T) {
	payload := bytes.Repeat([]byte("z"), 4096)
	c := newReadTestConn(mysqlPacket(0, payload), mysql.MYSQL_COMPRESS_NONE)
	if c.MaxAllowedPacket() != 0 {
		t.Fatalf("MaxAllowedPacket = %d, want 0 (unlimited)", c.MaxAllowedPacket())
	}

	got, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

// TestPacketTooLargeErrorIsBadConn covers the connection-reuse contract. The
// oversized payload is deliberately left in the stream, so the connection is
// desynced and must be discarded; callers decide that on ErrBadConn. Both
// spellings are checked because mysql.ErrorEqual walks pingcap/errors' Cause
// chain while errors.Is walks Unwrap, and the wrapping in ReadPacketReuseMem
// sits between the caller and this error.
func TestPacketTooLargeErrorIsBadConn(t *testing.T) {
	c := newReadTestConn(mysqlPacket(0, bytes.Repeat([]byte("x"), 512)), mysql.MYSQL_COMPRESS_NONE)
	c.SetMaxAllowedPacket(16)

	_, err := c.ReadPacket()
	if err == nil {
		t.Fatal("ReadPacket accepted a packet over the limit, want an error")
	}
	if !mysql.ErrorEqual(err, mysql.ErrBadConn) {
		t.Errorf("mysql.ErrorEqual(err, ErrBadConn) = false for %v, want true", err)
	}
	if !goErrors.Is(err, mysql.ErrBadConn) {
		t.Errorf("errors.Is(err, ErrBadConn) = false for %v, want true", err)
	}
}

// TestSetMaxAllowedPacketNegativeMeansUnlimited documents the clamp, so a
// caller computing a limit that goes negative gets the documented behaviour
// rather than a limit that rejects every packet.
func TestSetMaxAllowedPacketNegativeMeansUnlimited(t *testing.T) {
	c := newReadTestConn(nil, mysql.MYSQL_COMPRESS_NONE)
	c.SetMaxAllowedPacket(-1)
	if c.MaxAllowedPacket() != 0 {
		t.Errorf("MaxAllowedPacket = %d, want 0", c.MaxAllowedPacket())
	}
}
