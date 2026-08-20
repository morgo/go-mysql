package server

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
)

// readRawPacket reads one MySQL protocol packet (4-byte header + payload) off
// the wire. These tests speak the protocol directly rather than through a
// client, because the point is to send something no client would send.
func readRawPacket(conn net.Conn) (seq byte, payload []byte, err error) {
	var hdr [4]byte
	if _, err = io.ReadFull(conn, hdr[:]); err != nil {
		return 0, nil, err
	}
	length := int(uint32(hdr[0]) | uint32(hdr[1])<<8 | uint32(hdr[2])<<16)
	payload = make([]byte, length)
	if _, err = io.ReadFull(conn, payload); err != nil {
		return 0, nil, err
	}
	return hdr[3], payload, nil
}

// errPacketCode returns the error code carried by an ERR packet.
func errPacketCode(t *testing.T, payload []byte) uint16 {
	t.Helper()
	if len(payload) < 3 {
		t.Fatalf("packet of %d bytes is too short to be an ERR packet", len(payload))
	}
	if payload[0] != mysql.ERR_HEADER {
		t.Fatalf("packet header = %#x, want ERR (%#x)", payload[0], mysql.ERR_HEADER)
	}
	return binary.LittleEndian.Uint16(payload[1:3])
}

// serveOnce accepts one connection and runs the server side of it. The channel
// carries the handshake error, or — when the handshake succeeds — the result of
// serving one command.
func serveOnce(t *testing.T, srv *Server, auth AuthenticationHandler) (addr string, served <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	result := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			result <- err
			return
		}
		c, err := srv.NewCustomizedConn(conn, auth, EmptyHandler{})
		if err != nil {
			result <- err
			return
		}
		// Handshake succeeded, so the test is exercising the command phase.
		result <- c.HandleCommand()
	}()
	return ln.Addr().String(), result
}

// TestHandshakeResponseOverMaxAllowedPacket covers the pre-auth exposure. The
// handshake response is a client-supplied packet read before any credential is
// checked, so an unlimited server lets an unauthenticated peer decide how much
// memory it buffers. Only the 4-byte header is sent here: the server must refuse
// on the declared length alone, without waiting for — or reserving room for —
// the payload it was promised.
func TestHandshakeResponseOverMaxAllowedPacket(t *testing.T) {
	srv := NewDefaultServer()
	if err := srv.SetMaxAllowedPacket(1024); err != nil {
		t.Fatalf("SetMaxAllowedPacket: %v", err)
	}
	addr, handshake := serveOnce(t, srv, NewInMemoryAuthenticationHandler())

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}

	if _, _, err := readRawPacket(conn); err != nil {
		t.Fatalf("reading initial handshake: %v", err)
	}

	// Handshake response declaring an 8KiB payload, sequence 1. No payload follows.
	if _, err := conn.Write([]byte{0x00, 0x20, 0x00, 0x01}); err != nil {
		t.Fatalf("writing oversized handshake response header: %v", err)
	}

	_, payload, err := readRawPacket(conn)
	if err != nil {
		t.Fatalf("reading error response: %v", err)
	}
	if code := errPacketCode(t, payload); code != mysql.ER_NET_PACKET_TOO_LARGE {
		t.Errorf("error code = %d, want ER_NET_PACKET_TOO_LARGE (%d)", code, mysql.ER_NET_PACKET_TOO_LARGE)
	}
	if err := <-handshake; err == nil {
		t.Error("NewCustomizedConn succeeded on an oversized handshake response, want a failure")
	}
}

// TestServerDefaultsToMySQLMaxAllowedPacket pins the default a server starts
// with: unlimited reads are the vulnerable configuration, so the constructors
// must not leave the limit at zero.
func TestServerDefaultsToMySQLMaxAllowedPacket(t *testing.T) {
	if got := NewDefaultServer().MaxAllowedPacket(); got != packet.DefaultMaxAllowedPacket {
		t.Errorf("NewDefaultServer max allowed packet = %d, want %d", got, packet.DefaultMaxAllowedPacket)
	}
	srv := NewServer("8.0.11", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil)
	if got := srv.MaxAllowedPacket(); got != packet.DefaultMaxAllowedPacket {
		t.Errorf("NewServer max allowed packet = %d, want %d", got, packet.DefaultMaxAllowedPacket)
	}
}

// TestSetMaxAllowedPacketRejectsNegative keeps a caller from accidentally
// configuring a limit that would reject every packet.
func TestSetMaxAllowedPacketRejectsNegative(t *testing.T) {
	srv := NewDefaultServer()
	if err := srv.SetMaxAllowedPacket(-1); err == nil {
		t.Error("SetMaxAllowedPacket(-1) = nil, want an error")
	}
	if got := srv.MaxAllowedPacket(); got != packet.DefaultMaxAllowedPacket {
		t.Errorf("max allowed packet = %d after a rejected set, want it unchanged (%d)", got, packet.DefaultMaxAllowedPacket)
	}
}

// TestCommandOverMaxAllowedPacket covers the command phase, which is where a
// proxy spends its life: an authenticated client's oversized command must be
// answered with ER_NET_PACKET_TOO_LARGE, not merely dropped. Only the header is
// sent, so the server also has to refuse without waiting for the payload.
func TestCommandOverMaxAllowedPacket(t *testing.T) {
	srv := NewDefaultServer()
	if err := srv.SetMaxAllowedPacket(1024); err != nil {
		t.Fatalf("SetMaxAllowedPacket: %v", err)
	}
	auth := NewInMemoryAuthenticationHandler()
	if err := auth.AddUser("packetuser", "packetpass"); err != nil {
		t.Fatalf("AddUser: %v", err)
	}
	addr, served := serveOnce(t, srv, auth)

	c, err := client.Connect(addr, "packetuser", "packetpass", "")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close()

	// A command declaring a 4MiB payload, sequence 0. Written under the client's
	// packet layer so no payload follows; the client's sequence is advanced by
	// hand to match what the server will reply with.
	if _, err := c.Conn.Conn.Write([]byte{0x00, 0x00, 0x40, 0x00}); err != nil {
		t.Fatalf("writing oversized command header: %v", err)
	}
	c.Sequence = 1

	payload, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("reading error response: %v", err)
	}
	if code := errPacketCode(t, payload); code != mysql.ER_NET_PACKET_TOO_LARGE {
		t.Errorf("error code = %d, want ER_NET_PACKET_TOO_LARGE (%d)", code, mysql.ER_NET_PACKET_TOO_LARGE)
	}
	if err := <-served; err == nil {
		t.Error("HandleCommand succeeded on an oversized command, want a failure")
	}
}
