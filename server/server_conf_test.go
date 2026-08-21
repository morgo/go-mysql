package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
	"github.com/stretchr/testify/require"
)

var multiResultServerCapabilities = []uint32{
	mysql.CLIENT_MULTI_RESULTS,
	mysql.CLIENT_PS_MULTI_RESULTS,
}

func TestDefaultServerCapabilities(t *testing.T) {
	s := NewDefaultServer()
	assertServerAdvertisesCapabilities(t, s)
}

func TestNewServerCapabilities(t *testing.T) {
	s := NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil)
	assertServerAdvertisesCapabilities(t, s)
}

func assertServerAdvertisesCapabilities(t *testing.T, s *Server) {
	t.Helper()
	for _, cap := range multiResultServerCapabilities {
		require.True(t, s.Capability()&cap != 0,
			"expected server to advertise %s", mysql.CapNames[cap])
	}
}

// procHandler simulates a proxy handler that forwards CALL statements to an upstream MySQL.
type procHandler struct{ EmptyHandler }

func (h *procHandler) HandleQuery(query string) (*mysql.Result, error) {
	if query != "CALL get_report()" {
		return nil, nil
	}

	// A stored procedure that returns result sets requires CLIENT_MULTI_RESULTS to be
	// negotiated end-to-end. Without it, a real MySQL upstream returns Error 1312:
	// "PROCEDURE get_report can't return a result set in the given context".
	rs, err := mysql.BuildSimpleResultset(
		[]string{"id", "name"},
		[][]any{{1, "alice"}, {2, "bob"}},
		false,
	)
	if err != nil {
		return nil, err
	}
	return mysql.NewResult(rs), nil
}

// TestNegotiatedMultiResultsCapability reproduces the stored-procedure CALL issue:
// JDBC and other clients request CLIENT_MULTI_RESULTS during handshake, but when the
// go-mysql server does not advertise it, the negotiated capability is cleared and
// CALL statements that return result sets fail on a real MySQL upstream (Error 1312).
func TestNegotiatedMultiResultsCapability(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	svr := NewDefaultServer()
	authHandler := NewInMemoryAuthenticationHandler()
	require.NoError(t, authHandler.AddUser("root", ""))

	go func() {
		conn, acceptErr := l.Accept()
		if acceptErr != nil {
			return
		}
		sConn, connErr := svr.NewCustomizedConn(conn, authHandler, &procHandler{})
		if connErr != nil {
			return
		}
		for {
			if handleErr := sConn.HandleCommand(); handleErr != nil {
				return
			}
		}
	}()

	// Simulate JDBC Connector/J: request multi-result support during handshake.
	c, err := client.Connect(l.Addr().String(), "root", "", "",
		func(conn *client.Conn) error {
			if err := conn.SetCapability(mysql.CLIENT_MULTI_RESULTS); err != nil {
				return err
			}
			return conn.SetCapability(mysql.CLIENT_PS_MULTI_RESULTS)
		},
	)
	require.NoError(t, err)
	defer c.Close()

	negotiated := c.CapabilityString()
	require.Contains(t, negotiated, "CLIENT_MULTI_RESULTS",
		"CLIENT_MULTI_RESULTS must be negotiated for CALL stored procedures, got: %s", negotiated)
	require.Contains(t, negotiated, "CLIENT_PS_MULTI_RESULTS",
		"CLIENT_PS_MULTI_RESULTS must be negotiated for prepared CALL with OUT params, got: %s", negotiated)

	result, err := c.Execute("CALL get_report()")
	require.NoError(t, err)
	defer result.Close()
	require.True(t, result.HasResultset())
	require.Equal(t, 2, len(result.Values))
}

// TestLocalFilesCapabilityNegotiated verifies that CLIENT_LOCAL_FILES is advertised
// by the server and survives handshake negotiation when requested by the client.
func TestLocalFilesCapabilityNegotiated(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	svr := NewDefaultServer()
	require.NoError(t, svr.SetCapability(mysql.CLIENT_LOCAL_FILES))
	authHandler := NewInMemoryAuthenticationHandler()
	require.NoError(t, authHandler.AddUser("root", ""))

	go func() {
		conn, acceptErr := l.Accept()
		if acceptErr != nil {
			return
		}
		sConn, connErr := svr.NewCustomizedConn(conn, authHandler, EmptyHandler{})
		if connErr != nil {
			return
		}
		for {
			if handleErr := sConn.HandleCommand(); handleErr != nil {
				return
			}
		}
	}()

	c, err := client.Connect(l.Addr().String(), "root", "", "",
		func(conn *client.Conn) error {
			return conn.SetCapability(mysql.CLIENT_LOCAL_FILES)
		},
	)
	require.NoError(t, err)
	defer c.Close()

	negotiated := c.CapabilityString()
	require.Contains(t, negotiated, "CLIENT_LOCAL_FILES",
		"CLIENT_LOCAL_FILES must be negotiated for LOAD DATA LOCAL INFILE relay, got: %s", negotiated)
}

// TestLocalFilesCapabilityCanBeDisabled verifies that CLIENT_LOCAL_FILES is not advertised
// by default and is not negotiated unless explicitly enabled via SetCapability.
func TestLocalFilesCapabilityCanBeDisabled(t *testing.T) {
	svr := NewDefaultServer()
	require.False(t, svr.Capability()&mysql.CLIENT_LOCAL_FILES != 0)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	authHandler := NewInMemoryAuthenticationHandler()
	require.NoError(t, authHandler.AddUser("root", ""))

	go func() {
		conn, acceptErr := l.Accept()
		if acceptErr != nil {
			return
		}
		sConn, connErr := svr.NewCustomizedConn(conn, authHandler, EmptyHandler{})
		if connErr != nil {
			return
		}
		for {
			if handleErr := sConn.HandleCommand(); handleErr != nil {
				return
			}
		}
	}()

	c, err := client.Connect(l.Addr().String(), "root", "", "",
		func(conn *client.Conn) error {
			return conn.SetCapability(mysql.CLIENT_LOCAL_FILES)
		},
	)
	require.NoError(t, err)
	defer c.Close()

	negotiated := c.CapabilityString()
	require.NotContains(t, negotiated, "CLIENT_LOCAL_FILES",
		"CLIENT_LOCAL_FILES must not be negotiated when the server does not advertise it, got: %s", negotiated)
}

func TestSetCapabilityRejectsUnsafeFlags(t *testing.T) {
	svr := NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil)

	err := svr.SetCapability(mysql.CLIENT_SSL)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-user-configurable flags")
	require.Contains(t, err.Error(), fmt.Sprintf("%#x", mysql.CLIENT_SSL))
	require.False(t, svr.Capability()&mysql.CLIENT_SSL != 0)

	err = svr.SetCapability(mysql.CLIENT_LOCAL_FILES | mysql.CLIENT_SSL)
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf("%#x", mysql.CLIENT_SSL))
	require.False(t, svr.Capability()&mysql.CLIENT_LOCAL_FILES != 0)

	err = svr.UnsetCapability(mysql.CLIENT_PROTOCOL_41)
	require.Error(t, err)
	require.True(t, svr.Capability()&mysql.CLIENT_PROTOCOL_41 != 0)

	require.NoError(t, svr.SetCapability(mysql.CLIENT_LOCAL_FILES))
	require.True(t, svr.Capability()&mysql.CLIENT_LOCAL_FILES != 0)
	require.NoError(t, svr.UnsetCapability(mysql.CLIENT_LOCAL_FILES))
	require.False(t, svr.Capability()&mysql.CLIENT_LOCAL_FILES != 0)
}

// readRawPacket reads one packet (4-byte header + payload) off the wire. These
// tests speak the protocol directly, to send what no client would send.
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
	require.GreaterOrEqual(t, len(payload), 3, "too short to be an ERR packet")
	require.Equal(t, byte(mysql.ERR_HEADER), payload[0], "packet header should be ERR")
	return binary.LittleEndian.Uint16(payload[1:3])
}

// serveOnce accepts one connection and runs the server side of it. The channel
// carries the handshake error, or the result of serving one command.
func serveOnce(t *testing.T, srv *Server, auth AuthenticationHandler) (addr string, served <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
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

// TestHandshakeResponseOverMaxAllowedPacket covers the pre-auth exposure: the
// handshake response is read before any credential is checked. Only the header
// is sent, so the server must refuse on the declared length alone.
func TestHandshakeResponseOverMaxAllowedPacket(t *testing.T) {
	srv := NewDefaultServer()
	srv.MaxAllowedPacket = 1024
	addr, handshake := serveOnce(t, srv, NewInMemoryAuthenticationHandler())

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))

	_, _, err = readRawPacket(conn)
	require.NoError(t, err, "reading initial handshake")

	// Handshake response declaring an 8KiB payload, sequence 1. No payload follows.
	_, err = conn.Write([]byte{0x00, 0x20, 0x00, 0x01})
	require.NoError(t, err, "writing oversized handshake response header")

	_, payload, err := readRawPacket(conn)
	require.NoError(t, err, "reading error response")
	require.Equal(t, uint16(mysql.ER_NET_PACKET_TOO_LARGE), errPacketCode(t, payload))
	require.Error(t, <-handshake, "NewCustomizedConn should reject an oversized handshake response")
}

// TestServerDefaultsToMySQLMaxAllowedPacket pins the default: unlimited is the
// vulnerable configuration, so no constructor may leave the limit at zero.
func TestServerDefaultsToMySQLMaxAllowedPacket(t *testing.T) {
	require.Equal(t, packet.DefaultMaxAllowedPacket, NewDefaultServer().MaxAllowedPacket)
	srv := NewServer("8.0.11", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil)
	require.Equal(t, packet.DefaultMaxAllowedPacket, srv.MaxAllowedPacket)
}

// TestCommandOverMaxAllowedPacket covers the command phase, where a proxy spends
// its life: an oversized command must be answered with ER_NET_PACKET_TOO_LARGE,
// not merely dropped.
func TestCommandOverMaxAllowedPacket(t *testing.T) {
	srv := NewDefaultServer()
	srv.MaxAllowedPacket = 1024
	auth := NewInMemoryAuthenticationHandler()
	require.NoError(t, auth.AddUser("packetuser", "packetpass"))
	addr, served := serveOnce(t, srv, auth)

	c, err := client.Connect(addr, "packetuser", "packetpass", "")
	require.NoError(t, err)
	defer c.Close()
	// A server that failed to refuse would block on the promised payload; bound
	// the read so that fails the test rather than hanging it.
	require.NoError(t, c.Conn.Conn.SetDeadline(time.Now().Add(10*time.Second)))

	// A command declaring a 4MiB payload, sequence 0, written under the client's
	// packet layer so none follows. Its sequence is advanced by hand to match.
	_, err = c.Conn.Conn.Write([]byte{0x00, 0x00, 0x40, 0x00})
	require.NoError(t, err, "writing oversized command header")
	c.Sequence = 1

	payload, err := c.ReadPacket()
	require.NoError(t, err, "reading error response")
	require.Equal(t, uint16(mysql.ER_NET_PACKET_TOO_LARGE), errPacketCode(t, payload))
	require.Error(t, <-served, "HandleCommand should reject an oversized command")
}
