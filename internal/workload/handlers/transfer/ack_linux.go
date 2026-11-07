package transfer

import (
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

func bytesAcked(conn net.Conn) (int64, bool) {
	conn = netConn(conn)
	if conn == nil {
		return 0, false
	}
	raw, ok := conn.(syscall.Conn)
	if !ok {
		return 0, false
	}
	sys, err := raw.SyscallConn()
	if err != nil {
		return 0, false
	}
	var acked int64
	var got bool
	err = sys.Control(func(fd uintptr) {
		info, err := unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
		if err != nil || info == nil {
			return
		}
		acked = int64(info.Bytes_acked)
		got = true
	})
	if err != nil || !got {
		return 0, false
	}
	return acked, true
}

func netConn(conn net.Conn) net.Conn {
	for conn != nil {
		unwrap, ok := conn.(interface{ NetConn() net.Conn })
		if !ok {
			return conn
		}
		next := unwrap.NetConn()
		if next == nil || next == conn {
			return conn
		}
		conn = next
	}
	return nil
}
