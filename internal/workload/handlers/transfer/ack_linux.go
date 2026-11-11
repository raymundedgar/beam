package transfer

import (
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

func bytesAcked(conn net.Conn) (int64, bool) {
	snap := readTCP(conn)
	if !snap.ok {
		return 0, false
	}
	return snap.acked, true
}

func readTCP(conn net.Conn) tcpSnap {
	conn = netConn(conn)
	if conn == nil {
		return tcpSnap{}
	}
	raw, ok := conn.(syscall.Conn)
	if !ok {
		return tcpSnap{}
	}
	sys, err := raw.SyscallConn()
	if err != nil {
		return tcpSnap{}
	}
	var snap tcpSnap
	err = sys.Control(func(fd uintptr) {
		info, err := unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
		if err != nil || info == nil {
			return
		}
		buf, _ := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF)
		snap = tcpSnap{
			acked: int64(info.Bytes_acked), rttUs: info.Rtt, cwnd: info.Snd_cwnd,
			retrans: info.Total_retrans, deliveryRate: info.Delivery_rate,
			rwndLimited: info.Rwnd_limited, sndbufLimited: info.Sndbuf_limited,
			sndbuf: buf, ok: true,
		}
	})
	if err != nil {
		return tcpSnap{}
	}
	return snap
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
