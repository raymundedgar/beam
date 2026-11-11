//go:build !linux

package transfer

import "net"

func bytesAcked(net.Conn) (int64, bool) { return 0, false }

func readTCP(net.Conn) tcpSnap { return tcpSnap{} }
