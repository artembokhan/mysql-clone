package clone

import (
	"errors"
	"io"
	"net"
	"strings"
	"syscall"
)

func isBenignDisconnect(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}

	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "connection was bad") {
		return true
	}
	if strings.Contains(msg, "use of closed network connection") {
		return true
	}
	if strings.Contains(msg, "broken pipe") || strings.Contains(msg, "reset by peer") {
		return true
	}

	return false
}
