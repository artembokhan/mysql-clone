package clone

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
)

const defaultMySQLPort = "3306"

func (d *Dumper) connect(ctx context.Context) (*client.Conn, error) {
	network, address, err := resolveMySQLAddress(d.cfg.Addr)
	if err != nil {
		return nil, err
	}

	dialer := (&net.Dialer{Timeout: d.cfg.ConnectTimeout}).DialContext

	options := []client.Option{
		func(c *client.Conn) error {
			c.ReadTimeout = d.cfg.ReadTimeout
			c.WriteTimeout = d.cfg.WriteTimeout
			if d.cfg.TLS {
				c.UseSSL(d.cfg.TLSInsecureSkipVerify)
			}
			if d.cfg.Compress {
				if err := c.SetCapability(mysql.CLIENT_COMPRESS); err != nil {
					return err
				}
			}
			return nil
		},
	}

	conn, err := client.ConnectWithDialer(ctx, network, address, d.cfg.User, d.cfg.Password, "", dialer, options...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func resolveMySQLAddress(raw string) (network string, address string, err error) {
	addr := strings.TrimSpace(raw)
	if addr == "" {
		return "", "", errors.New("mysql address is empty")
	}

	if strings.Contains(addr, "/") {
		return "unix", addr, nil
	}

	host, port, splitErr := net.SplitHostPort(addr)
	if splitErr == nil {
		if port == "" {
			return "", "", fmt.Errorf("invalid mysql address %q: empty port", raw)
		}
		return "tcp", net.JoinHostPort(host, port), nil
	}

	if !strings.Contains(addr, ":") {
		return "tcp", net.JoinHostPort(addr, defaultMySQLPort), nil
	}

	if strings.HasPrefix(addr, "[") && !strings.Contains(addr, "]") {
		return "", "", fmt.Errorf("invalid mysql address %q: malformed IPv6 host", raw)
	}

	if strings.HasPrefix(addr, "[") && strings.HasSuffix(addr, "]") {
		trimmedHost := strings.TrimSuffix(strings.TrimPrefix(addr, "["), "]")
		return "tcp", net.JoinHostPort(trimmedHost, defaultMySQLPort), nil
	}

	if strings.Count(addr, ":") > 1 && !strings.Contains(addr, "]") {
		return "tcp", net.JoinHostPort(addr, defaultMySQLPort), nil
	}

	return "", "", fmt.Errorf("invalid mysql address %q: use host[:port] or unix socket path", raw)
}

func (d *Dumper) enterCloneMode(conn *client.Conn) error {
	d.debugf("enter clone mode: sending COM_CLONE")
	if err := d.sendCommand(conn, byte(mysql.COM_CLONE), nil); err != nil {
		return err
	}

	pkt, err := conn.ReadPacket()
	if err != nil {
		return fmt.Errorf("read COM_CLONE response: %w", err)
	}
	if len(pkt) == 0 {
		return errors.New("empty COM_CLONE response")
	}

	switch pkt[0] {
	case mysql.OK_HEADER, mysql.EOF_HEADER:
		d.debugf("enter clone mode: response header=0x%02x len=%d", pkt[0], len(pkt))
		return nil
	case mysql.ERR_HEADER:
		return conn.HandleErrorPacket(pkt)
	default:
		return fmt.Errorf("unexpected COM_CLONE response header 0x%02x", pkt[0])
	}
}

func (d *Dumper) sendCommand(conn *client.Conn, command byte, payload []byte) error {
	d.debugPacketf("SEND command=%s(%d) payload_len=%d payload_preview=%s",
		commandName(command), command, len(payload), hexPreview(payload, 32))
	conn.ResetSequence()

	packet := make([]byte, 5+len(payload))
	packet[4] = command
	copy(packet[5:], payload)

	if err := conn.WritePacket(packet); err != nil {
		return err
	}

	d.mu.Lock()
	d.manifest.Stats.CommandPackets++
	d.mu.Unlock()
	if d.writer != nil {
		if err := d.writer.writeFrame(frameDirCommand, command, payload); err != nil {
			return err
		}
	}

	return nil
}

func (d *Dumper) readCloneResponses(ctx context.Context, conn *client.Conn, stage string, onPacket func(resp cloneResponse) error) error {
	done := make(chan struct{})
	if ctx.Done() != nil {
		go func() {
			select {
			case <-ctx.Done():
				_ = conn.Close()
			case <-done:
			}
		}()
	}
	defer close(done)

	for {
		select {
		case <-ctx.Done():
			d.reportInterrupt(ctx.Err().Error())
			return ctx.Err()
		default:
		}

		// Each clone response is sent as a separate net transaction and starts
		// from sequence id 0.
		conn.ResetSequence()

		pkt, err := conn.ReadPacket()
		if err != nil {
			if ctx.Err() != nil {
				d.reportInterrupt(ctx.Err().Error())
				return ctx.Err()
			}
			d.debugf("read response error: %v", err)
			return err
		}
		resp, err := parseCloneResponsePacket(pkt)
		if err != nil {
			return err
		}

		code := resp.Code
		payload := resp.Payload
		d.noteResponse(stage, code)
		d.mu.Lock()
		d.respCodeCount[code]++
		d.manifest.Stats.ResponsePackets++
		d.mu.Unlock()
		d.debugPacketf("RECV code=%s(%d) payload_len=%d payload_preview=%s",
			responseName(code), code, len(payload), hexPreview(payload, 32))

		if d.writer != nil {
			if err := d.writer.writeFrame(frameDirResponse, code, payload); err != nil {
				return err
			}
		}

		switch code {
		case resComplete:
			d.debugf("received COM_RES_COMPLETE")
			return nil

		case resError:
			errNo, message, err := parseCloneError(payload)
			if err != nil {
				return fmt.Errorf("parse COM_RES_ERROR: %w", err)
			}
			d.debugf("received COM_RES_ERROR err_no=%d message=%q", errNo, message)
			return fmt.Errorf("donor COM_RES_ERROR %d: %s", errNo, message)

		default:
			if onPacket != nil {
				if err := onPacket(resp); err != nil {
					return err
				}
			}
		}
	}
}
