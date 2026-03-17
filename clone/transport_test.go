package clone

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/packet"
)

func TestReadCloneResponsesCanceledReports(t *testing.T) {
	cfg := DefaultConfig()
	cfg.User = "test"
	cfg.DryRun = true

	dumper, err := NewDumper(cfg)
	if err != nil {
		t.Fatalf("NewDumper error: %v", err)
	}

	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	pconn := packet.NewConnWithTimeout(clientConn, time.Second, time.Second, 1024)
	conn := &client.Conn{Conn: pconn}
	dumper.noteResponse("TEST", 0)

	oldStderr := os.Stderr
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe error: %v", err)
	}
	os.Stderr = writePipe
	t.Cleanup(func() {
		os.Stderr = oldStderr
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- dumper.readCloneResponses(ctx, conn, "TEST", nil)
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()

	var readErr error
	select {
	case readErr = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for readCloneResponses")
	}

	_ = writePipe.Close()
	output, _ := io.ReadAll(readPipe)
	_ = readPipe.Close()

	if !errors.Is(readErr, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", readErr)
	}

	text := string(output)
	if !strings.Contains(text, "interrupted:") {
		t.Fatalf("expected interrupt report, got %q", text)
	}
	if !strings.Contains(text, "stage=TEST") {
		t.Fatalf("expected stage in report, got %q", text)
	}
}
