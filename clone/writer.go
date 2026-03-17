package clone

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	writerBufferSize = 1 << 20
	frameHeaderSize  = 16
)

type dumpWriter struct {
	streamFile *os.File
	stream     *bufio.Writer
	dataFile   *os.File
	data       *bufio.Writer
}

func newDumpWriter(outDir string) (*dumpWriter, error) {
	streamPath := filepath.Join(outDir, "stream.bin")
	dataPath := filepath.Join(outDir, "data.bin")

	streamFile, err := os.OpenFile(streamPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o666)
	if err != nil {
		return nil, fmt.Errorf("create stream file: %w", err)
	}

	dataFile, err := os.OpenFile(dataPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o666)
	if err != nil {
		_ = streamFile.Close()
		_ = os.Remove(streamPath)
		return nil, fmt.Errorf("create data file: %w", err)
	}

	dw := &dumpWriter{
		streamFile: streamFile,
		stream:     bufio.NewWriterSize(streamFile, writerBufferSize),
		dataFile:   dataFile,
		data:       bufio.NewWriterSize(dataFile, writerBufferSize),
	}

	if _, err := dw.stream.Write(streamMagic[:]); err != nil {
		_ = dw.close()
		return nil, fmt.Errorf("write stream header: %w", err)
	}

	return dw, nil
}

func (d *dumpWriter) writeFrame(direction byte, code byte, payload []byte) error {
	if len(payload) > int(^uint32(0)) {
		return fmt.Errorf("payload too large: %d", len(payload))
	}

	var header [frameHeaderSize]byte
	header[0] = direction
	header[1] = code
	binary.LittleEndian.PutUint64(header[4:], uint64(time.Now().UnixNano()))
	binary.LittleEndian.PutUint32(header[12:], uint32(len(payload)))

	if _, err := d.stream.Write(header[:]); err != nil {
		return fmt.Errorf("write frame header: %w", err)
	}

	if len(payload) == 0 {
		return nil
	}

	if _, err := d.stream.Write(payload); err != nil {
		return fmt.Errorf("write frame payload: %w", err)
	}

	return nil
}

func (d *dumpWriter) writeData(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}

	if _, err := d.data.Write(payload); err != nil {
		return fmt.Errorf("write data payload: %w", err)
	}
	return nil
}

func (d *dumpWriter) close() error {
	var errs []error

	if d.stream != nil {
		if err := d.stream.Flush(); err != nil {
			errs = append(errs, err)
		}
	}
	if d.data != nil {
		if err := d.data.Flush(); err != nil {
			errs = append(errs, err)
		}
	}
	if d.streamFile != nil {
		if err := d.streamFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if d.dataFile != nil {
		if err := d.dataFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}
