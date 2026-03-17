package clone

import (
	"encoding/binary"
	"fmt"
)

const maxInt = int(^uint(0) >> 1)

type codecReader struct {
	data []byte
	pos  int
}

func newCodecReader(data []byte) *codecReader {
	return &codecReader{data: data}
}

func (r *codecReader) remaining() int {
	return len(r.data) - r.pos
}

func (r *codecReader) readBytes(n int) ([]byte, error) {
	if n < 0 {
		return nil, fmt.Errorf("invalid read size %d", n)
	}
	if r.remaining() < n {
		return nil, fmt.Errorf("short read: need=%d remaining=%d", n, r.remaining())
	}
	begin := r.pos
	r.pos += n
	return r.data[begin:r.pos], nil
}

func (r *codecReader) readUint8() (byte, error) {
	b, err := r.readBytes(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func (r *codecReader) readUint16(order binary.ByteOrder) (uint16, error) {
	b, err := r.readBytes(2)
	if err != nil {
		return 0, err
	}
	return order.Uint16(b), nil
}

func (r *codecReader) readUint32(order binary.ByteOrder) (uint32, error) {
	b, err := r.readBytes(4)
	if err != nil {
		return 0, err
	}
	return order.Uint32(b), nil
}

func (r *codecReader) readUint64(order binary.ByteOrder) (uint64, error) {
	b, err := r.readBytes(8)
	if err != nil {
		return 0, err
	}
	return order.Uint64(b), nil
}

func uint32ToInt(v uint32) (int, error) {
	if uint64(v) > uint64(maxInt) {
		return 0, fmt.Errorf("value %d exceeds int range", v)
	}
	return int(v), nil
}
