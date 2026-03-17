package clone

import (
	"encoding/binary"
	"errors"
	"fmt"
)

func serializeInitCommand(version uint32, ddlTimeoutSec uint32, backupLock bool, locators [][]byte) []byte {
	timeoutValue := ddlTimeoutSec
	if !backupLock {
		timeoutValue |= noBackupLockFlag
	}

	total := 8
	for _, loc := range locators {
		total += len(loc)
	}

	buf := make([]byte, total)
	binary.LittleEndian.PutUint32(buf[0:4], version)
	binary.LittleEndian.PutUint32(buf[4:8], timeoutValue)

	offset := 8
	for _, loc := range locators {
		copy(buf[offset:], loc)
		offset += len(loc)
	}

	return buf
}

func serializeAckCommand(errCode int32, serializedLocator []byte, descriptor []byte) []byte {
	buf := make([]byte, 4+len(serializedLocator)+4+len(descriptor))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(errCode))

	offset := 4
	copy(buf[offset:], serializedLocator)
	offset += len(serializedLocator)

	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(descriptor)))
	offset += 4
	copy(buf[offset:], descriptor)

	return buf
}

func parseCloneResponsePacket(packet []byte) (cloneResponse, error) {
	if len(packet) == 0 {
		return cloneResponse{}, errors.New("empty clone response packet")
	}
	return cloneResponse{
		Code:    packet[0],
		Payload: packet[1:],
	}, nil
}

func parseLocators(payload []byte) (uint32, []locator, error) {
	r := newCodecReader(payload)
	version, err := r.readUint32(binary.LittleEndian)
	if err != nil {
		return 0, nil, fmt.Errorf("locator payload: %w", err)
	}

	locs := make([]locator, 0)
	for r.remaining() > 0 {
		locStart := r.pos

		dbType, err := r.readUint8()
		if err != nil {
			return 0, nil, fmt.Errorf("locator db type: %w", err)
		}

		locLen, err := r.readUint32(binary.LittleEndian)
		if err != nil {
			return 0, nil, fmt.Errorf("locator length: %w", err)
		}
		locLenInt, err := uint32ToInt(locLen)
		if err != nil {
			return 0, nil, fmt.Errorf("locator length conversion: %w", err)
		}

		locData, err := r.readBytes(locLenInt)
		if err != nil {
			return 0, nil, fmt.Errorf("locator body len=%d: %w", locLen, err)
		}

		raw := append([]byte(nil), payload[locStart:r.pos]...)
		locData = append([]byte(nil), locData...)
		locs = append(locs, locator{DBType: dbType, Raw: raw, Data: locData})
	}

	return version, locs, nil
}

func parseDescriptor(payload []byte) (descriptor, error) {
	r := newCodecReader(payload)
	dbType, err := r.readUint8()
	if err != nil {
		return descriptor{}, fmt.Errorf("descriptor db type: %w", err)
	}
	locIndex, err := r.readUint8()
	if err != nil {
		return descriptor{}, fmt.Errorf("descriptor locator index: %w", err)
	}
	body, err := r.readBytes(r.remaining())
	if err != nil {
		return descriptor{}, fmt.Errorf("descriptor body: %w", err)
	}
	return descriptor{
		DBType:   dbType,
		LocIndex: locIndex,
		Body:     append([]byte(nil), body...),
	}, nil
}

func parseCloneError(payload []byte) (int32, string, error) {
	r := newCodecReader(payload)
	errCodeU32, err := r.readUint32(binary.LittleEndian)
	if err != nil {
		return 0, "", fmt.Errorf("error code: %w", err)
	}

	msgRaw, err := r.readBytes(r.remaining())
	if err != nil {
		return 0, "", fmt.Errorf("error message: %w", err)
	}

	errCode := int32(errCodeU32)
	msg := string(msgRaw)
	return errCode, msg, nil
}

func parseKeyOnly(payload []byte) (string, error) {
	r := newCodecReader(payload)
	keyLen, err := r.readUint32(binary.LittleEndian)
	if err != nil {
		return "", fmt.Errorf("key length: %w", err)
	}
	keyLenInt, err := uint32ToInt(keyLen)
	if err != nil {
		return "", fmt.Errorf("key length conversion: %w", err)
	}
	keyRaw, err := r.readBytes(keyLenInt)
	if err != nil {
		return "", fmt.Errorf("key body len=%d: %w", keyLen, err)
	}
	return string(keyRaw), nil
}

func parseKeyValue(payload []byte) (string, string, error) {
	r := newCodecReader(payload)

	keyLen, err := r.readUint32(binary.LittleEndian)
	if err != nil {
		return "", "", fmt.Errorf("key length: %w", err)
	}
	keyLenInt, err := uint32ToInt(keyLen)
	if err != nil {
		return "", "", fmt.Errorf("key length conversion: %w", err)
	}
	keyRaw, err := r.readBytes(keyLenInt)
	if err != nil {
		return "", "", fmt.Errorf("key body len=%d: %w", keyLen, err)
	}
	valueLen, err := r.readUint32(binary.LittleEndian)
	if err != nil {
		return "", "", fmt.Errorf("value length: %w", err)
	}
	valueLenInt, err := uint32ToInt(valueLen)
	if err != nil {
		return "", "", fmt.Errorf("value length conversion: %w", err)
	}
	valueRaw, err := r.readBytes(valueLenInt)
	if err != nil {
		return "", "", fmt.Errorf("value body len=%d: %w", valueLen, err)
	}

	return string(keyRaw), string(valueRaw), nil
}
