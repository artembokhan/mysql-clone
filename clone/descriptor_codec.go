package clone

import (
	"encoding/binary"
	"errors"
	"fmt"
)

func parseCloneDescHeader(desc []byte) (cloneDescHeader, error) {
	if len(desc) < cloneDescHeaderLen {
		return cloneDescHeader{}, fmt.Errorf("descriptor too short: %d", len(desc))
	}

	header := cloneDescHeader{
		Version: binary.BigEndian.Uint32(desc[0:4]),
		Length:  binary.BigEndian.Uint32(desc[4:8]),
		Type:    binary.BigEndian.Uint32(desc[8:12]),
	}
	if header.Length < cloneDescHeaderLen {
		return cloneDescHeader{}, fmt.Errorf("invalid descriptor length: %d", header.Length)
	}
	if int(header.Length) > len(desc) {
		return cloneDescHeader{}, fmt.Errorf("truncated descriptor body: have=%d need=%d", len(desc), header.Length)
	}

	return header, nil
}

func parseCloneFileDescriptor(desc []byte) (cloneFileDescriptor, error) {
	header, err := parseCloneDescHeader(desc)
	if err != nil {
		return cloneFileDescriptor{}, err
	}
	if header.Type != cloneDescTypeFileMetadata {
		return cloneFileDescriptor{}, fmt.Errorf("unexpected descriptor type for file metadata: %d", header.Type)
	}

	raw := desc[:header.Length]
	r := newCodecReader(raw[cloneDescHeaderLen:])

	state, err := r.readUint32(binary.BigEndian)
	if err != nil {
		return cloneFileDescriptor{}, fmt.Errorf("file state: %w", err)
	}
	fileSize, err := r.readUint64(binary.BigEndian)
	if err != nil {
		return cloneFileDescriptor{}, fmt.Errorf("file size: %w", err)
	}
	allocSize, err := r.readUint64(binary.BigEndian)
	if err != nil {
		return cloneFileDescriptor{}, fmt.Errorf("alloc size: %w", err)
	}
	fspFlags, err := r.readUint32(binary.BigEndian)
	if err != nil {
		return cloneFileDescriptor{}, fmt.Errorf("fsp flags: %w", err)
	}
	fsBlockSize, err := r.readUint32(binary.BigEndian)
	if err != nil {
		return cloneFileDescriptor{}, fmt.Errorf("fsp block size: %w", err)
	}
	flags, err := r.readUint16(binary.BigEndian)
	if err != nil {
		return cloneFileDescriptor{}, fmt.Errorf("file flags: %w", err)
	}
	spaceID, err := r.readUint32(binary.BigEndian)
	if err != nil {
		return cloneFileDescriptor{}, fmt.Errorf("space id: %w", err)
	}
	fileIndex, err := r.readUint32(binary.BigEndian)
	if err != nil {
		return cloneFileDescriptor{}, fmt.Errorf("file index: %w", err)
	}
	beginChunk, err := r.readUint32(binary.BigEndian)
	if err != nil {
		return cloneFileDescriptor{}, fmt.Errorf("begin chunk: %w", err)
	}
	endChunk, err := r.readUint32(binary.BigEndian)
	if err != nil {
		return cloneFileDescriptor{}, fmt.Errorf("end chunk: %w", err)
	}
	fileNameLen, err := r.readUint32(binary.BigEndian)
	if err != nil {
		return cloneFileDescriptor{}, fmt.Errorf("file name length: %w", err)
	}
	fileNameLenInt, err := uint32ToInt(fileNameLen)
	if err != nil {
		return cloneFileDescriptor{}, fmt.Errorf("file name length conversion: %w", err)
	}

	fileName := ""
	if fileNameLen > 0 {
		fileNameRaw, err := r.readBytes(fileNameLenInt)
		if err != nil {
			return cloneFileDescriptor{}, fmt.Errorf("file name body len=%d: %w", fileNameLen, err)
		}
		if fileNameRaw[len(fileNameRaw)-1] != 0 {
			return cloneFileDescriptor{}, errors.New("file name in descriptor is not NUL-terminated")
		}
		fileName = string(fileNameRaw[:len(fileNameRaw)-1])
	}

	return cloneFileDescriptor{
		Header:      header,
		State:       state,
		FileSize:    fileSize,
		AllocSize:   allocSize,
		FSPFlags:    fspFlags,
		FSBlockSize: fsBlockSize,
		Flags:       flags,
		SpaceID:     spaceID,
		FileIndex:   fileIndex,
		BeginChunk:  beginChunk,
		EndChunk:    endChunk,
		FileName:    fileName,
	}, nil
}

func parseCloneDataDescriptor(desc []byte) (cloneDataDescriptor, error) {
	header, err := parseCloneDescHeader(desc)
	if err != nil {
		return cloneDataDescriptor{}, err
	}
	if header.Type != cloneDescTypeData {
		return cloneDataDescriptor{}, fmt.Errorf("unexpected descriptor type for data descriptor: %d", header.Type)
	}

	raw := desc[:header.Length]
	r := newCodecReader(raw[cloneDescHeaderLen:])

	state, err := r.readUint32(binary.BigEndian)
	if err != nil {
		return cloneDataDescriptor{}, fmt.Errorf("data state: %w", err)
	}
	taskIndex, err := r.readUint32(binary.BigEndian)
	if err != nil {
		return cloneDataDescriptor{}, fmt.Errorf("task index: %w", err)
	}
	taskChunk, err := r.readUint32(binary.BigEndian)
	if err != nil {
		return cloneDataDescriptor{}, fmt.Errorf("task chunk: %w", err)
	}
	taskBlock, err := r.readUint32(binary.BigEndian)
	if err != nil {
		return cloneDataDescriptor{}, fmt.Errorf("task block: %w", err)
	}
	fileIndex, err := r.readUint32(binary.BigEndian)
	if err != nil {
		return cloneDataDescriptor{}, fmt.Errorf("file index: %w", err)
	}
	dataLen, err := r.readUint32(binary.BigEndian)
	if err != nil {
		return cloneDataDescriptor{}, fmt.Errorf("data len: %w", err)
	}
	fileOffset, err := r.readUint64(binary.BigEndian)
	if err != nil {
		return cloneDataDescriptor{}, fmt.Errorf("file offset: %w", err)
	}
	fileSize, err := r.readUint64(binary.BigEndian)
	if err != nil {
		return cloneDataDescriptor{}, fmt.Errorf("file size: %w", err)
	}

	return cloneDataDescriptor{
		Header:     header,
		State:      state,
		TaskIndex:  taskIndex,
		TaskChunk:  taskChunk,
		TaskBlock:  taskBlock,
		FileIndex:  fileIndex,
		DataLen:    dataLen,
		FileOffset: fileOffset,
		FileSize:   fileSize,
	}, nil
}

func parseCloneStateDescriptor(desc []byte) (cloneStateDescriptor, error) {
	header, err := parseCloneDescHeader(desc)
	if err != nil {
		return cloneStateDescriptor{}, err
	}
	if header.Type != cloneDescTypeState {
		return cloneStateDescriptor{}, fmt.Errorf("unexpected descriptor type for state descriptor: %d", header.Type)
	}
	if header.Length < cloneStateDescriptorLen {
		return cloneStateDescriptor{}, fmt.Errorf("state descriptor too short: %d", header.Length)
	}

	raw := desc[:header.Length]
	r := newCodecReader(raw[cloneDescHeaderLen:])

	state, err := r.readUint32(binary.BigEndian)
	if err != nil {
		return cloneStateDescriptor{}, fmt.Errorf("state state: %w", err)
	}
	taskIndex, err := r.readUint32(binary.BigEndian)
	if err != nil {
		return cloneStateDescriptor{}, fmt.Errorf("state task index: %w", err)
	}
	numChunks, err := r.readUint32(binary.BigEndian)
	if err != nil {
		return cloneStateDescriptor{}, fmt.Errorf("state chunk count: %w", err)
	}
	numFiles, err := r.readUint32(binary.BigEndian)
	if err != nil {
		return cloneStateDescriptor{}, fmt.Errorf("state file count: %w", err)
	}
	estimateBytes, err := r.readUint64(binary.BigEndian)
	if err != nil {
		return cloneStateDescriptor{}, fmt.Errorf("state estimate bytes: %w", err)
	}
	estimateDisk, err := r.readUint64(binary.BigEndian)
	if err != nil {
		return cloneStateDescriptor{}, fmt.Errorf("state estimate disk: %w", err)
	}
	flags, err := r.readUint16(binary.BigEndian)
	if err != nil {
		return cloneStateDescriptor{}, fmt.Errorf("state flags: %w", err)
	}

	return cloneStateDescriptor{
		Header:        header,
		State:         state,
		TaskIndex:     taskIndex,
		NumChunks:     numChunks,
		NumFiles:      numFiles,
		EstimateBytes: estimateBytes,
		EstimateDisk:  estimateDisk,
		Flags:         flags,
	}, nil
}

func buildCloneStateACKDescriptor(desc []byte) ([]byte, error) {
	header, err := parseCloneDescHeader(desc)
	if err != nil {
		return nil, err
	}
	if header.Type != cloneDescTypeState {
		return nil, fmt.Errorf("unexpected descriptor type for state ACK descriptor: %d", header.Type)
	}
	if header.Length < cloneStateDescriptorLen {
		return nil, fmt.Errorf("state descriptor too short for ACK: %d", header.Length)
	}

	result := append([]byte(nil), desc[:header.Length]...)
	flags := binary.BigEndian.Uint16(result[cloneStateFlagsOffset : cloneStateFlagsOffset+2])
	flags |= cloneStateFlagACK
	binary.BigEndian.PutUint16(result[cloneStateFlagsOffset:cloneStateFlagsOffset+2], flags)
	return result, nil
}

func cloneDescriptorTypeName(descType uint32) string {
	switch descType {
	case cloneDescTypeLocator:
		return "CLONE_DESC_LOCATOR"
	case cloneDescTypeTaskMetadata:
		return "CLONE_DESC_TASK_METADATA"
	case cloneDescTypeState:
		return "CLONE_DESC_STATE"
	case cloneDescTypeFileMetadata:
		return "CLONE_DESC_FILE_METADATA"
	case cloneDescTypeData:
		return "CLONE_DESC_DATA"
	default:
		return "CLONE_DESC_UNKNOWN"
	}
}
