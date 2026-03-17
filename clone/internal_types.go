package clone

const (
	cloneProtocolVersionV3 = uint32(0x0102)
	noBackupLockFlag       = uint32(1 << 31)

	cloneSnapshotNone     = uint32(0)
	cloneSnapshotInit     = uint32(1)
	cloneSnapshotFileCopy = uint32(2)
	cloneSnapshotPageCopy = uint32(3)
	cloneSnapshotRedoCopy = uint32(4)
	cloneSnapshotDone     = uint32(5)

	cmdInit    byte = 1
	cmdAttach  byte = 2
	cmdReinit  byte = 3
	cmdExecute byte = 4
	cmdAck     byte = 5
	cmdExit    byte = 6

	resLocs      byte = 1
	resDataDesc  byte = 2
	resData      byte = 3
	resPlugin    byte = 4
	resConfig    byte = 5
	resCollation byte = 6
	resPluginV2  byte = 7
	resConfigV3  byte = 8
	resComplete  byte = 99
	resError     byte = 100

	frameDirCommand  = byte(1)
	frameDirResponse = byte(2)

	cloneDescHeaderLen = 12

	cloneDescTypeLocator      = uint32(1)
	cloneDescTypeTaskMetadata = uint32(2)
	cloneDescTypeState        = uint32(3)
	cloneDescTypeFileMetadata = uint32(4)
	cloneDescTypeData         = uint32(5)

	cloneFileStateOffset     = cloneDescHeaderLen
	cloneFileSizeOffset      = cloneFileStateOffset + 4
	cloneFileAllocSizeOffset = cloneFileSizeOffset + 8
	cloneFileFSPOffset       = cloneFileAllocSizeOffset + 8
	cloneFileFSBLKOffset     = cloneFileFSPOffset + 4
	cloneFileFlagsOffset     = cloneFileFSBLKOffset + 4
	cloneFileSpaceIDOffset   = cloneFileFlagsOffset + 2
	cloneFileIndexOffset     = cloneFileSpaceIDOffset + 4
	cloneFileBChunkOffset    = cloneFileIndexOffset + 4
	cloneFileEChunkOffset    = cloneFileBChunkOffset + 4
	cloneFileFNameLenOffset  = cloneFileEChunkOffset + 4
	cloneFileFNameOffset     = cloneFileFNameLenOffset + 4
	cloneFileBaseLen         = cloneFileFNameOffset

	cloneDataStateOffset    = cloneDescHeaderLen
	cloneDataTaskIndex      = cloneDataStateOffset + 4
	cloneDataTaskChunk      = cloneDataTaskIndex + 4
	cloneDataTaskBlock      = cloneDataTaskChunk + 4
	cloneDataFileIndex      = cloneDataTaskBlock + 4
	cloneDataLenOffset      = cloneDataFileIndex + 4
	cloneDataFileOffOffset  = cloneDataLenOffset + 4
	cloneDataFileSizeOffset = cloneDataFileOffOffset + 8
	cloneDataDescLen        = cloneDataFileSizeOffset + 8

	cloneStateStateOffset   = cloneDescHeaderLen
	cloneStateTaskIndex     = cloneStateStateOffset + 4
	cloneStateNumChunks     = cloneStateTaskIndex + 4
	cloneStateNumFiles      = cloneStateNumChunks + 4
	cloneStateEstimateBytes = cloneStateNumFiles + 4
	cloneStateEstimateDisk  = cloneStateEstimateBytes + 8
	cloneStateFlagsOffset   = cloneStateEstimateDisk + 8
	cloneStateDescriptorLen = cloneStateFlagsOffset + 2

	cloneStateFlagStart = uint16(1 << 0)
	cloneStateFlagACK   = uint16(1 << 1)

	cloneFileFlagZlib    = uint16(1)
	cloneFileFlagLz4     = uint16(2)
	cloneFileFlagAES     = uint16(3)
	cloneFileFlagRenamed = uint16(4)
	cloneFileFlagDeleted = uint16(5)
	cloneFileFlagHasKey  = uint16(6)
)

var streamMagic = [8]byte{'C', 'L', 'N', 'D', 'M', 'P', '0', '1'}

type locator struct {
	DBType byte
	Raw    []byte
	Data   []byte
}

type descriptor struct {
	DBType   byte
	LocIndex byte
	Body     []byte
}

type cloneResponse struct {
	Code    byte
	Payload []byte
}

type cloneDescHeader struct {
	Version uint32
	Length  uint32
	Type    uint32
}

type cloneFileDescriptor struct {
	Header      cloneDescHeader
	State       uint32
	FileSize    uint64
	AllocSize   uint64
	FSPFlags    uint32
	FSBlockSize uint32
	Flags       uint16
	SpaceID     uint32
	FileIndex   uint32
	BeginChunk  uint32
	EndChunk    uint32
	FileName    string
}

type cloneDataDescriptor struct {
	Header     cloneDescHeader
	State      uint32
	TaskIndex  uint32
	TaskChunk  uint32
	TaskBlock  uint32
	FileIndex  uint32
	DataLen    uint32
	FileOffset uint64
	FileSize   uint64
}

type cloneStateDescriptor struct {
	Header        cloneDescHeader
	State         uint32
	TaskIndex     uint32
	NumChunks     uint32
	NumFiles      uint32
	EstimateBytes uint64
	EstimateDisk  uint64
	Flags         uint16
}
