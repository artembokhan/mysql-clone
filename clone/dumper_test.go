package clone

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSerializeInitCommand(t *testing.T) {
	payload := serializeInitCommand(0x0102, 300, false, nil)
	if got := binary.LittleEndian.Uint32(payload[:4]); got != 0x0102 {
		t.Fatalf("version mismatch: got=%x", got)
	}

	timeout := binary.LittleEndian.Uint32(payload[4:8])
	if timeout&(1<<31) == 0 {
		t.Fatalf("expected no-backup-lock flag to be set")
	}
	if timeout&^uint32(1<<31) != 300 {
		t.Fatalf("timeout mismatch: got=%d", timeout&^uint32(1<<31))
	}
}

func TestConfigValidateMode(t *testing.T) {
	cfg := DefaultConfig()
	cfg.User = "root"
	cfg.OutDir = t.TempDir()

	for _, mode := range []string{ModeInnoDB, ModeBinary} {
		cfg.Mode = mode
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected mode %q to pass validation, got error: %v", mode, err)
		}
	}

	cfg.Mode = "unknown"
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected unknown mode to fail validation")
	}
}

func TestConfigValidateDryRunWithoutOutDir(t *testing.T) {
	cfg := DefaultConfig()
	cfg.User = "root"
	cfg.OutDir = ""
	cfg.DryRun = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected dry-run config without out dir to pass validation: %v", err)
	}
}

func TestConfigValidateProgressInterval(t *testing.T) {
	cfg := DefaultConfig()
	cfg.User = "root"
	cfg.OutDir = t.TempDir()
	cfg.ProgressInterval = -time.Second
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected negative progress interval to fail validation")
	}
}

func TestDumperCleansCreatedOutputsOnFailure(t *testing.T) {
	modes := []string{ModeBinary, ModeInnoDB}
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			baseDir := t.TempDir()
			outDir := filepath.Join(baseDir, "out")

			cfg := DefaultConfig()
			cfg.User = "root"
			cfg.Password = "x"
			cfg.Addr = "127.0.0.1:1"
			cfg.OutDir = outDir
			cfg.Mode = mode
			cfg.ConnectTimeout = 200 * time.Millisecond
			cfg.ReadTimeout = time.Second
			cfg.WriteTimeout = time.Second

			dumper, err := NewDumper(cfg)
			if err != nil {
				t.Fatalf("NewDumper error: %v", err)
			}

			err = dumper.Run(context.Background())
			if err == nil {
				t.Fatalf("expected run to fail due connection error")
			}

			if _, statErr := os.Stat(outDir); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("expected output directory to be cleaned up, got stat err=%v", statErr)
			}
		})
	}
}

func TestParseCloneResponsePacket(t *testing.T) {
	resp, err := parseCloneResponsePacket([]byte{resConfig, 1, 2, 3})
	if err != nil {
		t.Fatalf("parseCloneResponsePacket error: %v", err)
	}
	if resp.Code != resConfig {
		t.Fatalf("code mismatch: got=%d", resp.Code)
	}
	if len(resp.Payload) != 3 {
		t.Fatalf("payload length mismatch: got=%d", len(resp.Payload))
	}
}

func TestParseCloneResponsePacketEmpty(t *testing.T) {
	if _, err := parseCloneResponsePacket(nil); err == nil {
		t.Fatalf("expected parseCloneResponsePacket to fail on empty packet")
	}
}

func TestParseLocators(t *testing.T) {
	payload := make([]byte, 0)
	version := make([]byte, 4)
	binary.LittleEndian.PutUint32(version, 0x0102)
	payload = append(payload, version...)

	loc1 := []byte{1, 3, 0, 0, 0, 10, 11, 12}
	loc2 := []byte{2, 2, 0, 0, 0, 20, 21}
	payload = append(payload, loc1...)
	payload = append(payload, loc2...)

	gotVersion, locs, err := parseLocators(payload)
	if err != nil {
		t.Fatalf("parseLocators error: %v", err)
	}
	if gotVersion != 0x0102 {
		t.Fatalf("version mismatch: got=%x", gotVersion)
	}
	if len(locs) != 2 {
		t.Fatalf("locators mismatch: got=%d", len(locs))
	}
	if locs[0].DBType != 1 || len(locs[0].Data) != 3 {
		t.Fatalf("locator[0] mismatch")
	}
	if locs[1].DBType != 2 || len(locs[1].Data) != 2 {
		t.Fatalf("locator[1] mismatch")
	}
}

func TestParseKeyValue(t *testing.T) {
	key := []byte("plugin")
	val := []byte("mysql_clone")

	payload := make([]byte, 4+len(key)+4+len(val))
	binary.LittleEndian.PutUint32(payload[0:4], uint32(len(key)))
	copy(payload[4:], key)
	offset := 4 + len(key)
	binary.LittleEndian.PutUint32(payload[offset:offset+4], uint32(len(val)))
	copy(payload[offset+4:], val)

	gotKey, gotValue, err := parseKeyValue(payload)
	if err != nil {
		t.Fatalf("parseKeyValue error: %v", err)
	}
	if gotKey != string(key) || gotValue != string(val) {
		t.Fatalf("unexpected key-value: %s=%s", gotKey, gotValue)
	}
}

func TestParseDescriptor(t *testing.T) {
	payload := []byte{7, 2, 0xaa, 0xbb, 0xcc}
	desc, err := parseDescriptor(payload)
	if err != nil {
		t.Fatalf("parseDescriptor error: %v", err)
	}
	if desc.DBType != 7 || desc.LocIndex != 2 || len(desc.Body) != 3 {
		t.Fatalf("descriptor mismatch: %+v", desc)
	}
}

func TestParseCloneFileDescriptor(t *testing.T) {
	name := []byte("testdb/t.ibd\x00")
	totalLen := cloneFileBaseLen + len(name)
	raw := make([]byte, totalLen)
	binary.BigEndian.PutUint32(raw[0:4], 100)
	binary.BigEndian.PutUint32(raw[4:8], uint32(totalLen))
	binary.BigEndian.PutUint32(raw[8:12], cloneDescTypeFileMetadata)
	binary.BigEndian.PutUint32(raw[cloneFileStateOffset:cloneFileSizeOffset], 1)
	binary.BigEndian.PutUint64(raw[cloneFileSizeOffset:cloneFileAllocSizeOffset], 4096)
	binary.BigEndian.PutUint64(raw[cloneFileAllocSizeOffset:cloneFileFSPOffset], 4096)
	binary.BigEndian.PutUint32(raw[cloneFileFSPOffset:cloneFileFSBLKOffset], 0)
	binary.BigEndian.PutUint32(raw[cloneFileFSBLKOffset:cloneFileFlagsOffset], 0)
	binary.BigEndian.PutUint16(raw[cloneFileFlagsOffset:cloneFileSpaceIDOffset], 1<<(cloneFileFlagRenamed-1))
	binary.BigEndian.PutUint32(raw[cloneFileSpaceIDOffset:cloneFileIndexOffset], 42)
	binary.BigEndian.PutUint32(raw[cloneFileIndexOffset:cloneFileBChunkOffset], 9)
	binary.BigEndian.PutUint32(raw[cloneFileBChunkOffset:cloneFileEChunkOffset], 1)
	binary.BigEndian.PutUint32(raw[cloneFileEChunkOffset:cloneFileFNameLenOffset], 3)
	binary.BigEndian.PutUint32(raw[cloneFileFNameLenOffset:cloneFileFNameOffset], uint32(len(name)))
	copy(raw[cloneFileFNameOffset:], name)

	got, err := parseCloneFileDescriptor(raw)
	if err != nil {
		t.Fatalf("parseCloneFileDescriptor error: %v", err)
	}
	if got.FileIndex != 9 {
		t.Fatalf("file index mismatch: got=%d", got.FileIndex)
	}
	if got.SpaceID != 42 {
		t.Fatalf("space id mismatch: got=%d", got.SpaceID)
	}
	if got.FileName != "testdb/t.ibd" {
		t.Fatalf("file name mismatch: got=%q", got.FileName)
	}
	if !cloneFlagSet(got.Flags, cloneFileFlagRenamed) {
		t.Fatalf("expected renamed flag in file descriptor flags=%#x", got.Flags)
	}
}

func TestParseCloneDataDescriptor(t *testing.T) {
	raw := make([]byte, cloneDataDescLen)
	binary.BigEndian.PutUint32(raw[0:4], 100)
	binary.BigEndian.PutUint32(raw[4:8], cloneDataDescLen)
	binary.BigEndian.PutUint32(raw[8:12], cloneDescTypeData)
	binary.BigEndian.PutUint32(raw[cloneDataStateOffset:cloneDataTaskIndex], 1)
	binary.BigEndian.PutUint32(raw[cloneDataTaskIndex:cloneDataTaskChunk], 2)
	binary.BigEndian.PutUint32(raw[cloneDataTaskChunk:cloneDataTaskBlock], 3)
	binary.BigEndian.PutUint32(raw[cloneDataTaskBlock:cloneDataFileIndex], 4)
	binary.BigEndian.PutUint32(raw[cloneDataFileIndex:cloneDataLenOffset], 8)
	binary.BigEndian.PutUint32(raw[cloneDataLenOffset:cloneDataFileOffOffset], 16)
	binary.BigEndian.PutUint64(raw[cloneDataFileOffOffset:cloneDataFileSizeOffset], 128)
	binary.BigEndian.PutUint64(raw[cloneDataFileSizeOffset:cloneDataDescLen], 1024)

	got, err := parseCloneDataDescriptor(raw)
	if err != nil {
		t.Fatalf("parseCloneDataDescriptor error: %v", err)
	}
	if got.FileIndex != 8 || got.DataLen != 16 || got.FileOffset != 128 || got.FileSize != 1024 {
		t.Fatalf("data descriptor mismatch: %+v", got)
	}
}

func TestParseCloneStateDescriptor(t *testing.T) {
	raw := make([]byte, cloneStateDescriptorLen)
	binary.BigEndian.PutUint32(raw[0:4], 100)
	binary.BigEndian.PutUint32(raw[4:8], cloneStateDescriptorLen)
	binary.BigEndian.PutUint32(raw[8:12], cloneDescTypeState)
	binary.BigEndian.PutUint32(raw[cloneStateStateOffset:cloneStateTaskIndex], 1)
	binary.BigEndian.PutUint32(raw[cloneStateTaskIndex:cloneStateNumChunks], 2)
	binary.BigEndian.PutUint32(raw[cloneStateNumChunks:cloneStateNumFiles], 3)
	binary.BigEndian.PutUint32(raw[cloneStateNumFiles:cloneStateEstimateBytes], 4)
	binary.BigEndian.PutUint64(raw[cloneStateEstimateBytes:cloneStateEstimateDisk], 5)
	binary.BigEndian.PutUint64(raw[cloneStateEstimateDisk:cloneStateFlagsOffset], 6)
	binary.BigEndian.PutUint16(raw[cloneStateFlagsOffset:cloneStateDescriptorLen], cloneStateFlagStart)

	got, err := parseCloneStateDescriptor(raw)
	if err != nil {
		t.Fatalf("parseCloneStateDescriptor error: %v", err)
	}
	if got.State != 1 || got.TaskIndex != 2 || got.NumChunks != 3 || got.NumFiles != 4 {
		t.Fatalf("state descriptor mismatch: %+v", got)
	}
	if got.EstimateBytes != 5 || got.EstimateDisk != 6 {
		t.Fatalf("state descriptor estimates mismatch: %+v", got)
	}
	if got.Flags != cloneStateFlagStart {
		t.Fatalf("state descriptor flags mismatch: %d", got.Flags)
	}
}

func TestBuildCloneStateACKDescriptor(t *testing.T) {
	raw := make([]byte, cloneStateDescriptorLen)
	binary.BigEndian.PutUint32(raw[0:4], 100)
	binary.BigEndian.PutUint32(raw[4:8], cloneStateDescriptorLen)
	binary.BigEndian.PutUint32(raw[8:12], cloneDescTypeState)
	binary.BigEndian.PutUint16(raw[cloneStateFlagsOffset:cloneStateDescriptorLen], cloneStateFlagStart)

	acked, err := buildCloneStateACKDescriptor(raw)
	if err != nil {
		t.Fatalf("buildCloneStateACKDescriptor error: %v", err)
	}
	if len(acked) != len(raw) {
		t.Fatalf("length mismatch: got=%d want=%d", len(acked), len(raw))
	}
	flags := binary.BigEndian.Uint16(acked[cloneStateFlagsOffset:cloneStateDescriptorLen])
	if flags&cloneStateFlagACK == 0 {
		t.Fatalf("expected ACK flag in state descriptor, flags=%d", flags)
	}
	if flags&cloneStateFlagStart == 0 {
		t.Fatalf("expected START flag to be preserved, flags=%d", flags)
	}
}

func TestSanitizeCloneFilePath(t *testing.T) {
	got, err := sanitizeCloneFilePath("./testdb/t.ibd")
	if err != nil {
		t.Fatalf("sanitizeCloneFilePath error: %v", err)
	}
	if got != filepath.FromSlash("testdb/t.ibd") {
		t.Fatalf("path mismatch: %q", got)
	}

	if _, err := sanitizeCloneFilePath("../etc/passwd"); err == nil {
		t.Fatalf("expected sanitizeCloneFilePath to reject upward traversal")
	}
	if _, err := sanitizeCloneFilePath("/var/lib/mysql/ibdata1"); err == nil {
		t.Fatalf("expected sanitizeCloneFilePath to reject absolute paths")
	}
	if _, err := sanitizeCloneFilePath(`C:\mysql\data\ibdata1`); err == nil {
		t.Fatalf("expected sanitizeCloneFilePath to reject drive-like paths")
	}
}

func TestCloneRestorerApplyData(t *testing.T) {
	tmp := t.TempDir()
	noLog := func(string, ...any) {}

	restorer, err := newCloneRestorer(tmp, noLog, noLog)
	if err != nil {
		t.Fatalf("newCloneRestorer error: %v", err)
	}

	fileDesc := cloneFileDescriptor{
		FileIndex: 7,
		FileName:  "./testdb/t.ibd",
		FileSize:  8,
	}
	if err := restorer.registerFile(fileDesc); err != nil {
		t.Fatalf("registerFile error: %v", err)
	}

	payload := []byte("ABCD")
	dataDesc := cloneDataDescriptor{
		FileIndex:  7,
		DataLen:    uint32(len(payload)),
		FileOffset: 2,
		FileSize:   8,
	}
	if _, err := restorer.applyData(dataDesc, payload); err != nil {
		t.Fatalf("applyData error: %v", err)
	}
	if err := restorer.close(); err != nil {
		t.Fatalf("restorer close error: %v", err)
	}

	outPath := filepath.Join(tmp, filepath.FromSlash("testdb/t.ibd"))
	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read restored file error: %v", err)
	}
	if len(got) != 8 {
		t.Fatalf("restored file length mismatch: got=%d", len(got))
	}
	if !bytes.Equal(got[2:6], payload) {
		t.Fatalf("restored payload mismatch: got=%q", got[2:6])
	}
}

func TestResolveMySQLAddress(t *testing.T) {
	testCases := []struct {
		name        string
		raw         string
		network     string
		address     string
		expectError bool
	}{
		{name: "host", raw: "127.0.0.1", network: "tcp", address: "127.0.0.1:3306"},
		{name: "host-port", raw: "127.0.0.1:3307", network: "tcp", address: "127.0.0.1:3307"},
		{name: "ipv6-host", raw: "::1", network: "tcp", address: "[::1]:3306"},
		{name: "ipv6-port", raw: "[::1]:3307", network: "tcp", address: "[::1]:3307"},
		{name: "socket", raw: "/tmp/mysql.sock", network: "unix", address: "/tmp/mysql.sock"},
		{name: "bad", raw: "[::1", expectError: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			network, address, err := resolveMySQLAddress(tc.raw)
			if tc.expectError {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveMySQLAddress error: %v", err)
			}
			if network != tc.network || address != tc.address {
				t.Fatalf("unexpected resolve result: network=%q address=%q", network, address)
			}
		})
	}
}
