package clone

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInnoDBPageChecksumVariants(t *testing.T) {
	page := make([]byte, innodbDefaultPageSize)
	for i := range page {
		page[i] = byte((i * 31) & 0xFF)
	}

	copy(page[filPageLSN:filPageLSN+4], []byte{0, 0, 0, 7})
	copy(page[filPageLSN+4:filPageLSN+8], []byte{1, 2, 3, 4})
	copy(page[len(page)-filPageEndLSNOldChecksum+4:len(page)-filPageEndLSNOldChecksum+8], []byte{1, 2, 3, 4})

	newSum := calcPageNewChecksum(page)
	writeUint32BE(page[filPageSpaceOrChksum:filPageSpaceOrChksum+4], newSum)
	writeUint32BE(page[len(page)-filPageEndLSNOldChecksum:len(page)-filPageEndLSNOldChecksum+4], readUint32BE(page[filPageLSN:filPageLSN+4]))
	if !isInnoDBPageChecksumValid(page) {
		t.Fatalf("expected page with innodb checksum to be valid")
	}

	page2 := append([]byte(nil), page...)
	writeUint32BE(page2[filPageSpaceOrChksum:filPageSpaceOrChksum+4], bufNoChecksumMagic)
	writeUint32BE(page2[len(page2)-filPageEndLSNOldChecksum:len(page2)-filPageEndLSNOldChecksum+4], bufNoChecksumMagic)
	if !isInnoDBPageChecksumValid(page2) {
		t.Fatalf("expected page with none checksum to be valid")
	}

	page3 := append([]byte(nil), page...)
	writeUint32BE(page3[filPageSpaceOrChksum:filPageSpaceOrChksum+4], 0)
	writeUint32BE(page3[len(page3)-filPageEndLSNOldChecksum:len(page3)-filPageEndLSNOldChecksum+4], 0)
	crc := calcPageCRC32(page3, false)
	writeUint32BE(page3[filPageSpaceOrChksum:filPageSpaceOrChksum+4], crc)
	writeUint32BE(page3[len(page3)-filPageEndLSNOldChecksum:len(page3)-filPageEndLSNOldChecksum+4], crc)
	if !isInnoDBPageChecksumValid(page3) {
		t.Fatalf("expected page with crc32 checksum to be valid")
	}
}

func TestValidateInnoDBPathFileAndDirectory(t *testing.T) {
	tmp := t.TempDir()
	filePath := filepath.Join(tmp, "t.ibd")

	page := make([]byte, innodbDefaultPageSize)
	copy(page[filPageLSN+4:filPageLSN+8], []byte{9, 9, 9, 9})
	copy(page[len(page)-filPageEndLSNOldChecksum+4:len(page)-filPageEndLSNOldChecksum+8], []byte{9, 9, 9, 9})
	writeUint32BE(page[filPageSpaceOrChksum:filPageSpaceOrChksum+4], bufNoChecksumMagic)
	writeUint32BE(page[len(page)-filPageEndLSNOldChecksum:len(page)-filPageEndLSNOldChecksum+4], bufNoChecksumMagic)

	if err := os.WriteFile(filePath, page, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	sumFile, err := ValidateInnoDBPath(filePath)
	if err != nil {
		t.Fatalf("ValidateInnoDBPath(file) error: %v", err)
	}
	if sumFile.FilesChecked != 1 || sumFile.PagesChecked != 1 {
		t.Fatalf("unexpected file summary: %+v", sumFile)
	}

	sumDir, err := ValidateInnoDBPath(tmp)
	if err != nil {
		t.Fatalf("ValidateInnoDBPath(dir) error: %v", err)
	}
	if sumDir.FilesChecked != 1 || sumDir.PagesChecked != 1 {
		t.Fatalf("unexpected dir summary: %+v", sumDir)
	}
}

func TestDetectInnoDBPageSize(t *testing.T) {
	page := make([]byte, innodbDefaultPageSize)
	offset := filPageData + fspSpaceFlagsOffset
	flags := uint32(0)
	flags |= 3 << fspFlagsPosPageSsize
	writeUint32BE(page[offset:offset+4], flags)

	if got := detectInnoDBPageSize(page); got != 4096 {
		t.Fatalf("unexpected page size: got=%d want=4096", got)
	}
}

func writeUint32BE(data []byte, v uint32) {
	if len(data) < 4 {
		return
	}
	data[0] = byte(v >> 24)
	data[1] = byte(v >> 16)
	data[2] = byte(v >> 8)
	data[3] = byte(v)
}
