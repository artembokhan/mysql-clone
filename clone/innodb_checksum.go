package clone

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	filPageSpaceOrChksum     = 0
	filPageOffset            = 4
	filPageLSN               = 16
	filPageFileFlushLSN      = 26
	filPageArchLogNoOrSpace  = 34
	filPageData              = 38
	filPageEndLSNOldChecksum = 8

	fspSpaceFlagsOffset = 16

	fspFlagsPosPageSsize  = 6
	fspFlagsMaskPageSsize = uint32(0x3C0)

	innodbDefaultPageSize = 16 * 1024
	innodbMinPageSize     = 4 * 1024
	innodbMaxPageSize     = 64 * 1024
	univZipSizeMin        = 1 << 10

	bufNoChecksumMagic = uint32(0xDEADBEEF)
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

type InnoDBChecksumSummary struct {
	FilesChecked int
	PagesChecked uint64
}

type InnoDBChecksumProgress struct {
	FilesTotal   int
	FileIndex    int
	File         string
	FilePages    uint64
	FilePage     uint64
	PagesChecked uint64
	Elapsed      time.Duration
}

type checksumProgressReporter struct {
	interval     time.Duration
	progress     func(InnoDBChecksumProgress)
	start        time.Time
	lastReported time.Time
	filesTotal   int
	fileIndex    int
	file         string
	filePages    uint64
	filePage     uint64
	pagesChecked uint64
}

func newChecksumProgressReporter(interval time.Duration, filesTotal int, progress func(InnoDBChecksumProgress)) *checksumProgressReporter {
	if progress == nil || interval <= 0 {
		return nil
	}
	return &checksumProgressReporter{
		interval:   interval,
		progress:   progress,
		start:      time.Now(),
		filesTotal: filesTotal,
	}
}

func (r *checksumProgressReporter) startFile(index int, name string, pages uint64) {
	if r == nil {
		return
	}
	r.fileIndex = index
	r.file = name
	r.filePages = pages
	r.filePage = 0
	r.report(true)
}

func (r *checksumProgressReporter) pageDone() {
	if r == nil {
		return
	}
	r.filePage++
	r.pagesChecked++
	r.report(false)
}

func (r *checksumProgressReporter) finishFile() {
	if r == nil {
		return
	}
	r.filePage = r.filePages
	r.report(true)
}

func (r *checksumProgressReporter) report(force bool) {
	if r == nil {
		return
	}
	now := time.Now()
	if !force && now.Sub(r.lastReported) < r.interval {
		return
	}
	r.lastReported = now
	r.progress(InnoDBChecksumProgress{
		FilesTotal:   r.filesTotal,
		FileIndex:    r.fileIndex,
		File:         r.file,
		FilePages:    r.filePages,
		FilePage:     r.filePage,
		PagesChecked: r.pagesChecked,
		Elapsed:      now.Sub(r.start),
	})
}

type pageChecksumError struct {
	File string
	Page uint64
}

func (e pageChecksumError) Error() string {
	return fmt.Sprintf("checksum mismatch file=%q page=%d", e.File, e.Page)
}

func ValidateInnoDBPath(path string) (InnoDBChecksumSummary, error) {
	return ValidateInnoDBPathWithContext(context.Background(), path)
}

func ValidateInnoDBPathWithContext(ctx context.Context, path string) (InnoDBChecksumSummary, error) {
	return ValidateInnoDBPathWithProgress(ctx, path, 0, nil)
}

func ValidateInnoDBPathWithProgress(
	ctx context.Context,
	path string,
	progressInterval time.Duration,
	progress func(InnoDBChecksumProgress),
) (InnoDBChecksumSummary, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	st, err := os.Stat(path)
	if err != nil {
		return InnoDBChecksumSummary{}, err
	}

	var files []string
	if st.Mode().IsRegular() {
		files = []string{path}
	} else if st.IsDir() {
		if err := filepath.WalkDir(path, func(p string, d os.DirEntry, walkErr error) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				return nil
			}
			if d.Type()&os.ModeType != 0 {
				return nil
			}
			if isInnoDBTablespaceFile(d.Name()) {
				files = append(files, p)
			}
			return nil
		}); err != nil {
			return InnoDBChecksumSummary{}, err
		}
	} else {
		return InnoDBChecksumSummary{}, fmt.Errorf("unsupported path type: %s", path)
	}

	if len(files) == 0 {
		return InnoDBChecksumSummary{}, fmt.Errorf("no InnoDB tablespace files found in %q", path)
	}

	reporter := newChecksumProgressReporter(progressInterval, len(files), progress)

	summary := InnoDBChecksumSummary{}
	for i, file := range files {
		if err := ctx.Err(); err != nil {
			return summary, err
		}
		display := file
		if st.IsDir() {
			if rel, err := filepath.Rel(path, file); err == nil {
				display = rel
			}
		} else {
			display = filepath.Base(file)
		}
		pages, err := validateInnoDBFileChecksumsWithContext(ctx, file, reporter, i+1, display)
		if err != nil {
			return summary, err
		}
		summary.FilesChecked++
		summary.PagesChecked += pages
	}

	return summary, nil
}

func isInnoDBTablespaceFile(fileName string) bool {
	name := strings.ToLower(strings.TrimSpace(fileName))
	if strings.HasSuffix(name, ".ibd") {
		return true
	}
	if strings.HasPrefix(name, "ibdata") {
		return true
	}
	if strings.HasPrefix(name, "undo_") {
		return true
	}
	if strings.HasPrefix(name, "innodb_undo_") {
		return true
	}
	return false
}

func validateInnoDBFileChecksums(path string) (uint64, error) {
	return validateInnoDBFileChecksumsWithContext(context.Background(), path, nil, 0, "")
}

func validateInnoDBFileChecksumsWithContext(
	ctx context.Context,
	path string,
	reporter *checksumProgressReporter,
	fileIndex int,
	display string,
) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if st.Size() == 0 {
		return 0, nil
	}

	header := make([]byte, innodbDefaultPageSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return 0, fmt.Errorf("read page 0 for page-size detection: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	pageSize := detectInnoDBPageSize(header)
	if pageSize < innodbMinPageSize || pageSize > innodbMaxPageSize {
		return 0, fmt.Errorf("invalid detected InnoDB page size: %d", pageSize)
	}
	if st.Size()%int64(pageSize) != 0 {
		return 0, fmt.Errorf("file size %d is not multiple of page size %d: %s", st.Size(), pageSize, path)
	}

	pageCount := uint64(st.Size() / int64(pageSize))
	if reporter != nil {
		reporter.startFile(fileIndex, display, pageCount)
	}
	page := make([]byte, pageSize)
	for i := uint64(0); i < pageCount; i++ {
		if err := ctx.Err(); err != nil {
			return i, err
		}
		if _, err := io.ReadFull(f, page); err != nil {
			return i, fmt.Errorf("read page %d: %w", i, err)
		}
		if !isInnoDBPageChecksumValid(page) {
			return i, pageChecksumError{File: path, Page: i}
		}
		if reporter != nil {
			reporter.pageDone()
		}
	}
	if reporter != nil {
		reporter.finishFile()
	}

	return pageCount, nil
}

func detectInnoDBPageSize(firstPage []byte) int {
	const flagsOffset = filPageData + fspSpaceFlagsOffset
	if len(firstPage) < flagsOffset+4 {
		return innodbDefaultPageSize
	}

	flags := readUint32BE(firstPage[flagsOffset : flagsOffset+4])
	ssize := (flags & fspFlagsMaskPageSsize) >> fspFlagsPosPageSsize
	if ssize == 0 {
		return innodbDefaultPageSize
	}

	size := (univZipSizeMin >> 1) << ssize
	if size < innodbMinPageSize || size > innodbMaxPageSize {
		return innodbDefaultPageSize
	}
	if size&(size-1) != 0 {
		return innodbDefaultPageSize
	}

	return int(size)
}

func isInnoDBPageChecksumValid(page []byte) bool {
	if len(page) < innodbMinPageSize {
		return false
	}
	if len(page) < filPageLSN+8 || len(page) < filPageData+1 || len(page) < filPageEndLSNOldChecksum {
		return false
	}

	if !bytesEqual(page[filPageLSN+4:filPageLSN+8], page[len(page)-filPageEndLSNOldChecksum+4:len(page)-filPageEndLSNOldChecksum+8]) {
		return false
	}

	checksumField1 := readUint32BE(page[filPageSpaceOrChksum : filPageSpaceOrChksum+4])
	checksumField2 := readUint32BE(page[len(page)-filPageEndLSNOldChecksum : len(page)-filPageEndLSNOldChecksum+4])

	if checksumField1 == 0 && checksumField2 == 0 && readUint64BE(page[filPageLSN:filPageLSN+8]) == 0 {
		empty := true
		for i, b := range page {
			if (i < filPageFileFlushLSN || i >= filPageArchLogNoOrSpace) && b != 0 {
				empty = false
				break
			}
		}
		if empty {
			return true
		}
	}

	if isChecksumValidInnoDB(page, checksumField1, checksumField2) {
		return true
	}
	if isChecksumValidNone(checksumField1, checksumField2) {
		return true
	}
	if isChecksumValidCRC32(page, checksumField1, checksumField2) {
		return true
	}

	return false
}

func isChecksumValidInnoDB(page []byte, checksumField1 uint32, checksumField2 uint32) bool {
	oldChecksum := calcPageOldChecksum(page)
	newChecksum := calcPageNewChecksum(page)
	lsnLow := readUint32BE(page[filPageLSN : filPageLSN+4])

	if checksumField2 != lsnLow && checksumField2 != oldChecksum {
		return false
	}

	return checksumField1 == 0 || checksumField1 == newChecksum
}

func isChecksumValidNone(checksumField1 uint32, checksumField2 uint32) bool {
	return checksumField1 == checksumField2 && checksumField1 == bufNoChecksumMagic
}

func isChecksumValidCRC32(page []byte, checksumField1 uint32, checksumField2 uint32) bool {
	if checksumField1 != checksumField2 {
		return false
	}

	if checksumField1 == calcPageCRC32(page, false) {
		return true
	}

	return checksumField1 == calcPageCRC32(page, true)
}

func calcPageCRC32(page []byte, legacyBigEndian bool) uint32 {
	left := page[filPageOffset:filPageFileFlushLSN]
	right := page[filPageData : len(page)-filPageEndLSNOldChecksum]

	var c1 uint32
	var c2 uint32
	if legacyBigEndian {
		c1 = crc32LegacyBigEndian(left)
		c2 = crc32LegacyBigEndian(right)
	} else {
		c1 = crc32.Checksum(left, castagnoliTable)
		c2 = crc32.Checksum(right, castagnoliTable)
	}
	return c1 ^ c2
}

func calcPageNewChecksum(page []byte) uint32 {
	left := page[filPageOffset:filPageFileFlushLSN]
	right := page[filPageData : len(page)-filPageEndLSNOldChecksum]
	return hashBinaryIB(left) + hashBinaryIB(right)
}

func calcPageOldChecksum(page []byte) uint32 {
	return hashBinaryIB(page[:filPageFileFlushLSN])
}

func hashBinaryIB(data []byte) uint32 {
	var hashValue uint32
	for _, b := range data {
		hashValue = hashUint32PairIB(hashValue, uint32(b))
	}
	return hashValue
}

func hashUint32PairIB(n1 uint32, n2 uint32) uint32 {
	const hashRandomMask uint32 = 1463735687
	const hashRandomMask2 uint32 = 1653893711
	return ((((n1 ^ n2 ^ hashRandomMask2) << 8) + n1) ^ hashRandomMask) + n2
}

func crc32LegacyBigEndian(data []byte) uint32 {
	h := crc32.New(castagnoliTable)

	i := 0
	for i+8 <= len(data) {
		var chunk [8]byte
		copy(chunk[:], data[i:i+8])
		reverseBytes(chunk[:])
		_, _ = h.Write(chunk[:])
		i += 8
	}
	if i < len(data) {
		_, _ = h.Write(data[i:])
	}

	return h.Sum32()
}

func reverseBytes(data []byte) {
	for i, j := 0, len(data)-1; i < j; i, j = i+1, j-1 {
		data[i], data[j] = data[j], data[i]
	}
}

func readUint32BE(data []byte) uint32 {
	if len(data) < 4 {
		return 0
	}
	return uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3])
}

func readUint64BE(data []byte) uint64 {
	if len(data) < 8 {
		return 0
	}
	return uint64(data[0])<<56 |
		uint64(data[1])<<48 |
		uint64(data[2])<<40 |
		uint64(data[3])<<32 |
		uint64(data[4])<<24 |
		uint64(data[5])<<16 |
		uint64(data[6])<<8 |
		uint64(data[7])
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func isNoSuchFile(err error) bool {
	return errors.Is(err, os.ErrNotExist)
}
