package clone

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

type restoreFile struct {
	fileIndex  uint32
	pathRel    string
	pathAbs    string
	targetSize uint64
	deleted    bool
	fh         *os.File
}

type cloneRestorer struct {
	rootDir  string
	files    map[uint64]*restoreFile
	seenPath map[string]struct{}
	debugf   func(string, ...any)
	warnf    func(string, ...any)
	mu       sync.Mutex
}

func newCloneRestorer(rootDir string, debugf func(string, ...any), warnf func(string, ...any)) (*cloneRestorer, error) {
	if err := os.MkdirAll(rootDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("create restore directory: %w", err)
	}

	return &cloneRestorer{
		rootDir:  rootDir,
		files:    make(map[uint64]*restoreFile),
		seenPath: make(map[string]struct{}),
		debugf:   debugf,
		warnf:    warnf,
	}, nil
}

func (r *cloneRestorer) fileCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seenPath)
}

func (r *cloneRestorer) debug(format string, args ...any) {
	if r.debugf == nil {
		return
	}
	r.debugf(format, args...)
}

func (r *cloneRestorer) warn(format string, args ...any) {
	if r.warnf == nil {
		return
	}
	r.warnf(format, args...)
}

func (r *cloneRestorer) registerFile(meta cloneFileDescriptor) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := fileKey(meta.State, meta.FileIndex)
	entry, ok := r.files[key]
	if !ok {
		entry = &restoreFile{fileIndex: meta.FileIndex}
		r.files[key] = entry
	}

	pathName := meta.FileName
	if pathName == "" && meta.State == cloneSnapshotRedoCopy {
		pathName = fmt.Sprintf("ib_logfile%d", meta.FileIndex)
	}
	if pathName != "" {
		relPath, err := sanitizeCloneFilePath(pathName)
		if err != nil {
			return fmt.Errorf("sanitize file path for index %d: %w", meta.FileIndex, err)
		}
		absPath := filepath.Join(r.rootDir, relPath)
		if entry.pathAbs != "" && entry.pathAbs != absPath && entry.fh != nil {
			if err := entry.fh.Close(); err != nil {
				return fmt.Errorf("close old file handle %q: %w", entry.pathAbs, err)
			}
			entry.fh = nil
		}
		entry.pathRel = relPath
		entry.pathAbs = absPath
	}

	entry.deleted = cloneFlagSet(meta.Flags, cloneFileFlagDeleted)
	if meta.FileSize > entry.targetSize {
		entry.targetSize = meta.FileSize
	}

	if entry.deleted {
		r.debug("restore register deleted file index=%d path=%q", entry.fileIndex, entry.pathRel)
		if entry.fh != nil {
			if err := entry.fh.Close(); err != nil {
				return fmt.Errorf("close deleted file handle %q: %w", entry.pathAbs, err)
			}
			entry.fh = nil
		}
		return nil
	}

	if entry.pathAbs == "" {
		entry.pathRel = filepath.Join("__unnamed", fmt.Sprintf("file_%08d.bin", entry.fileIndex))
		entry.pathAbs = filepath.Join(r.rootDir, entry.pathRel)
		r.warn("clone file index %d has empty file name, using %q", entry.fileIndex, entry.pathRel)
	}

	if err := r.ensureOpen(entry); err != nil {
		return err
	}

	if meta.State == cloneSnapshotRedoCopy && meta.FileIndex == 0 {
		if err := r.ensureSecondRedoFile(meta.FileSize); err != nil {
			return err
		}
	}

	r.debug("restore file metadata index=%d path=%q size=%d flags=0x%x",
		meta.FileIndex, entry.pathRel, meta.FileSize, meta.Flags)
	return nil
}

func (r *cloneRestorer) applyData(desc cloneDataDescriptor, payload []byte) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := fileKey(desc.State, desc.FileIndex)
	entry, ok := r.files[key]
	if !ok {
		redoState := desc.State == cloneSnapshotRedoCopy
		pathRel := filepath.Join("__unknown", fmt.Sprintf("file_%08d.bin", desc.FileIndex))
		if redoState {
			pathRel = fmt.Sprintf("ib_logfile%d", desc.FileIndex)
		}
		entry = &restoreFile{
			fileIndex: desc.FileIndex,
			pathRel:   pathRel,
		}
		entry.pathAbs = filepath.Join(r.rootDir, entry.pathRel)
		r.files[key] = entry
		r.warn("clone data for file index %d arrived before file metadata, using %q", desc.FileIndex, entry.pathRel)
	}

	if entry.deleted {
		r.warn("skip data for deleted file index %d path=%q", desc.FileIndex, entry.pathRel)
		return 0, nil
	}

	if err := r.ensureOpen(entry); err != nil {
		return 0, err
	}

	expectedEnd := desc.FileOffset + uint64(len(payload))
	if expectedEnd > entry.targetSize {
		entry.targetSize = expectedEnd
	}
	if desc.FileSize > entry.targetSize {
		entry.targetSize = desc.FileSize
	}

	n, err := entry.fh.WriteAt(payload, int64(desc.FileOffset))
	if err != nil {
		return 0, fmt.Errorf("write data index=%d path=%q offset=%d len=%d: %w",
			desc.FileIndex, entry.pathRel, desc.FileOffset, len(payload), err)
	}
	if n != len(payload) {
		return 0, fmt.Errorf("short write index=%d path=%q offset=%d written=%d want=%d",
			desc.FileIndex, entry.pathRel, desc.FileOffset, n, len(payload))
	}

	return uint64(n), nil
}

func (r *cloneRestorer) ensureOpen(entry *restoreFile) error {
	if entry.pathAbs == "" {
		return fmt.Errorf("empty restore file path for index %d", entry.fileIndex)
	}

	if err := os.MkdirAll(filepath.Dir(entry.pathAbs), os.ModePerm); err != nil {
		return fmt.Errorf("create parent directory for %q: %w", entry.pathAbs, err)
	}
	if entry.fh != nil {
		return nil
	}

	fh, err := os.OpenFile(entry.pathAbs, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return fmt.Errorf("open restore file %q: %w", entry.pathAbs, err)
	}
	entry.fh = fh
	r.seenPath[entry.pathRel] = struct{}{}
	return nil
}

func (r *cloneRestorer) close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	for _, entry := range r.files {
		if entry.deleted && entry.pathAbs != "" {
			if entry.fh != nil {
				if err := entry.fh.Close(); err != nil {
					errs = append(errs, fmt.Errorf("close deleted file %q: %w", entry.pathAbs, err))
				}
				entry.fh = nil
			}
			if err := os.Remove(entry.pathAbs); err != nil && !errors.Is(err, os.ErrNotExist) {
				errs = append(errs, fmt.Errorf("remove deleted file %q: %w", entry.pathAbs, err))
			}
			continue
		}

		if entry.pathAbs == "" {
			continue
		}
		if err := r.ensureOpen(entry); err != nil {
			errs = append(errs, err)
			continue
		}

		if err := entry.fh.Truncate(int64(entry.targetSize)); err != nil {
			errs = append(errs, fmt.Errorf("truncate file %q to %d bytes: %w",
				entry.pathAbs, entry.targetSize, err))
		}
		if err := entry.fh.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close file %q: %w", entry.pathAbs, err))
		}
		entry.fh = nil
	}

	if len(errs) == 0 {
		for _, schemaDir := range []string{"mysql", "performance_schema"} {
			if err := os.MkdirAll(filepath.Join(r.rootDir, schemaDir), os.ModePerm); err != nil {
				errs = append(errs, fmt.Errorf("create schema directory %q: %w", schemaDir, err))
			}
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

func (r *cloneRestorer) ensureSecondRedoFile(targetSize uint64) error {
	key := fileKey(cloneSnapshotRedoCopy, 1)
	entry, ok := r.files[key]
	if !ok {
		entry = &restoreFile{fileIndex: 1}
		r.files[key] = entry
	}

	if entry.pathAbs == "" {
		redoName := fmt.Sprintf("ib_logfile%d", entry.fileIndex)
		relPath, err := sanitizeCloneFilePath(redoName)
		if err != nil {
			return fmt.Errorf("sanitize redo file path for index %d: %w", entry.fileIndex, err)
		}
		entry.pathRel = relPath
		entry.pathAbs = filepath.Join(r.rootDir, relPath)
	}

	if targetSize > entry.targetSize {
		entry.targetSize = targetSize
	}

	if err := r.ensureOpen(entry); err != nil {
		return err
	}
	return nil
}

func cloneFlagSet(flags uint16, bit uint16) bool {
	if bit == 0 || bit > 16 {
		return false
	}
	mask := uint16(1 << (bit - 1))
	return flags&mask != 0
}

func fileKey(state uint32, index uint32) uint64 {
	if state == cloneSnapshotRedoCopy {
		return (uint64(1) << 32) | uint64(index)
	}
	return uint64(index)
}

func sanitizeCloneFilePath(fileName string) (string, error) {
	normalized := strings.TrimSpace(fileName)
	if normalized == "" {
		return "", errors.New("empty normalized path")
	}

	normalized = strings.ReplaceAll(normalized, "\\", "/")
	normalized = strings.TrimPrefix(normalized, "./")
	normalized = path.Clean(normalized)

	if normalized == "." || normalized == "" {
		return "", errors.New("empty normalized path")
	}
	if path.IsAbs(normalized) {
		return "", fmt.Errorf("absolute path is not allowed: %q", fileName)
	}
	if strings.Contains(normalized, ":") {
		return "", fmt.Errorf("path with volume/drive marker is not allowed: %q", fileName)
	}
	if normalized == ".." || strings.HasPrefix(normalized, "../") {
		return "", fmt.Errorf("path escapes restore root: %q", fileName)
	}
	return filepath.FromSlash(normalized), nil
}
