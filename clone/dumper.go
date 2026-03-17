package clone

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
)

type Dumper struct {
	cfg      Config
	connMain *client.Conn
	connAux  *client.Conn
	writer   *dumpWriter
	restorer restoreTarget

	locators []locator

	manifest Manifest

	respCodeCount  map[byte]uint64
	cleanupFiles   map[string]struct{}
	cleanupDirs    map[string]struct{}
	mu             sync.Mutex
	interruptOnce  sync.Once
	lastProgressAt int64

	lastStage     string
	lastRespCode  byte
	lastRespAt    time.Time
	lastDescType  uint32
	pendingExpect bool
	pendingLoc    int
	pendingFile   int
	pendingOffset uint64
	pendingLen    uint32
}

type restoreTarget interface {
	registerFile(meta cloneFileDescriptor) error
	applyData(desc cloneDataDescriptor, payload []byte) (uint64, error)
	close() error
	fileCount() int
}

func joinErr(base error, add error) error {
	if add == nil {
		return base
	}
	if base == nil {
		return add
	}
	return errors.Join(base, add)
}

func NewDumper(cfg Config) (*Dumper, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	startedAt := time.Now().UTC()
	return &Dumper{
		cfg: cfg,
		manifest: Manifest{
			ToolVersion: Version,
			StartedAt:   startedAt.Format(time.RFC3339Nano),
			Source: SourceManifest{
				Addr: cfg.Addr,
				User: cfg.User,
			},
			Options: OptionsManifest{
				Mode:          cfg.Mode,
				Compress:      cfg.Compress,
				TLS:           cfg.TLS,
				DDLTimeoutSec: cfg.DDLTimeoutSec,
				BackupLock:    cfg.BackupLock,
				DryRun:        cfg.DryRun,
				ProgressEvery: cfg.ProgressInterval.String(),
			},
			Protocol: ProtocolManifest{
				Requested: cloneProtocolVersionV3,
			},
			Stats: StatsManifest{
				DataByLocator: make(map[byte]uint64),
			},
			Configs:   make(map[string]string),
			ConfigsV3: make(map[string]string),
		},
		respCodeCount: make(map[byte]uint64),
		cleanupFiles:  make(map[string]struct{}),
		cleanupDirs:   make(map[string]struct{}),
		pendingLoc:    -1,
		pendingFile:   -1,
	}, nil
}

func (d *Dumper) noteResponse(stage string, code byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if stage != "" {
		d.lastStage = stage
	}
	d.lastRespCode = code
	d.lastRespAt = time.Now()
}

func (d *Dumper) noteDescriptor(descType uint32) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastDescType = descType
}

func (d *Dumper) setPending(expect bool, desc *descriptor, dataDesc *cloneDataDescriptor) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pendingExpect = expect
	if desc != nil {
		d.pendingLoc = int(desc.LocIndex)
	} else {
		d.pendingLoc = -1
	}
	if dataDesc != nil {
		d.pendingFile = int(dataDesc.FileIndex)
		d.pendingOffset = dataDesc.FileOffset
		d.pendingLen = dataDesc.DataLen
	} else {
		d.pendingFile = -1
		d.pendingOffset = 0
		d.pendingLen = 0
	}
}

func (d *Dumper) reportInterrupt(reason string) {
	d.interruptOnce.Do(func() {
		d.mu.Lock()
		stage := d.lastStage
		if stage == "" {
			stage = "unknown"
		}
		resp := fmt.Sprintf("%s(%d)", responseName(d.lastRespCode), d.lastRespCode)
		desc := "n/a"
		if d.lastDescType != 0 {
			desc = fmt.Sprintf("%s(%d)", cloneDescriptorTypeName(d.lastDescType), d.lastDescType)
		}
		pending := "none"
		if d.pendingExpect {
			pending = fmt.Sprintf("data loc=%d file=%d off=%d len=%d", d.pendingLoc, d.pendingFile, d.pendingOffset, d.pendingLen)
		}
		at := "unknown"
		if !d.lastRespAt.IsZero() {
			at = d.lastRespAt.UTC().Format(time.RFC3339)
		}
		statsResp := d.manifest.Stats.ResponsePackets
		statsDataPackets := d.manifest.Stats.DataPackets
		statsDataBytes := d.manifest.Stats.DataBytes
		d.mu.Unlock()
		fmt.Fprintf(os.Stderr,
			"interrupted: %s; stage=%s last_resp=%s at=%s last_desc=%s pending=%s stats: responses=%d data_packets=%d data_bytes=%s\n",
			reason,
			stage,
			resp,
			at,
			desc,
			pending,
			statsResp,
			statsDataPackets,
			HumanBytes(statsDataBytes),
		)
	})
}

func (d *Dumper) logProgress() {
	interval := d.cfg.ProgressInterval
	if interval == 0 {
		return
	}

	now := time.Now()
	last := atomic.LoadInt64(&d.lastProgressAt)
	if last != 0 {
		lastTime := time.Unix(0, last)
		if now.Sub(lastTime) < interval {
			return
		}
	}
	if !atomic.CompareAndSwapInt64(&d.lastProgressAt, last, now.UnixNano()) {
		return
	}

	d.mu.Lock()
	respPackets := d.manifest.Stats.ResponsePackets
	dataPackets := d.manifest.Stats.DataPackets
	dataBytes := d.manifest.Stats.DataBytes
	respCodes := make(map[byte]uint64, len(d.respCodeCount))
	for code, count := range d.respCodeCount {
		respCodes[code] = count
	}
	d.mu.Unlock()

	fmt.Fprintf(os.Stderr, "progress packets=%d data_packets=%d data_bytes=%s\n",
		respPackets,
		dataPackets,
		humanBytes(dataBytes),
	)
	d.debugf("progress detail: response_code_counters=%v", respCodes)
}

func (d *Dumper) addWarning(format string, args ...any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.manifest.Warnings = append(d.manifest.Warnings, fmt.Sprintf(format, args...))
}

func (d *Dumper) emitDebug(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stderr, "[DEBUG %s] %s\n", time.Now().UTC().Format(time.RFC3339Nano), msg)
}

func (d *Dumper) debugf(format string, args ...any) {
	if !d.cfg.Debug {
		return
	}
	d.emitDebug(format, args...)
}

func (d *Dumper) debugPacketf(format string, args ...any) {
	if !d.cfg.DebugPackets {
		return
	}
	d.emitDebug(format, args...)
}

func (d *Dumper) Run(ctx context.Context) error {
	var runErr error
	defer func() {
		d.manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		if runErr != nil {
			d.manifest.Error = runErr.Error()
		}
		if d.restorer != nil {
			d.manifest.Stats.RestoredFiles = uint64(d.restorer.fileCount())
		}
		runErr = joinErr(runErr, d.close())
		if runErr == nil {
			runErr = joinErr(runErr, d.verifyOutputChecksums(ctx))
		}
		if runErr == nil {
			runErr = joinErr(runErr, d.writeManifest())
		}
		if runErr != nil {
			runErr = joinErr(runErr, d.cleanupCreatedOutputs())
		}
	}()

	if err := d.setupOutputs(); err != nil {
		runErr = err
		return runErr
	}

	d.debugf("run start addr=%s out=%s mode=%s backup_lock=%v compress=%v tls=%v dry_run=%v",
		d.cfg.Addr, d.cfg.OutDir, d.cfg.Mode, d.cfg.BackupLock, d.cfg.Compress, d.cfg.TLS, d.cfg.DryRun,
	)
	if d.restorer != nil {
		switch d.cfg.Mode {
		case ModeInnoDB:
			d.debugf("restore enabled dir=%s", d.cfg.innoDBOutputDir())
		}
	}

	mainConn, err := d.connect(ctx)
	if err != nil {
		runErr = err
		return runErr
	}
	d.connMain = mainConn
	d.debugf("main connected server_version=%s conn_id=%d", d.connMain.GetServerVersion(), d.connMain.GetConnectionID())

	if err := d.enterCloneMode(d.connMain); err != nil {
		runErr = fmt.Errorf("main connection COM_CLONE failed: %w", err)
		return runErr
	}

	auxConn, err := d.connect(ctx)
	if err != nil {
		runErr = fmt.Errorf("aux connection failed: %w", err)
		return runErr
	}
	d.connAux = auxConn
	d.debugf("aux connected server_version=%s conn_id=%d", d.connAux.GetServerVersion(), d.connAux.GetConnectionID())

	if err := d.enterCloneMode(d.connAux); err != nil {
		runErr = fmt.Errorf("aux connection COM_CLONE failed: %w", err)
		return runErr
	}

	if err := d.commandInit(ctx); err != nil {
		runErr = err
		return runErr
	}

	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	workers := d.cfg.Concurrency
	if workers < 1 {
		workers = 1
	}

	if workers == 1 {
		if err := d.commandExecute(execCtx); err != nil {
			runErr = err
			return runErr
		}
	} else {
		if err := d.sendExecute(d.connMain, "COM_EXECUTE"); err != nil {
			runErr = err
			return runErr
		}

		errCh := make(chan error, workers)
		go func() {
			errCh <- d.readExecute(execCtx, d.connMain, d.connAux, "COM_EXECUTE")
		}()
		for i := 1; i < workers; i++ {
			workerID := i
			go func() {
				errCh <- d.runAttachedWorker(execCtx, workerID)
			}()
		}

		var firstErr error
		for i := 0; i < workers; i++ {
			err := <-errCh
			if err != nil && firstErr == nil {
				firstErr = err
				cancel()
			}
		}
		if firstErr != nil {
			runErr = firstErr
			return runErr
		}
	}

	if d.connAux != nil {
		if err := d.commandExit(context.Background(), d.connAux); err != nil {
			if isBenignDisconnect(err) {
				d.debugf("aux COM_EXIT connection closed: %v", err)
			} else {
				d.addWarning("aux COM_EXIT failed: %v", err)
			}
		}
	}
	if d.connMain != nil {
		if err := d.commandExit(context.Background(), d.connMain); err != nil {
			if isBenignDisconnect(err) {
				d.debugf("main COM_EXIT connection closed: %v", err)
			} else {
				d.addWarning("main COM_EXIT failed: %v", err)
			}
		}
	}

	runErr = nil
	return nil
}

func (d *Dumper) runAttachedWorker(ctx context.Context, id int) error {
	mainConn, err := d.connect(ctx)
	if err != nil {
		return fmt.Errorf("worker %d main connection failed: %w", id, err)
	}
	defer func() {
		_ = mainConn.Close()
	}()

	if err := d.enterCloneMode(mainConn); err != nil {
		return fmt.Errorf("worker %d COM_CLONE failed: %w", id, err)
	}

	auxConn, err := d.connect(ctx)
	if err != nil {
		return fmt.Errorf("worker %d aux connection failed: %w", id, err)
	}
	defer func() {
		_ = auxConn.Close()
	}()

	if err := d.enterCloneMode(auxConn); err != nil {
		return fmt.Errorf("worker %d aux COM_CLONE failed: %w", id, err)
	}

	attachStage := fmt.Sprintf("COM_ATTACH[%d]", id)
	if err := d.commandAttachWithLocators(ctx, mainConn, attachStage); err != nil {
		return fmt.Errorf("worker %d attach failed: %w", id, err)
	}

	execStage := fmt.Sprintf("COM_EXECUTE[%d]", id)
	if err := d.commandExecuteOn(ctx, mainConn, auxConn, execStage); err != nil {
		return fmt.Errorf("worker %d execute failed: %w", id, err)
	}

	if err := d.commandExit(context.Background(), auxConn); err != nil {
		if isBenignDisconnect(err) {
			d.debugf("worker %d aux COM_EXIT connection closed: %v", id, err)
		} else {
			d.addWarning("worker %d aux COM_EXIT failed: %v", id, err)
		}
	}
	if err := d.commandExit(context.Background(), mainConn); err != nil {
		if isBenignDisconnect(err) {
			d.debugf("worker %d main COM_EXIT connection closed: %v", id, err)
		} else {
			d.addWarning("worker %d main COM_EXIT failed: %v", id, err)
		}
	}

	return nil
}

func (d *Dumper) verifyOutputChecksums(ctx context.Context) error {
	if !d.cfg.VerifyChecksums || d.cfg.DryRun || d.cfg.Mode != ModeInnoDB {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	var progress func(InnoDBChecksumProgress)
	if d.cfg.ProgressInterval > 0 {
		progress = func(p InnoDBChecksumProgress) {
			fmt.Fprintf(os.Stderr,
				"checksum progress files=%d/%d file_pages=%d/%d pages=%d elapsed=%s file=%s\n",
				p.FileIndex,
				p.FilesTotal,
				p.FilePage,
				p.FilePages,
				p.PagesChecked,
				p.Elapsed.Round(time.Second),
				p.File,
			)
		}
	}
	summary, err := ValidateInnoDBPathWithProgress(ctx, d.cfg.innoDBOutputDir(), d.cfg.ProgressInterval, progress)
	if err != nil {
		return fmt.Errorf("innodb checksum validation failed: %w", err)
	}
	d.manifest.Stats.ChecksumFiles = uint64(summary.FilesChecked)
	d.manifest.Stats.ChecksumPages = summary.PagesChecked
	d.debugf("innodb checksum ok files=%d pages=%d", summary.FilesChecked, summary.PagesChecked)
	return nil
}

func (d *Dumper) setupOutputs() error {
	if d.cfg.DryRun {
		return nil
	}

	createdOutDir, err := ensureDirExists(d.cfg.OutDir)
	if err != nil {
		return fmt.Errorf("prepare output directory: %w", err)
	}
	if createdOutDir {
		d.trackDir(d.cfg.OutDir)
	}

	switch d.cfg.Mode {
	case ModeBinary:
		dw, err := newDumpWriter(d.cfg.OutDir)
		if err != nil {
			return err
		}
		d.writer = dw
		d.trackFile(filepath.Join(d.cfg.OutDir, "stream.bin"))
		d.trackFile(filepath.Join(d.cfg.OutDir, "data.bin"))
		return nil

	case ModeInnoDB:
		restoreDir := d.cfg.innoDBOutputDir()
		if err := ensurePathAbsent(restoreDir, "restore directory"); err != nil {
			return err
		}
		restorer, err := newCloneRestorer(restoreDir, d.debugf, d.addWarning)
		if err != nil {
			return err
		}
		d.restorer = restorer
		d.trackDir(restoreDir)
		return nil
	}

	return fmt.Errorf("unsupported mode: %s", d.cfg.Mode)
}

func (d *Dumper) writeManifest() error {
	if d.cfg.DryRun {
		return nil
	}

	path := filepath.Join(d.cfg.OutDir, "manifest.json")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o666)
	if err != nil {
		return fmt.Errorf("create manifest: %w", err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(d.manifest); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	d.trackFile(path)
	return nil
}

func (d *Dumper) close() error {
	var errs []error
	if d.connMain != nil {
		if err := d.connMain.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if d.connAux != nil {
		if err := d.connAux.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if d.writer != nil {
		if err := d.writer.close(); err != nil {
			errs = append(errs, err)
		}
	}
	if d.restorer != nil {
		if err := d.restorer.close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

func (d *Dumper) Manifest() Manifest {
	return d.manifest
}

func (d *Dumper) trackFile(path string) {
	d.cleanupFiles[path] = struct{}{}
}

func (d *Dumper) trackDir(path string) {
	d.cleanupDirs[path] = struct{}{}
}

func (d *Dumper) cleanupCreatedOutputs() error {
	var errs []error

	for filePath := range d.cleanupFiles {
		if err := os.Remove(filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, fmt.Errorf("remove created file %q: %w", filePath, err))
		}
	}

	dirs := make([]string, 0, len(d.cleanupDirs))
	for dirPath := range d.cleanupDirs {
		dirs = append(dirs, dirPath)
	}
	sort.Slice(dirs, func(i, j int) bool {
		return len(dirs[i]) > len(dirs[j])
	})

	for _, dirPath := range dirs {
		if err := os.RemoveAll(dirPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, fmt.Errorf("remove created directory %q: %w", dirPath, err))
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

func ensureDirExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err == nil {
		if !info.IsDir() {
			return false, fmt.Errorf("path exists and is not a directory: %s", path)
		}
		return false, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}

	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return false, err
	}
	return true, nil
}

func ensurePathAbsent(path string, label string) error {
	_, err := os.Stat(path)
	if err == nil {
		return fmt.Errorf("%s already exists: %s", label, path)
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return fmt.Errorf("stat %s: %w", label, err)
}
