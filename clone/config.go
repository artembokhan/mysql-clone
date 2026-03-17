package clone

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"
)

const (
	// Version is the mysql-clone tool/library version.
	Version = "0.1.0"

	ModeInnoDB = "innodb"
	ModeBinary = "binary"

	DefaultProgressInterval = time.Duration(0)
)

// Config configures clone network dump and optional restore.
type Config struct {
	Addr                  string
	User                  string
	Password              string
	OutDir                string
	Mode                  string
	ConnectTimeout        time.Duration
	ReadTimeout           time.Duration
	WriteTimeout          time.Duration
	Compress              bool
	TLS                   bool
	TLSInsecureSkipVerify bool
	DDLTimeoutSec         uint32
	BackupLock            bool
	DryRun                bool
	VerifyChecksums       bool
	Concurrency           int
	ProgressInterval      time.Duration
	Debug                 bool
	DebugPackets          bool
}

func DefaultConfig() Config {
	return Config{
		Addr:             "127.0.0.1",
		OutDir:           "",
		Mode:             ModeInnoDB,
		DDLTimeoutSec:    300,
		BackupLock:       true,
		VerifyChecksums:  true,
		Concurrency:      1,
		ProgressInterval: DefaultProgressInterval,
		ConnectTimeout:   10 * time.Second,
		ReadTimeout:      20 * time.Second,
		WriteTimeout:     30 * time.Second,
	}
}

func (cfg Config) Validate() error {
	if cfg.User == "" {
		return errors.New("user is required")
	}
	if !cfg.DryRun && cfg.OutDir == "" {
		return errors.New("out directory is required")
	}
	if cfg.Mode != ModeInnoDB && cfg.Mode != ModeBinary {
		return fmt.Errorf("invalid mode: %s", cfg.Mode)
	}
	if cfg.ConnectTimeout <= 0 {
		return fmt.Errorf("connect timeout must be positive: %s", cfg.ConnectTimeout)
	}
	if cfg.ReadTimeout <= 0 {
		return fmt.Errorf("read timeout must be positive: %s", cfg.ReadTimeout)
	}
	if cfg.WriteTimeout <= 0 {
		return fmt.Errorf("write timeout must be positive: %s", cfg.WriteTimeout)
	}
	if cfg.Concurrency <= 0 {
		return fmt.Errorf("concurrency must be positive: %d", cfg.Concurrency)
	}
	if cfg.Mode == ModeBinary && cfg.Concurrency > 1 {
		return errors.New("concurrency > 1 is not supported in binary mode")
	}
	if cfg.ProgressInterval < 0 {
		return fmt.Errorf("progress interval must be >= 0: %s", cfg.ProgressInterval)
	}
	if !cfg.BackupLock {
		return errors.New("backup lock must be enabled (native clone behavior)")
	}
	return nil
}

func (cfg Config) innoDBOutputDir() string {
	return filepath.Join(cfg.OutDir, "innodb")
}

func (cfg Config) InnoDBOutputDir() string {
	return cfg.innoDBOutputDir()
}
