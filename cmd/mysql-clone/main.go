package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"clone-dump/clone"
)

func parseConfig() (clone.Config, bool, error) {
	cfg := clone.DefaultConfig()

	var connectTimeout string
	var readTimeout string
	var writeTimeout string
	var progressInterval string
	var ddlTimeoutSec uint
	var showVersion bool

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.StringVar(&cfg.Addr, "addr", cfg.Addr, "MySQL address in host[:port] or unix socket format (default port: 3306)")
	flag.StringVar(&cfg.User, "user", "", "MySQL user (required)")
	flag.StringVar(&cfg.Password, "password", "", "MySQL password")
	flag.StringVar(&cfg.OutDir, "out", cfg.OutDir, "Output directory (required unless -dry-run)")
	flag.StringVar(&cfg.Mode, "mode", cfg.Mode, "Output mode: innodb, binary")
	flag.UintVar(&ddlTimeoutSec, "ddl-timeout-sec", uint(cfg.DDLTimeoutSec), "DDL timeout in seconds passed to COM_INIT")
	flag.BoolVar(&cfg.DryRun, "dry-run", cfg.DryRun, "Run protocol flow without writing files/directories")
	flag.BoolVar(&cfg.VerifyChecksums, "verify-checksums", cfg.VerifyChecksums, "Verify InnoDB page checksums after clone (slow for large datasets)")
	flag.IntVar(&cfg.Concurrency, "concurrency", cfg.Concurrency, "Number of parallel clone connections (innodb mode only)")
	flag.BoolVar(&cfg.Compress, "compress", cfg.Compress, "Enable protocol compression (default: false)")
	flag.BoolVar(&cfg.TLS, "tls", cfg.TLS, "Enable TLS (default: false)")
	flag.BoolVar(&cfg.TLSInsecureSkipVerify, "tls-insecure-skip-verify", cfg.TLSInsecureSkipVerify, "Skip TLS certificate verification (default: false)")
	flag.BoolVar(&cfg.Debug, "debug", cfg.Debug, "Enable debug logs (default: false)")
	flag.BoolVar(&cfg.DebugPackets, "debug-packets", cfg.DebugPackets, "Enable per-packet debug logs (default: false)")
	flag.StringVar(&connectTimeout, "connect-timeout", cfg.ConnectTimeout.String(), "Connection timeout")
	flag.StringVar(&readTimeout, "read-timeout", cfg.ReadTimeout.String(), "Socket read timeout")
	flag.StringVar(&writeTimeout, "write-timeout", cfg.WriteTimeout.String(), "Socket write timeout")
	flag.StringVar(&progressInterval, "progress-interval", cfg.ProgressInterval.String(), "Progress print interval during COM_EXECUTE (0 disables)")
	flag.BoolVar(&showVersion, "version", false, "Print tool version")

	flag.Parse()
	if showVersion {
		return cfg, true, nil
	}
	if len(os.Args) == 1 {
		flag.Usage()
		return cfg, false, flag.ErrHelp
	}

	ct, err := time.ParseDuration(connectTimeout)
	if err != nil {
		return cfg, false, fmt.Errorf("invalid -connect-timeout: %w", err)
	}
	rt, err := time.ParseDuration(readTimeout)
	if err != nil {
		return cfg, false, fmt.Errorf("invalid -read-timeout: %w", err)
	}
	wt, err := time.ParseDuration(writeTimeout)
	if err != nil {
		return cfg, false, fmt.Errorf("invalid -write-timeout: %w", err)
	}
	pt, err := time.ParseDuration(progressInterval)
	if err != nil {
		return cfg, false, fmt.Errorf("invalid -progress-interval: %w", err)
	}

	cfg.ConnectTimeout = ct
	cfg.ReadTimeout = rt
	cfg.WriteTimeout = wt
	cfg.ProgressInterval = pt
	cfg.DDLTimeoutSec = uint32(ddlTimeoutSec)
	cfg.Mode = strings.ToLower(strings.TrimSpace(cfg.Mode))

	if err := cfg.Validate(); err != nil {
		return cfg, false, fmt.Errorf("invalid config: %w", err)
	}

	return cfg, false, nil
}

func main() {
	cfg, showVersion, err := parseConfig()
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(2)
	}
	if showVersion {
		fmt.Println(clone.Version)
		os.Exit(0)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dumper, err := clone.NewDumper(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create dumper failed: %v\n", err)
		os.Exit(2)
	}

	if err := dumper.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "clone dump failed: %v\n", err)
		os.Exit(1)
	}

	manifest := dumper.Manifest()
	fmt.Printf("clone dump complete\n")
	if cfg.OutDir != "" {
		fmt.Printf("output: %s\n", cfg.OutDir)
	}
	fmt.Printf("mode: %s\n", cfg.Mode)
	fmt.Printf("dry-run: %v\n", cfg.DryRun)
	fmt.Printf("data bytes: %s\n", clone.HumanBytes(manifest.Stats.DataBytes))
	fmt.Printf("packets: commands=%d responses=%d data=%d descriptors=%d\n",
		manifest.Stats.CommandPackets,
		manifest.Stats.ResponsePackets,
		manifest.Stats.DataPackets,
		manifest.Stats.DescriptorPackets,
	)
	if cfg.Mode == clone.ModeInnoDB {
		fmt.Printf("restore stats: files=%d writes=%d bytes=%s\n",
			manifest.Stats.RestoredFiles,
			manifest.Stats.RestoredWrites,
			clone.HumanBytes(manifest.Stats.RestoredDataBytes),
		)
		if manifest.Stats.ChecksumFiles > 0 || manifest.Stats.ChecksumPages > 0 {
			fmt.Printf("checksum stats: files=%d pages=%d\n",
				manifest.Stats.ChecksumFiles,
				manifest.Stats.ChecksumPages,
			)
		}
		if !cfg.DryRun {
			fmt.Printf("innodb dir: %s\n", cfg.InnoDBOutputDir())
		}
	}
	if cfg.Mode == clone.ModeBinary && !cfg.DryRun {
		fmt.Printf("binary files: %s/stream.bin, %s/data.bin\n", cfg.OutDir, cfg.OutDir)
	}
	if len(manifest.Warnings) > 0 {
		fmt.Printf("warnings: %d (see manifest.json)\n", len(manifest.Warnings))
	}
}
