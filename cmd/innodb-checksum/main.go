package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"clone-dump/clone"
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s <path-to-innodb-file-or-dir>\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	var showVersion bool
	flag.BoolVar(&showVersion, "version", false, "Print tool version")
	flag.Usage = usage
	flag.Parse()

	if showVersion {
		fmt.Println(clone.Version)
		return
	}

	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}

	path := flag.Arg(0)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	summary, err := clone.ValidateInnoDBPathWithContext(ctx, path)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Fprintln(os.Stderr, "innodb checksum interrupted")
			os.Exit(130)
		}
		fmt.Fprintf(os.Stderr, "innodb checksum failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("ok files=%d pages=%d\n", summary.FilesChecked, summary.PagesChecked)
}
