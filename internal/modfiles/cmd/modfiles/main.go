package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/looprig/sessionstore/internal/modfiles"
)

func main() {
	root := flag.String("root", ".", "module root to enumerate")
	flag.Parse()

	err := run(*root, os.Stdout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "modfiles: %v\n", err)
		os.Exit(1)
	}
}

func run(root string, output io.Writer) error {
	files, err := modfiles.Files(root)
	if err != nil {
		return err
	}
	return modfiles.WriteNull(output, files)
}
