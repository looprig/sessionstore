package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/looprig/sessionstore/internal/modfiles"
)

func main() {
	root := flag.String("root", ".", "module root to enumerate")
	flag.Parse()

	files, err := modfiles.Files(*root)
	if err == nil {
		err = modfiles.WriteNull(os.Stdout, files)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "modfiles: %v\n", err)
		os.Exit(1)
	}
}
