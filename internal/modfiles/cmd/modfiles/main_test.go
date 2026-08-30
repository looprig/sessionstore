package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/looprig/sessionstore/internal/modfiles"
)

func TestRunRejectsGoSymlinkBeforeWritingFileList(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	targetRoot := t.TempDir()
	target := filepath.Join(targetRoot, "target.go")
	if err := os.WriteFile(target, []byte("package target\n"), 0o600); err != nil {
		t.Fatalf("write target: %v", err)
	}
	if err := os.Symlink(target, filepath.Join(root, "linked.go")); err != nil {
		t.Fatalf("create Go file symlink: %v", err)
	}

	err := run(root, io.Discard)
	var symlinkErr *modfiles.SymlinkError
	if !errors.As(err, &symlinkErr) {
		t.Fatalf("run() error = %v, want *modfiles.SymlinkError", err)
	}
}
