package modfiles

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestFilesReturnsOnlyModuleOwnedGoFiles(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFixture(t, root, "go.mod", "module example.com/root\n")
	writeFixture(t, root, "root.go", "package root\n")
	writeFixture(t, root, "nested/real.go", "package nested\n")
	writeFixture(t, root, "nested/tagged.go", "//go:build customtag\n\npackage nested\n")
	writeFixture(t, root, "nested/file with\nnewline.go", "package nested\n")

	for _, path := range []string{
		".worktrees/branch/ignored.go",
		".hidden/ignored.go",
		"_hidden/ignored.go",
		"testdata/ignored.go",
		"nested/testdata/ignored.go",
		"vendor/ignored.go",
		"_ignored.go",
		".ignored.go",
	} {
		writeFixture(t, root, path, "package ignored\n")
	}
	writeFixture(t, root, "nested-module/go.mod", "module example.com/nested\n")
	writeFixture(t, root, "nested-module/ignored.go", "package ignored\n")
	writeFixture(t, root, "nested-repository/.git/HEAD", "ref: refs/heads/main\n")
	writeFixture(t, root, "nested-repository/ignored.go", "package ignored\n")
	writeFixture(t, root, "nested-repository-file/.git", "gitdir: elsewhere\n")
	writeFixture(t, root, "nested-repository-file/ignored.go", "package ignored\n")

	got, err := Files(root)
	if err != nil {
		t.Fatalf("Files(%q): %v", root, err)
	}
	want := []string{
		filepath.Join(root, "nested", "file with\nnewline.go"),
		filepath.Join(root, "nested", "real.go"),
		filepath.Join(root, "nested", "tagged.go"),
		filepath.Join(root, "root.go"),
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("Files() = %q, want %q", got, want)
	}
}

func TestWriteNullPreservesUnusualFilenames(t *testing.T) {
	t.Parallel()

	files := []string{"/tmp/space name.go", "/tmp/new\nline.go", "/tmp/-leading.go"}
	var got bytes.Buffer
	if err := WriteNull(&got, files); err != nil {
		t.Fatalf("WriteNull: %v", err)
	}
	want := strings.Join(files, "\x00") + "\x00"
	if got.String() != want {
		t.Fatalf("WriteNull() = %q, want %q", got.String(), want)
	}
}

func TestFilesRejectsModuleOwnedGoSymlink(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	targetRoot := t.TempDir()
	writeFixture(t, root, "go.mod", "module example.com/root\n")
	target := filepath.Join(targetRoot, "target.go")
	writeFixture(t, targetRoot, "target.go", "package target\n")
	link := filepath.Join(root, "linked.go")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("create Go file symlink: %v", err)
	}

	_, err := Files(root)
	var symlinkErr *SymlinkError
	if !errors.As(err, &symlinkErr) {
		t.Fatalf("Files() error = %v, want *SymlinkError", err)
	}
	if symlinkErr.Path != link {
		t.Fatalf("SymlinkError.Path = %q, want %q", symlinkErr.Path, link)
	}
}

func TestFilesSkipsDirectoryAndIgnoredBoundarySymlinks(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	targetRoot := t.TempDir()
	writeFixture(t, root, "go.mod", "module example.com/root\n")
	writeFixture(t, root, "root.go", "package root\n")
	writeFixture(t, targetRoot, "target.go", "package target\n")
	if err := os.Symlink(targetRoot, filepath.Join(root, "linked-directory")); err != nil {
		t.Fatalf("create directory symlink: %v", err)
	}

	for _, boundary := range []string{".worktrees/branch", "testdata", "nested-module", "nested-repository"} {
		if boundary == "nested-module" {
			writeFixture(t, root, filepath.Join(boundary, "go.mod"), "module example.com/nested\n")
		} else if boundary == "nested-repository" {
			writeFixture(t, root, filepath.Join(boundary, ".git", "HEAD"), "ref: refs/heads/main\n")
		} else if err := os.MkdirAll(filepath.Join(root, boundary), 0o755); err != nil {
			t.Fatalf("create ignored boundary: %v", err)
		}
		if err := os.Symlink(filepath.Join(targetRoot, "target.go"), filepath.Join(root, boundary, "linked.go")); err != nil {
			t.Fatalf("create ignored Go symlink in %s: %v", boundary, err)
		}
	}

	got, err := Files(root)
	if err != nil {
		t.Fatalf("Files(): %v", err)
	}
	want := []string{filepath.Join(root, "root.go")}
	if !slices.Equal(got, want) {
		t.Fatalf("Files() = %q, want %q", got, want)
	}
}

func writeFixture(t *testing.T, root, relative, content string) {
	t.Helper()
	path := filepath.Join(root, relative)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create fixture directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write fixture %q: %v", relative, err)
	}
}
