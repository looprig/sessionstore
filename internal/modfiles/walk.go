// Package modfiles enumerates Go source files owned by this module while excluding
// structural directories and nested repository or module boundaries.
package modfiles

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// SymlinkError reports a module-owned Go source path that is a symbolic link.
// Discovery fails closed rather than following a link that may escape the module.
type SymlinkError struct {
	Path string
}

func (e *SymlinkError) Error() string {
	return "modfiles: module-owned Go file is a symbolic link: " + e.Path
}

// Files returns absolute paths to every module-owned Go source file below root.
// Build constraints are deliberately ignored so tagged production files remain
// visible to dependency and formatting checks.
func Files(root string) ([]string, error) {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}

	var files []string
	err = filepath.WalkDir(absoluteRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path != absoluteRoot && entry.IsDir() {
			if ignoredDirectory(entry.Name()) {
				return filepath.SkipDir
			}
			nested, err := nestedBoundary(path)
			if err != nil {
				return err
			}
			if nested {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() || ignoredFile(entry.Name()) || !strings.HasSuffix(entry.Name(), ".go") {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return &SymlinkError{Path: path}
		}
		if info.Mode().IsRegular() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.Sort(files)
	return files, nil
}

// WriteNull writes paths separated and terminated by NUL bytes for safe piping to
// tools such as xargs -0, including when a filename contains whitespace or newlines.
func WriteNull(w io.Writer, paths []string) error {
	for _, path := range paths {
		if _, err := io.WriteString(w, path); err != nil {
			return err
		}
		if _, err := io.WriteString(w, "\x00"); err != nil {
			return err
		}
	}
	return nil
}

func ignoredDirectory(name string) bool {
	return name == "vendor" || name == "testdata" || goIgnoredName(name)
}

func ignoredFile(name string) bool {
	return goIgnoredName(name)
}

func goIgnoredName(name string) bool {
	return name != "" && (name[0] == '.' || name[0] == '_')
}

func nestedBoundary(path string) (bool, error) {
	for _, marker := range []string{"go.mod", ".git"} {
		_, err := os.Lstat(filepath.Join(path, marker))
		switch {
		case err == nil:
			return true, nil
		case os.IsNotExist(err):
			continue
		default:
			return false, err
		}
	}
	return false, nil
}
