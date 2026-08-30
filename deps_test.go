package sessionstore_test

import (
	"errors"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore/internal/modfiles"
	"github.com/looprig/storage"
)

func TestAllowedProductionImport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		importPath string
		want       bool
	}{
		{name: "standard library", importPath: "encoding/json", want: true},
		{name: "Core root", importPath: "github.com/looprig/core", want: true},
		{name: "Core package", importPath: "github.com/looprig/core/sessionwire/v1", want: true},
		{name: "Storage root", importPath: "github.com/looprig/storage", want: true},
		{name: "SessionStore internal package", importPath: "github.com/looprig/sessionstore/internal/codec", want: true},
		{name: "Storage memory provider", importPath: "github.com/looprig/storage/memstore", want: false},
		{name: "cgo pseudo-package", importPath: "C", want: false},
		{name: "Harness", importPath: "github.com/looprig/harness", want: false},
		{name: "Factory", importPath: "github.com/looprig/factory", want: false},
		{name: "Host", importPath: "github.com/looprig/host", want: false},
		{name: "Centrifuge", importPath: "github.com/centrifugal/centrifuge", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := allowedProductionImport(tt.importPath); got != tt.want {
				t.Errorf("allowedProductionImport(%q) = %v, want %v", tt.importPath, got, tt.want)
			}
		})
	}
}

func TestReleasedDependencySurfaceCompiles(t *testing.T) {
	t.Parallel()

	var tenantID sessionwire.TenantID = "tenant"
	var composite storage.Composite
	if tenantID == "" || composite.OrderedIndex != nil {
		t.Fatal("unexpected dependency zero values")
	}
}

func TestProductionImportsStayWithinBoundary(t *testing.T) {
	t.Parallel()

	productionFiles, violations, err := productionImportViolations(".")
	if err != nil {
		t.Fatalf("inspect production imports: %v", err)
	}
	for _, violation := range violations {
		t.Error(violation)
	}
	if productionFiles == 0 {
		t.Fatal("no production Go files found; dependency boundary check would be vacuous")
	}
}

func TestProductionImportScanHonorsModuleOwnership(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeGoFixture(t, root, "go.mod", "module github.com/looprig/sessionstore\n")
	for _, path := range []string{
		"nested/forbidden.go",
		".worktrees/branch/forbidden.go",
		"testdata/forbidden.go",
		"_ignored/forbidden.go",
		".ignored/forbidden.go",
	} {
		writeGoFixture(t, root, path, "//go:build fixturetag\n\npackage fixture\n\nimport _ \"github.com/looprig/harness\"\n")
	}
	writeGoFixture(t, root, "nested/support_test.go", "package fixture\n\nimport _ \"github.com/looprig/harness\"\n")
	writeGoFixture(t, root, "nested-module/go.mod", "module example.com/nested\n")
	writeGoFixture(t, root, "nested-module/forbidden.go", "package fixture\n\nimport _ \"github.com/looprig/harness\"\n")
	writeGoFixture(t, root, "nested-repository/.git/HEAD", "ref: refs/heads/main\n")
	writeGoFixture(t, root, "nested-repository/forbidden.go", "package fixture\n\nimport _ \"github.com/looprig/harness\"\n")

	productionFiles, violations, err := productionImportViolations(root)
	if err != nil {
		t.Fatalf("productionImportViolations: %v", err)
	}
	if productionFiles != 1 {
		t.Fatalf("production files = %d, want 1", productionFiles)
	}
	if len(violations) != 1 || !strings.Contains(violations[0], filepath.Join("nested", "forbidden.go")) {
		t.Fatalf("violations = %q, want only real nested package violation", violations)
	}
}

func TestProductionImportScanRemainsNonvacuous(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeGoFixture(t, root, "go.mod", "module github.com/looprig/sessionstore\n")
	writeGoFixture(t, root, "testdata/ignored.go", "package ignored\n")

	productionFiles, _, err := productionImportViolations(root)
	if err != nil {
		t.Fatalf("productionImportViolations: %v", err)
	}
	if productionFiles != 0 {
		t.Fatalf("production files = %d, want 0", productionFiles)
	}
}

func TestProductionImportScanRejectsGoSymlink(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	targetRoot := t.TempDir()
	writeGoFixture(t, root, "go.mod", "module github.com/looprig/sessionstore\n")
	writeGoFixture(t, targetRoot, "target.go", "package target\n")
	link := filepath.Join(root, "linked.go")
	if err := os.Symlink(filepath.Join(targetRoot, "target.go"), link); err != nil {
		t.Fatalf("create Go file symlink: %v", err)
	}

	_, _, err := productionImportViolations(root)
	var symlinkErr *modfiles.SymlinkError
	if !errors.As(err, &symlinkErr) {
		t.Fatalf("productionImportViolations() error = %v, want *modfiles.SymlinkError", err)
	}
}

func productionImportViolations(root string) (int, []string, error) {
	files, err := modfiles.Files(root)
	if err != nil {
		return 0, nil, err
	}
	productionFiles := 0
	var violations []string
	for _, path := range files {
		if !isProductionGoFile(filepath.Base(path)) {
			continue
		}
		productionFiles++
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return 0, nil, err
		}
		for _, spec := range parsed.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return 0, nil, err
			}
			if !allowedProductionImport(importPath) {
				violations = append(violations, "production file "+path+" imports forbidden package "+strconv.Quote(importPath))
			}
		}
	}
	return productionFiles, violations, nil
}

func writeGoFixture(t *testing.T, root, relative, content string) {
	t.Helper()
	path := filepath.Join(root, relative)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create fixture directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write fixture %q: %v", relative, err)
	}
}

func allowedProductionImport(importPath string) bool {
	if importPath == "C" {
		return false
	}
	if importPath == "github.com/looprig/core" || strings.HasPrefix(importPath, "github.com/looprig/core/") {
		return true
	}
	if importPath == "github.com/looprig/storage" {
		return true
	}
	if importPath == "github.com/looprig/sessionstore" || strings.HasPrefix(importPath, "github.com/looprig/sessionstore/") {
		return true
	}
	first, _, _ := strings.Cut(importPath, "/")
	return !strings.Contains(first, ".")
}

func isProductionGoFile(name string) bool {
	return strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go")
}
