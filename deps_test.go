package sessionstore_test

import (
	"errors"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
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
		{name: "SessionStore test support", importPath: "github.com/looprig/sessionstore/internal/testkit", want: false},
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
	writeGoFixture(t, root, "nested/imports_testkit.go", "package fixture\n\nimport _ \"github.com/looprig/sessionstore/internal/testkit\"\n")
	writeGoFixture(t, root, "internal/testkit/allowed.go", "package testkit\n\nimport _ \"github.com/looprig/storage/memstore\"\n")
	writeGoFixture(t, root, "internal/testkit/forbidden.go", "package testkit\n\nimport _ \"github.com/looprig/harness\"\n")
	writeGoFixture(t, root, "nested-module/go.mod", "module example.com/nested\n")
	writeGoFixture(t, root, "nested-module/forbidden.go", "package fixture\n\nimport _ \"github.com/looprig/harness\"\n")
	writeGoFixture(t, root, "nested-repository/.git/HEAD", "ref: refs/heads/main\n")
	writeGoFixture(t, root, "nested-repository/forbidden.go", "package fixture\n\nimport _ \"github.com/looprig/harness\"\n")

	productionFiles, violations, err := productionImportViolations(root)
	if err != nil {
		t.Fatalf("productionImportViolations: %v", err)
	}
	if productionFiles != 2 {
		t.Fatalf("production files = %d, want 2", productionFiles)
	}
	for _, suffix := range []string{
		filepath.Join("nested", "forbidden.go"),
		filepath.Join("nested", "imports_testkit.go"),
		filepath.Join("internal", "testkit", "forbidden.go"),
	} {
		if !slices.ContainsFunc(violations, func(violation string) bool { return strings.Contains(violation, suffix) }) {
			t.Errorf("violations = %q, want a violation for %s", violations, suffix)
		}
	}
	if slices.ContainsFunc(violations, func(violation string) bool {
		return strings.Contains(violation, filepath.Join("internal", "testkit", "allowed.go"))
	}) {
		t.Fatalf("violations = %q, storage/memstore must be allowed in internal/testkit", violations)
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
		testSupport := isInternalTestSupport(root, path)
		if !testSupport {
			productionFiles++
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return 0, nil, err
		}
		for _, spec := range parsed.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return 0, nil, err
			}
			allowed := allowedProductionImport(importPath)
			kind := "production file "
			if testSupport {
				allowed = allowedTestSupportImport(importPath)
				kind = "test-support file "
			}
			if !allowed {
				violations = append(violations, kind+path+" imports forbidden package "+strconv.Quote(importPath))
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
	if importPath == "github.com/looprig/sessionstore/internal/testkit" || strings.HasPrefix(importPath, "github.com/looprig/sessionstore/internal/testkit/") {
		return false
	}
	if importPath == "github.com/looprig/sessionstore" || strings.HasPrefix(importPath, "github.com/looprig/sessionstore/") {
		return true
	}
	first, _, _ := strings.Cut(importPath, "/")
	return !strings.Contains(first, ".")
}

func allowedTestSupportImport(importPath string) bool {
	if importPath == "github.com/looprig/storage/memstore" {
		return true
	}
	return allowedProductionImport(importPath)
}

func isInternalTestSupport(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	relative = filepath.ToSlash(relative)
	return strings.HasPrefix(relative, "internal/testkit/")
}

func isProductionGoFile(name string) bool {
	return strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go")
}
