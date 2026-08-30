package sessionstore_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
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

	productionFiles := 0
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != "." && (entry.Name() == ".git" || entry.Name() == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		if !isProductionGoFile(entry.Name()) {
			return nil
		}

		productionFiles++
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range parsed.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			if !allowedProductionImport(importPath) {
				t.Errorf("production file %s imports forbidden package %q", path, importPath)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("inspect production imports: %v", err)
	}
	if productionFiles == 0 {
		t.Fatal("no production Go files found; dependency boundary check would be vacuous")
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
