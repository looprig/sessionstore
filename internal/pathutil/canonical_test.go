package pathutil

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func TestCanonicalize(t *testing.T) {
	tests := []struct {
		name         string
		setup        func(*testing.T) ([]string, []string)
		wantErr      bool
		wantNotExist bool
	}{
		{name: "root", setup: func(t *testing.T) ([]string, []string) {
			root, err := filepath.EvalSymlinks(string(os.PathSeparator))
			if err != nil {
				t.Fatal(err)
			}
			return []string{string(os.PathSeparator)}, []string{root}
		}},
		{name: "relative existing input", setup: func(t *testing.T) ([]string, []string) {
			cwd, err := os.Getwd()
			if err != nil {
				t.Fatal(err)
			}
			target := t.TempDir()
			relative, err := filepath.Rel(cwd, target)
			if err != nil {
				t.Fatal(err)
			}
			canonical, err := filepath.EvalSymlinks(target)
			if err != nil {
				t.Fatal(err)
			}
			return []string{relative}, []string{canonical}
		}},
		{name: "sorted deduplicated and empty ignored", setup: func(t *testing.T) ([]string, []string) {
			a, b := t.TempDir(), t.TempDir()
			a, _ = filepath.EvalSymlinks(a)
			b, _ = filepath.EvalSymlinks(b)
			want := []string{a, b}
			sort.Strings(want)
			return []string{b, "", a, b}, want
		}},
		{name: "missing tail below symlink", setup: func(t *testing.T) ([]string, []string) {
			target := t.TempDir()
			alias := filepath.Join(t.TempDir(), "alias")
			if err := os.Symlink(target, alias); err != nil {
				t.Fatal(err)
			}
			target, _ = filepath.EvalSymlinks(target)
			return []string{filepath.Join(alias, "missing", "tail")}, []string{filepath.Join(target, "missing", "tail")}
		}},
		{name: "broken symlink", setup: func(t *testing.T) ([]string, []string) {
			base := t.TempDir()
			alias := filepath.Join(base, "broken")
			if err := os.Symlink(filepath.Join(base, "absent"), alias); err != nil {
				t.Fatal(err)
			}
			return []string{filepath.Join(alias, "tail")}, nil
		}, wantErr: true, wantNotExist: true},
		{name: "file ancestor", setup: func(t *testing.T) ([]string, []string) {
			file := filepath.Join(t.TempDir(), "file")
			if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
			return []string{filepath.Join(file, "tail")}, nil
		}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paths, want := tt.setup(t)
			got, err := Canonicalize(paths)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				var pathErr *CanonicalPathError
				if !errors.As(err, &pathErr) {
					t.Fatalf("err = %T, want *CanonicalPathError", err)
				}
				if tt.wantNotExist && !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("err = %v, want not-exist cause", err)
				}
				return
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("Canonicalize = %v, want %v", got, want)
			}
		})
	}
}
