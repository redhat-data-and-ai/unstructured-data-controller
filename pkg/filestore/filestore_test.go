/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package filestore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// onlyEntry fails unless dir holds exactly want, so a stray temporary file shows up.
func onlyEntry(t *testing.T, dir, want string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	assert.Equal(t, []string{want}, names)
}

func TestWriteFileAtomic(t *testing.T) {
	t.Run("writes the contents with the usual permissions", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "converted.json")

		require.NoError(t, writeFileAtomic(path, []byte(`{"ok":true}`)))

		data, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, `{"ok":true}`, string(data))
		info, err := os.Stat(path)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o644), info.Mode().Perm())
		onlyEntry(t, dir, "converted.json")
	})

	t.Run("replaces an existing file whole", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "converted.json")
		require.NoError(t, os.WriteFile(path, []byte("an older and much longer body"), 0o644))

		require.NoError(t, writeFileAtomic(path, []byte("new")))

		data, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, "new", string(data))
		onlyEntry(t, dir, "converted.json")
	})

	t.Run("a missing directory is an error", func(t *testing.T) {
		err := writeFileAtomic(filepath.Join(t.TempDir(), "missing", "converted.json"), []byte("x"))
		assert.ErrorIs(t, err, os.ErrNotExist)
	})

	t.Run("a failed rename leaves no temporary file behind", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "converted.json")
		// A non-empty directory where the file should go makes the rename fail.
		require.NoError(t, os.MkdirAll(filepath.Join(path, "occupied"), 0o755))

		require.Error(t, writeFileAtomic(path, []byte("x")))

		onlyEntry(t, dir, "converted.json")
	})
}
