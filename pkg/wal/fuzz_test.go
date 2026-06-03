package wal

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func FuzzReplay(f *testing.F) {
	// Seed: valid v2 header + one record containing `{"x":1}`.
	payload := []byte(`{"x":1}`)
	var seed [headerSize + recordOverhead + len(`{"x":1}`)]byte
	binary.LittleEndian.PutUint32(seed[0:4], walMagic)
	seed[4] = walVersion
	binary.LittleEndian.PutUint32(seed[8:12], uint32(len(payload)))
	binary.LittleEndian.PutUint32(seed[12:16], crc32.ChecksumIEEE(payload))
	copy(seed[16:], payload)
	f.Add(seed[:])

	// Seed: empty v2 file.
	var empty [headerSize]byte
	binary.LittleEndian.PutUint32(empty[0:4], walMagic)
	empty[4] = walVersion
	f.Add(empty[:])

	// Seed: random garbage.
	f.Add([]byte("notawalfile"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		path := filepath.Join(dir, walFile)
		_ = os.WriteFile(path, data, 0o644)

		w, err := Open(dir)
		if err != nil {
			return // ErrLocked won't happen in single-process; other errors fine
		}
		defer w.Close()
		_ = w.Replay(func(b []byte) error { return nil }) // must not panic
	})
}
