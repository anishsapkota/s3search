package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func writeNRecords(t *testing.T, w *WAL, n int) {
	t.Helper()
	for i := range n {
		raw := []byte(fmt.Sprintf(`{"i":%d}`, i))
		if err := w.Append(raw); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func replayAll(t *testing.T, w *WAL) [][]byte {
	t.Helper()
	var got [][]byte
	if err := w.Replay(func(b []byte) error {
		cp := make([]byte, len(b))
		copy(cp, b)
		got = append(got, cp)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	return got
}

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	writeNRecords(t, w, 1000)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	got := replayAll(t, w2)
	if len(got) != 1000 {
		t.Fatalf("want 1000 records, got %d", len(got))
	}
	for i, b := range got {
		want := fmt.Sprintf(`{"i":%d}`, i)
		if string(b) != want {
			t.Fatalf("record %d: got %q want %q", i, b, want)
		}
	}
}

func TestPartialWrite_Truncated(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	writeNRecords(t, w, 10)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Truncate last 5 payload bytes of the last record.
	path := filepath.Join(dir, walFile)
	info, _ := os.Stat(path)
	_ = os.Truncate(path, info.Size()-5)

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	got := replayAll(t, w2)
	if len(got) != 9 {
		t.Fatalf("want 9 records after torn tail, got %d", len(got))
	}

	// File must have been rewritten to only cover 9 records.
	info2, _ := os.Stat(path)
	if info2.Size() >= info.Size() {
		t.Fatalf("expected file shrinkage after truncate-tail, size=%d", info2.Size())
	}
}

func TestCRCMismatch_InMiddle(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	writeNRecords(t, w, 10)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt one byte inside record 5's payload.
	// Records 0-4 each: recordOverhead(8) + len(`{"i":N}`) bytes.
	// Compute offset of record 5's payload start.
	path := filepath.Join(dir, walFile)
	f, _ := os.OpenFile(path, os.O_RDWR, 0o644)
	off := int64(headerSize)
	for i := range 5 {
		payload := []byte(fmt.Sprintf(`{"i":%d}`, i))
		off += int64(recordOverhead) + int64(len(payload))
	}
	// off now points at record 5 header; skip header to reach payload.
	off += int64(recordOverhead)
	// flip first payload byte
	buf := make([]byte, 1)
	f.ReadAt(buf, off)
	buf[0] ^= 0xFF
	f.WriteAt(buf, off)
	f.Close()

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	got := replayAll(t, w2)
	if len(got) != 5 {
		t.Fatalf("want 5 records before CRC-corrupt record, got %d", len(got))
	}
}

func TestZeroLengthGarbage(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	writeNRecords(t, w, 5)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Append 100 zero bytes after good records.
	path := filepath.Join(dir, walFile)
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write(make([]byte, 100))
	f.Close()

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	got := replayAll(t, w2)
	if len(got) != 5 {
		t.Fatalf("want 5 records, got %d", len(got))
	}
}

func TestV1Migration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, walFile)

	// Write v1 line-delimited JSON directly.
	f, _ := os.Create(path)
	for i := range 5 {
		f.WriteString(fmt.Sprintf("{\"i\":%d}\n", i))
	}
	f.Close()

	// Open should detect v1 and migrate.
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	got := replayAll(t, w)
	if len(got) != 5 {
		t.Fatalf("want 5 records after migration, got %d", len(got))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Second Open must see v2 magic — no re-migration.
	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	// Verify magic on disk.
	hdr := make([]byte, headerSize)
	w2.f.ReadAt(hdr, 0)
	if binary.LittleEndian.Uint32(hdr[0:4]) != walMagic {
		t.Fatal("expected v2 magic after second Open")
	}
	if hdr[4] != walVersion {
		t.Fatalf("expected version %d, got %d", walVersion, hdr[4])
	}

	got2 := replayAll(t, w2)
	if len(got2) != 5 {
		t.Fatalf("want 5 records on second Open, got %d", len(got2))
	}
}

func TestFlockBlocksSecondOpen(t *testing.T) {
	dir := t.TempDir()
	w1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w1.Close()

	_, err = Open(dir)
	if err == nil {
		t.Fatal("expected ErrLocked, got nil")
	}
	if !isErrLocked(err) {
		t.Fatalf("expected ErrLocked, got %v", err)
	}

	// After close, second Open must succeed.
	w1.Close()
	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	w2.Close()
}

func TestDiscardBuffer_NoLeakToNextBatch(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Append then discard (simulates mid-batch failure).
	_ = w.Append([]byte(`{"discard":true}`))
	w.DiscardBuffer()

	// Append + flush real record.
	if err := w.Append([]byte(`{"keep":true}`)); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	got := replayAll(t, w)
	if len(got) != 1 {
		t.Fatalf("want 1 record, got %d", len(got))
	}
	if string(got[0]) != `{"keep":true}` {
		t.Fatalf("unexpected record: %s", got[0])
	}
}

func isErrLocked(err error) bool {
	if err == nil {
		return false
	}
	// errors.Is won't work across fmt.Errorf wrapping without %w — check string.
	return len(err.Error()) > 0 && containsErrLocked(err)
}

func containsErrLocked(err error) bool {
	return fmt.Sprintf("%v", err) != "" &&
		(err == ErrLocked || unwrapContains(err))
}

func unwrapContains(err error) bool {
	type unwrapper interface{ Unwrap() error }
	for err != nil {
		if err == ErrLocked {
			return true
		}
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
