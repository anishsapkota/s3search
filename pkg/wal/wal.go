package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

const (
	walMagic       = uint32(0x53334C57) // "S3WL" little-endian
	walVersion     = uint8(2)
	headerSize     = 8 // magic(4) + version(1) + reserved(3)
	recordOverhead = 8 // payload_len(4) + crc32(4)

	walFile = "wal.log"
)

// ErrLocked is returned by Open when another process holds the WAL file lock.
var ErrLocked = errors.New("wal: another indexer holds the lock")

// WAL is an append-only write-ahead log with length-prefix + CRC32 framing.
// File layout:
//
//	[ u32 magic ][ u8 version ][ u8 reserved ×3 ]
//	[ record … ]
//
// Each record:
//
//	[ u32 payload_len ][ u32 crc32(payload,IEEE) ][ payload bytes ]
type WAL struct {
	f   *os.File
	bw  *bufio.Writer
	dir string
}

// Open opens (or creates) the WAL in dir.
// If the file is non-empty and lacks the v2 magic header it is treated as a
// v1 line-delimited WAL and transparently migrated to v2.
// Returns ErrLocked if another process holds an exclusive flock on the file.
func Open(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, walFile)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}

	// Exclusive, non-blocking lock.
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w at %s", ErrLocked, path)
		}
		return nil, fmt.Errorf("wal: flock %s: %w", path, err)
	}

	w := &WAL{f: f, bw: bufio.NewWriterSize(f, 64*1024), dir: dir}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	switch {
	case info.Size() == 0:
		// Fresh file: write v2 header.
		if err := w.writeHeader(); err != nil {
			_ = f.Close()
			return nil, err
		}
	default:
		// Check for v2 magic.
		hdr := make([]byte, headerSize)
		if _, err := f.ReadAt(hdr, 0); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("wal: read header: %w", err)
		}
		magic := binary.LittleEndian.Uint32(hdr[0:4])
		if magic != walMagic {
			// v1 file — migrate.
			if err := w.migrateV1ToV2(); err != nil {
				_ = f.Close()
				return nil, fmt.Errorf("wal: v1→v2 migration: %w", err)
			}
		}
		// v2 — seek to end for appending (O_APPEND not set; we manage position).
		if _, err := f.Seek(0, 2); err != nil {
			_ = f.Close()
			return nil, err
		}
	}

	return w, nil
}

func (w *WAL) writeHeader() error {
	var hdr [headerSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], walMagic)
	hdr[4] = walVersion
	// hdr[5:8] reserved zeros
	if _, err := w.bw.Write(hdr[:]); err != nil {
		return err
	}
	return w.bw.Flush()
}

// Append validates JSON, then writes a framed record to the buffer.
func (w *WAL) Append(rawJSON []byte) error {
	var compact bytes.Buffer
	if err := json.Compact(&compact, rawJSON); err != nil {
		return fmt.Errorf("wal: invalid JSON: %w", err)
	}
	payload := compact.Bytes()

	var rec [recordOverhead]byte
	binary.LittleEndian.PutUint32(rec[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(rec[4:8], crc32.ChecksumIEEE(payload))

	if _, err := w.bw.Write(rec[:]); err != nil {
		return err
	}
	_, err := w.bw.Write(payload)
	return err
}

// Flush flushes the buffer and fsyncs the file.
func (w *WAL) Flush() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

// Replay calls fn for each valid record in the WAL.
// On any framing or CRC error the file is truncated to the last known-good
// offset and replay returns without error (truncate-tail policy).
// Logical bad-JSON records (physically valid frames, invalid JSON) are skipped.
func (w *WAL) Replay(fn func(rawJSON []byte) error) error {
	if _, err := w.f.Seek(headerSize, 0); err != nil {
		return err
	}

	r := bufio.NewReader(w.f)
	goodOffset := int64(headerSize)
	good := 0

	for {
		var hdr [recordOverhead]byte
		if err := readFull(r, hdr[:]); err != nil {
			// Clean EOF — normal end of file.
			break
		}
		payloadLen := binary.LittleEndian.Uint32(hdr[0:4])
		wantCRC := binary.LittleEndian.Uint32(hdr[4:8])

		payload := make([]byte, payloadLen)
		if err := readFull(r, payload); err != nil {
			// Torn payload — truncate to last good record.
			slog.Warn("wal: torn payload, truncating tail", "good_records", good)
			return w.truncateToOffset(goodOffset)
		}

		if crc32.ChecksumIEEE(payload) != wantCRC {
			slog.Warn("wal: CRC mismatch, truncating tail", "good_records", good)
			return w.truncateToOffset(goodOffset)
		}

		goodOffset += int64(recordOverhead) + int64(payloadLen)

		if !json.Valid(payload) {
			slog.Warn("wal: skipping logically invalid JSON record", "len", payloadLen)
			good++
			continue
		}

		if err := fn(payload); err != nil {
			slog.Warn("wal: skipping entry that failed processing", "err", err)
		}
		good++
	}

	return nil
}

// DiscardBuffer throws away any unflushed bytes in the write buffer.
func (w *WAL) DiscardBuffer() {
	w.bw.Reset(w.f)
}

// Truncate empties the WAL and rewrites the v2 header.
func (w *WAL) Truncate() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, 0); err != nil {
		return err
	}
	w.bw.Reset(w.f)
	return w.writeHeader()
}

// Position flushes the buffer and returns the current file size in bytes.
// Useful for capturing a stable offset before snapshotting state that
// must later be truncated from the WAL.
func (w *WAL) Position() (int64, error) {
	if err := w.bw.Flush(); err != nil {
		return 0, err
	}
	info, err := w.f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// TruncatePrefix removes bytes [headerSize, off) from the WAL by reading
// the tail [off, EOF) into memory, truncating to 0, then rewriting
// header + tail. Brief stall under the caller's lock.
// Caller MUST serialize against Append (e.g. hold the index mutex).
// If off <= headerSize, returns nil (nothing to truncate).
// If off >= file size, equivalent to Truncate().
func (w *WAL) TruncatePrefix(off int64) error {
	if off <= int64(headerSize) {
		return nil
	}
	if err := w.bw.Flush(); err != nil {
		return err
	}
	info, err := w.f.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	if off >= size {
		return w.Truncate()
	}
	tailLen := size - off
	tail := make([]byte, tailLen)
	if _, err := w.f.ReadAt(tail, off); err != nil {
		return fmt.Errorf("wal: read tail: %w", err)
	}
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, 0); err != nil {
		return err
	}
	w.bw.Reset(w.f)
	if err := w.writeHeader(); err != nil {
		return err
	}
	if _, err := w.f.Write(tail); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, 2); err != nil {
		return err
	}
	return w.f.Sync()
}

// Close flushes and closes the WAL (flock released automatically by OS).
func (w *WAL) Close() error {
	_ = w.bw.Flush()
	return w.f.Close()
}

// IsEmpty reports whether the WAL contains no records (only a header).
func (w *WAL) IsEmpty() (bool, error) {
	info, err := w.f.Stat()
	if err != nil {
		return false, err
	}
	return info.Size() <= headerSize, nil
}

// truncateToOffset rewrites the file to contain only bytes [0, off) and
// repositions the write cursor at the end.
func (w *WAL) truncateToOffset(off int64) error {
	if err := w.f.Truncate(off); err != nil {
		return err
	}
	if _, err := w.f.Seek(off, 0); err != nil {
		return err
	}
	w.bw.Reset(w.f)
	return nil
}

// migrateV1ToV2 reads all valid v1 line-delimited records from the file,
// then rewrites the file in v2 format.
func (w *WAL) migrateV1ToV2() error {
	if _, err := w.f.Seek(0, 0); err != nil {
		return err
	}

	scanner := bufio.NewScanner(w.f)
	scanner.Buffer(make([]byte, 4*1024*1024), 4*1024*1024)

	var records [][]byte
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		if !json.Valid(line) {
			continue
		}
		cp := make([]byte, len(line))
		copy(cp, line)
		records = append(records, cp)
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	// Rewrite as v2.
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, 0); err != nil {
		return err
	}
	w.bw.Reset(w.f)

	if err := w.writeHeader(); err != nil {
		return err
	}
	for _, rec := range records {
		if err := w.Append(rec); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}

	slog.Info("wal: migrated v1→v2", "records", len(records))
	return nil
}

// readFull reads exactly len(buf) bytes; returns io.ErrUnexpectedEOF on short read
// and io.EOF only on a clean zero-byte read.
func readFull(r *bufio.Reader, buf []byte) error {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			if total == 0 {
				return err // clean EOF
			}
			return fmt.Errorf("short read: %w", err)
		}
	}
	return nil
}
