package repository

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	usageLogWALActiveFile = "active.wal"
	usageLogWALMaxRecord  = 64 * 1024 * 1024
	usageLogWALHeaderSize = 8
)

type usageLogWAL struct {
	dir      string
	maxBytes int64

	mu     sync.Mutex
	active *os.File
}

func openUsageLogWAL(dir string, maxBytes int64) (*usageLogWAL, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, errors.New("usage log WAL directory is empty")
	}
	if maxBytes <= 0 {
		return nil, errors.New("usage log WAL max bytes must be positive")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create usage log WAL directory: %w", err)
	}
	active, err := os.OpenFile(filepath.Join(dir, usageLogWALActiveFile), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open usage log WAL: %w", err)
	}
	return &usageLogWAL{dir: dir, maxBytes: maxBytes, active: active}, nil
}

func (w *usageLogWAL) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.active == nil {
		return nil
	}
	err := w.active.Close()
	w.active = nil
	return err
}

func (w *usageLogWAL) Append(payload []byte) error {
	if w == nil {
		return errors.New("usage log WAL is nil")
	}
	if len(payload) == 0 {
		return errors.New("usage log WAL payload is empty")
	}
	if len(payload) > usageLogWALMaxRecord {
		return fmt.Errorf("usage log WAL record exceeds %d bytes", usageLogWALMaxRecord)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.active == nil {
		return errors.New("usage log WAL is closed")
	}
	activeInfo, err := w.active.Stat()
	if err != nil {
		return fmt.Errorf("stat active usage log WAL: %w", err)
	}
	activeSize := activeInfo.Size()
	size, err := usageLogWALDirectorySize(w.dir)
	if err != nil {
		return err
	}
	recordSize := int64(usageLogWALHeaderSize + len(payload))
	if size+recordSize > w.maxBytes {
		return fmt.Errorf("usage log WAL capacity exceeded: current=%d record=%d max=%d", size, recordSize, w.maxBytes)
	}
	header := make([]byte, usageLogWALHeaderSize)
	binary.BigEndian.PutUint32(header[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:], crc32.ChecksumIEEE(payload))
	if _, err := w.active.Write(header); err != nil {
		w.rollbackAppendLocked(activeSize)
		return fmt.Errorf("write usage log WAL header: %w", err)
	}
	if _, err := w.active.Write(payload); err != nil {
		w.rollbackAppendLocked(activeSize)
		return fmt.Errorf("write usage log WAL payload: %w", err)
	}
	if err := w.active.Sync(); err != nil {
		w.rollbackAppendLocked(activeSize)
		return fmt.Errorf("sync usage log WAL: %w", err)
	}
	return nil
}

func (w *usageLogWAL) rollbackAppendLocked(size int64) {
	if w == nil || w.active == nil || size < 0 {
		return
	}
	if err := w.active.Truncate(size); err != nil {
		return
	}
	_, _ = w.active.Seek(0, io.SeekEnd)
}

// Replay rotates the active file and replays complete immutable segments. A
// segment is deleted only after every record has been accepted by fn. Retrying
// a partially replayed segment can enqueue duplicates, which are safe because
// ClickHouse rows use stable event IDs and ReplacingMergeTree semantics.
func (w *usageLogWAL) Replay(ctx context.Context, fn func(context.Context, []byte) error) error {
	if w == nil {
		return errors.New("usage log WAL is nil")
	}
	if fn == nil {
		return errors.New("usage log WAL replay callback is nil")
	}
	segments, err := w.rotateAndListSegments()
	if err != nil {
		return err
	}
	for _, segment := range segments {
		if err := replayUsageLogWALSegment(ctx, segment, fn); err != nil {
			return err
		}
		if err := os.Remove(segment); err != nil {
			return fmt.Errorf("remove replayed usage log WAL segment %s: %w", filepath.Base(segment), err)
		}
	}
	return nil
}

func (w *usageLogWAL) SizeBytes() (int64, error) {
	if w == nil {
		return 0, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return usageLogWALDirectorySize(w.dir)
}

func (w *usageLogWAL) rotateAndListSegments() ([]string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.active == nil {
		return nil, errors.New("usage log WAL is closed")
	}
	if err := w.active.Sync(); err != nil {
		return nil, fmt.Errorf("sync usage log WAL before rotation: %w", err)
	}
	info, err := w.active.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat usage log WAL: %w", err)
	}
	if err := w.active.Close(); err != nil {
		return nil, fmt.Errorf("close usage log WAL before rotation: %w", err)
	}
	w.active = nil
	activePath := filepath.Join(w.dir, usageLogWALActiveFile)
	if info.Size() > 0 {
		segmentPath := filepath.Join(w.dir, fmt.Sprintf("segment-%020d.wal", time.Now().UTC().UnixNano()))
		if err := os.Rename(activePath, segmentPath); err != nil {
			return nil, fmt.Errorf("rotate usage log WAL: %w", err)
		}
	}
	active, err := os.OpenFile(activePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("reopen usage log WAL after rotation: %w", err)
	}
	w.active = active
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, fmt.Errorf("list usage log WAL segments: %w", err)
	}
	segments := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "segment-") || !strings.HasSuffix(entry.Name(), ".wal") {
			continue
		}
		segments = append(segments, filepath.Join(w.dir, entry.Name()))
	}
	sort.Strings(segments)
	return segments, nil
}

func replayUsageLogWALSegment(ctx context.Context, path string, fn func(context.Context, []byte) error) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open usage log WAL segment %s: %w", filepath.Base(path), err)
	}
	defer func() { _ = file.Close() }()
	reader := bufio.NewReader(file)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		header := make([]byte, usageLogWALHeaderSize)
		_, err := io.ReadFull(reader, header)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			// A process crash can leave a trailing, incomplete record that was
			// never fsync-confirmed to the billing path. Ignore only that tail so
			// earlier durable records do not permanently block replay.
			return nil
		}
		if err != nil {
			return fmt.Errorf("read usage log WAL header from %s: %w", filepath.Base(path), err)
		}
		length := binary.BigEndian.Uint32(header[:4])
		checksum := binary.BigEndian.Uint32(header[4:])
		if length == 0 || length > usageLogWALMaxRecord {
			return fmt.Errorf("invalid usage log WAL record length %d in %s", length, filepath.Base(path))
		}
		payload := make([]byte, int(length))
		if _, err := io.ReadFull(reader, payload); errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return fmt.Errorf("read usage log WAL payload from %s: %w", filepath.Base(path), err)
		}
		if actual := crc32.ChecksumIEEE(payload); actual != checksum {
			return fmt.Errorf("usage log WAL checksum mismatch in %s", filepath.Base(path))
		}
		if err := fn(ctx, payload); err != nil {
			return err
		}
	}
}

func usageLogWALDirectorySize(dir string) (int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("list usage log WAL directory: %w", err)
	}
	var total int64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".wal") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return 0, fmt.Errorf("stat usage log WAL file %s: %w", entry.Name(), err)
		}
		total += info.Size()
	}
	return total, nil
}
