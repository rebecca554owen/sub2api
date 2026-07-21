package repository

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUsageLogWALAppendReplayAndDelete(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	wal, err := openUsageLogWAL(dir, 1024*1024)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })

	require.NoError(t, wal.Append([]byte(`{"event":1}`)))
	require.NoError(t, wal.Append([]byte(`{"event":2}`)))

	var replayed []string
	err = wal.Replay(context.Background(), func(_ context.Context, payload []byte) error {
		replayed = append(replayed, string(payload))
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{`{"event":1}`, `{"event":2}`}, replayed)
	size, err := wal.SizeBytes()
	require.NoError(t, err)
	require.Zero(t, size)
}

func TestUsageLogWALKeepsSegmentWhenReplayFails(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	wal, err := openUsageLogWAL(dir, 1024*1024)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	require.NoError(t, wal.Append([]byte("payload")))

	err = wal.Replay(context.Background(), func(context.Context, []byte) error {
		return os.ErrDeadlineExceeded
	})
	require.ErrorIs(t, err, os.ErrDeadlineExceeded)
	size, err := wal.SizeBytes()
	require.NoError(t, err)
	require.Positive(t, size)
}

func TestUsageLogWALRejectsCapacityOverflow(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	wal, err := openUsageLogWAL(dir, usageLogWALHeaderSize+3)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	require.Error(t, wal.Append([]byte("four")))
}

func TestReplayUsageLogWALDetectsCorruption(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "segment-corrupt.wal")
	require.NoError(t, os.WriteFile(path, []byte{0, 0, 0, 4, 0, 0, 0, 1, 't', 'e', 's', 't'}, 0o600))
	err := replayUsageLogWALSegment(context.Background(), path, func(context.Context, []byte) error { return nil })
	require.ErrorContains(t, err, "checksum mismatch")
}

func TestReplayUsageLogWALIgnoresIncompleteCrashTail(t *testing.T) {
	t.Parallel()
	payload := []byte("durable")
	record := make([]byte, usageLogWALHeaderSize+len(payload))
	binary.BigEndian.PutUint32(record[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(record[4:8], crc32.ChecksumIEEE(payload))
	copy(record[8:], payload)
	record = append(record, 0, 0, 0)
	path := filepath.Join(t.TempDir(), "segment-partial.wal")
	require.NoError(t, os.WriteFile(path, record, 0o600))

	var replayed [][]byte
	err := replayUsageLogWALSegment(context.Background(), path, func(_ context.Context, value []byte) error {
		replayed = append(replayed, append([]byte(nil), value...))
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, [][]byte{payload}, replayed)
}
