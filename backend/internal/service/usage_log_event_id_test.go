package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStableUsageLogEventIDRequiresRequestID(t *testing.T) {
	t.Parallel()
	require.Empty(t, stableUsageLogEventID("", 42))
	require.Empty(t, stableUsageLogEventID("   ", 42))
	require.Equal(t, stableUsageLogEventID("request-1", 42), stableUsageLogEventID(" request-1 ", 42))
	require.NotEqual(t, stableUsageLogEventID("request-1", 42), stableUsageLogEventID("request-1", 43))
}
