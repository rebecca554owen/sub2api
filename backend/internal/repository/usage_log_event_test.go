package repository

import (
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestUsageLogEventRoundTripPreservesScalarFields(t *testing.T) {
	t.Parallel()
	groupID := int64(7)
	multiplier := 1.25
	duration := 321
	model := "upstream-model"
	createdAt := time.Date(2026, 7, 21, 3, 4, 5, 6000000, time.UTC)
	source := &service.UsageLog{
		UserID: 1, APIKeyID: 2, AccountID: 3, RequestID: " req-1 ",
		UserEmail: "user@example.com", Username: "user", APIKeyName: "key",
		AccountName: "account", AccountPlatform: "openai", GroupID: &groupID,
		GroupName: "group", GroupPlatform: "openai", Model: "requested-model",
		RequestedModel: "requested-model", UpstreamModel: &model,
		InputTokens: 10, OutputTokens: 20, CacheReadTokens: 30,
		TotalCost: 1.2, ActualCost: 1.1, AccountRateMultiplier: &multiplier,
		RequestType: service.RequestTypeStream, DurationMs: &duration,
		ImageSizeBreakdown: map[string]int{"1024x1024": 2}, CreatedAt: createdAt,
	}

	event := usageLogEventFromService(source)
	restored := event.toService()

	require.NotEmpty(t, event.EventID)
	require.Positive(t, event.ID)
	require.Equal(t, "req-1", restored.RequestID)
	require.Equal(t, source.UserEmail, restored.UserEmail)
	require.Equal(t, source.AccountPlatform, restored.AccountPlatform)
	require.Equal(t, source.RequestedModel, restored.RequestedModel)
	require.Equal(t, source.InputTokens, restored.InputTokens)
	require.Equal(t, source.ActualCost, restored.ActualCost)
	require.Equal(t, source.ImageSizeBreakdown, restored.ImageSizeBreakdown)
	require.Equal(t, createdAt, restored.CreatedAt)
	require.True(t, restored.Stream)
}

func TestUsageLogEventIDIsStableForRequestAndAPIKey(t *testing.T) {
	t.Parallel()
	createdAt := time.Now().UTC()
	first := normalizeUsageLogEventID("", "request-123", 99, 0, createdAt)
	second := normalizeUsageLogEventID("", " request-123 ", 99, 100, createdAt.Add(time.Hour))
	require.Equal(t, first, second)
	require.NotEqual(t, first, normalizeUsageLogEventID("", "request-123", 100, 0, createdAt))
}
