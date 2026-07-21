package repository

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
)

const usageLogEventVersion = 1

// usageLogQueueEvent is the durable representation written to Redis Streams or
// the local WAL. It intentionally contains scalar snapshots only: serializing
// service associations would make events large and couple replay to mutable
// PostgreSQL entities.
type usageLogQueueEvent struct {
	Version            int           `json:"version"`
	BillingRequired    bool          `json:"billing_required"`
	BillingFingerprint string        `json:"billing_fingerprint,omitempty"`
	Log                usageLogEvent `json:"log"`
}

type usageLogEvent struct {
	ID        int64  `json:"id"`
	EventID   string `json:"event_id"`
	UserID    int64  `json:"user_id"`
	APIKeyID  int64  `json:"api_key_id"`
	AccountID int64  `json:"account_id"`
	RequestID string `json:"request_id"`

	UserEmail       string `json:"user_email"`
	Username        string `json:"username"`
	APIKeyName      string `json:"api_key_name"`
	AccountName     string `json:"account_name"`
	AccountPlatform string `json:"account_platform"`
	GroupName       string `json:"group_name"`
	GroupPlatform   string `json:"group_platform"`

	Model             string  `json:"model"`
	RequestedModel    string  `json:"requested_model"`
	UpstreamModel     *string `json:"upstream_model,omitempty"`
	ChannelID         *int64  `json:"channel_id,omitempty"`
	ModelMappingChain *string `json:"model_mapping_chain,omitempty"`
	BillingTier       *string `json:"billing_tier,omitempty"`
	BillingMode       *string `json:"billing_mode,omitempty"`
	ServiceTier       *string `json:"service_tier,omitempty"`
	ReasoningEffort   *string `json:"reasoning_effort,omitempty"`
	InboundEndpoint   *string `json:"inbound_endpoint,omitempty"`
	UpstreamEndpoint  *string `json:"upstream_endpoint,omitempty"`
	GroupID           *int64  `json:"group_id,omitempty"`
	SubscriptionID    *int64  `json:"subscription_id,omitempty"`

	InputTokens           int  `json:"input_tokens"`
	OutputTokens          int  `json:"output_tokens"`
	CacheCreationTokens   int  `json:"cache_creation_tokens"`
	CacheReadTokens       int  `json:"cache_read_tokens"`
	CacheCreation5mTokens int  `json:"cache_creation_5m_tokens"`
	CacheCreation1hTokens int  `json:"cache_creation_1h_tokens"`
	ImageInputTokens      int  `json:"image_input_tokens"`
	ImageOutputTokens     int  `json:"image_output_tokens"`
	ImageCount            int  `json:"image_count"`
	VideoCount            int  `json:"video_count"`
	VideoDurationSeconds  *int `json:"video_duration_seconds,omitempty"`
	DurationMs            *int `json:"duration_ms,omitempty"`
	FirstTokenMs          *int `json:"first_token_ms,omitempty"`

	ImageInputCost            float64  `json:"image_input_cost"`
	ImageOutputCost           float64  `json:"image_output_cost"`
	InputCost                 float64  `json:"input_cost"`
	OutputCost                float64  `json:"output_cost"`
	CacheCreationCost         float64  `json:"cache_creation_cost"`
	CacheReadCost             float64  `json:"cache_read_cost"`
	TotalCost                 float64  `json:"total_cost"`
	ActualCost                float64  `json:"actual_cost"`
	RateMultiplier            float64  `json:"rate_multiplier"`
	AccountRateMultiplier     *float64 `json:"account_rate_multiplier,omitempty"`
	AccountStatsCost          *float64 `json:"account_stats_cost,omitempty"`
	LongContextBillingApplied bool     `json:"long_context_billing_applied"`

	BillingType        int8                `json:"billing_type"`
	RequestType        service.RequestType `json:"request_type"`
	Stream             bool                `json:"stream"`
	OpenAIWSMode       bool                `json:"openai_ws_mode"`
	UserAgent          *string             `json:"user_agent,omitempty"`
	IPAddress          *string             `json:"ip_address,omitempty"`
	CacheTTLOverridden bool                `json:"cache_ttl_overridden"`

	ImageSize          *string        `json:"image_size,omitempty"`
	ImageInputSize     *string        `json:"image_input_size,omitempty"`
	ImageOutputSize    *string        `json:"image_output_size,omitempty"`
	ImageSizeSource    *string        `json:"image_size_source,omitempty"`
	ImageSizeBreakdown map[string]int `json:"image_size_breakdown,omitempty"`
	MediaType          *string        `json:"media_type,omitempty"`
	VideoResolution    *string        `json:"video_resolution,omitempty"`

	CreatedAt time.Time `json:"created_at"`
}

func newUsageLogQueueEvent(log *service.UsageLog, billingRequired bool, billingFingerprint ...string) usageLogQueueEvent {
	event := usageLogEventFromService(log)
	queueEvent := usageLogQueueEvent{
		Version:         usageLogEventVersion,
		BillingRequired: billingRequired,
		Log:             event,
	}
	if len(billingFingerprint) > 0 {
		queueEvent.BillingFingerprint = strings.TrimSpace(billingFingerprint[0])
	}
	return queueEvent
}

func usageLogEventFromService(log *service.UsageLog) usageLogEvent {
	if log == nil {
		return usageLogEvent{}
	}
	createdAt := log.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	} else {
		createdAt = createdAt.UTC()
	}
	eventID := normalizeUsageLogEventID(log.EventID, log.RequestID, log.APIKeyID, log.ID, createdAt)
	id := log.ID
	if id <= 0 {
		id = int64(xxhash.Sum64String(eventID) & math.MaxInt64)
		if id == 0 {
			id = 1
		}
	}
	return usageLogEvent{
		ID: id, EventID: eventID, UserID: log.UserID, APIKeyID: log.APIKeyID,
		AccountID: log.AccountID, RequestID: strings.TrimSpace(log.RequestID),
		UserEmail: log.UserEmail, Username: log.Username, APIKeyName: log.APIKeyName,
		AccountName: log.AccountName, AccountPlatform: log.AccountPlatform,
		GroupName: log.GroupName, GroupPlatform: log.GroupPlatform,
		Model: log.Model, RequestedModel: log.RequestedModel, UpstreamModel: log.UpstreamModel,
		ChannelID: log.ChannelID, ModelMappingChain: log.ModelMappingChain,
		BillingTier: log.BillingTier, BillingMode: log.BillingMode, ServiceTier: log.ServiceTier,
		ReasoningEffort: log.ReasoningEffort, InboundEndpoint: log.InboundEndpoint,
		UpstreamEndpoint: log.UpstreamEndpoint, GroupID: log.GroupID, SubscriptionID: log.SubscriptionID,
		InputTokens: log.InputTokens, OutputTokens: log.OutputTokens,
		CacheCreationTokens: log.CacheCreationTokens, CacheReadTokens: log.CacheReadTokens,
		CacheCreation5mTokens: log.CacheCreation5mTokens, CacheCreation1hTokens: log.CacheCreation1hTokens,
		ImageInputTokens: log.ImageInputTokens, ImageOutputTokens: log.ImageOutputTokens,
		ImageCount: log.ImageCount, VideoCount: log.VideoCount,
		VideoDurationSeconds: log.VideoDurationSeconds, DurationMs: log.DurationMs, FirstTokenMs: log.FirstTokenMs,
		ImageInputCost: log.ImageInputCost, ImageOutputCost: log.ImageOutputCost,
		InputCost: log.InputCost, OutputCost: log.OutputCost,
		CacheCreationCost: log.CacheCreationCost, CacheReadCost: log.CacheReadCost,
		TotalCost: log.TotalCost, ActualCost: log.ActualCost, RateMultiplier: log.RateMultiplier,
		AccountRateMultiplier: log.AccountRateMultiplier, AccountStatsCost: log.AccountStatsCost,
		LongContextBillingApplied: log.LongContextBillingApplied,
		BillingType:               log.BillingType, RequestType: log.EffectiveRequestType(),
		Stream: log.Stream, OpenAIWSMode: log.OpenAIWSMode, UserAgent: log.UserAgent,
		IPAddress: log.IPAddress, CacheTTLOverridden: log.CacheTTLOverridden,
		ImageSize: log.ImageSize, ImageInputSize: log.ImageInputSize,
		ImageOutputSize: log.ImageOutputSize, ImageSizeSource: log.ImageSizeSource,
		ImageSizeBreakdown: cloneStringIntMap(log.ImageSizeBreakdown), MediaType: log.MediaType,
		VideoResolution: log.VideoResolution, CreatedAt: createdAt,
	}
}

func (e usageLogEvent) toService() service.UsageLog {
	log := service.UsageLog{
		ID: e.ID, EventID: e.EventID, UserID: e.UserID, APIKeyID: e.APIKeyID,
		AccountID: e.AccountID, RequestID: e.RequestID,
		UserEmail: e.UserEmail, Username: e.Username, APIKeyName: e.APIKeyName,
		AccountName: e.AccountName, AccountPlatform: e.AccountPlatform,
		GroupName: e.GroupName, GroupPlatform: e.GroupPlatform,
		Model: e.Model, RequestedModel: e.RequestedModel, UpstreamModel: e.UpstreamModel,
		ChannelID: e.ChannelID, ModelMappingChain: e.ModelMappingChain,
		BillingTier: e.BillingTier, BillingMode: e.BillingMode, ServiceTier: e.ServiceTier,
		ReasoningEffort: e.ReasoningEffort, InboundEndpoint: e.InboundEndpoint,
		UpstreamEndpoint: e.UpstreamEndpoint, GroupID: e.GroupID, SubscriptionID: e.SubscriptionID,
		InputTokens: e.InputTokens, OutputTokens: e.OutputTokens,
		CacheCreationTokens: e.CacheCreationTokens, CacheReadTokens: e.CacheReadTokens,
		CacheCreation5mTokens: e.CacheCreation5mTokens, CacheCreation1hTokens: e.CacheCreation1hTokens,
		ImageInputTokens: e.ImageInputTokens, ImageOutputTokens: e.ImageOutputTokens,
		ImageCount: e.ImageCount, VideoCount: e.VideoCount,
		VideoDurationSeconds: e.VideoDurationSeconds, DurationMs: e.DurationMs, FirstTokenMs: e.FirstTokenMs,
		ImageInputCost: e.ImageInputCost, ImageOutputCost: e.ImageOutputCost,
		InputCost: e.InputCost, OutputCost: e.OutputCost,
		CacheCreationCost: e.CacheCreationCost, CacheReadCost: e.CacheReadCost,
		TotalCost: e.TotalCost, ActualCost: e.ActualCost, RateMultiplier: e.RateMultiplier,
		AccountRateMultiplier: e.AccountRateMultiplier, AccountStatsCost: e.AccountStatsCost,
		LongContextBillingApplied: e.LongContextBillingApplied,
		BillingType:               e.BillingType, RequestType: e.RequestType, Stream: e.Stream,
		OpenAIWSMode: e.OpenAIWSMode, UserAgent: e.UserAgent, IPAddress: e.IPAddress,
		CacheTTLOverridden: e.CacheTTLOverridden, ImageSize: e.ImageSize,
		ImageInputSize: e.ImageInputSize, ImageOutputSize: e.ImageOutputSize,
		ImageSizeSource: e.ImageSizeSource, ImageSizeBreakdown: cloneStringIntMap(e.ImageSizeBreakdown),
		MediaType: e.MediaType, VideoResolution: e.VideoResolution, CreatedAt: e.CreatedAt,
	}
	log.SyncRequestTypeAndLegacyFields()
	return log
}

func normalizeUsageLogEventID(eventID, requestID string, apiKeyID, logID int64, createdAt time.Time) string {
	if parsed, err := uuid.Parse(strings.TrimSpace(eventID)); err == nil {
		return parsed.String()
	}
	seed := fmt.Sprintf("%s:%d", strings.TrimSpace(requestID), apiKeyID)
	if strings.TrimSpace(requestID) == "" {
		seed = fmt.Sprintf("missing:%d:%d:%d", apiKeyID, logID, createdAt.UnixNano())
	}
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(seed)).String()
}

func cloneStringIntMap(source map[string]int) map[string]int {
	if len(source) == 0 {
		return nil
	}
	cloned := make(map[string]int, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}
