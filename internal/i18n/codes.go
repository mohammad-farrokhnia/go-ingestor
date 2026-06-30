package i18n

type MessageCode string

const (
	// Ingest.
	MsgAccepted         MessageCode = "ACCEPTED"
	MsgDropped          MessageCode = "DROPPED"
	MsgTenantQuota      MessageCode = "TENANT_QUOTA_EXCEEDED"
	MsgDisabled         MessageCode = "DISABLED"
	MsgInvalidJSON      MessageCode = "INVALID_JSON"
	MsgMissingEventID   MessageCode = "MISSING_EVENT_ID"
	MsgMissingTenantID  MessageCode = "MISSING_TENANT_ID"
	MsgMethodNotAllowed MessageCode = "METHOD_NOT_ALLOWED"
	MsgNotConfigured    MessageCode = "NOT_CONFIGURED"

	// Health.
	MsgHealthOK MessageCode = "HEALTH_OK"
	MsgReady    MessageCode = "READY"
	MsgNotReady MessageCode = "NOT_READY"

	// DLQ admin.
	MsgDLQReplayOK         MessageCode = "DLQ_REPLAY_OK"
	MsgDLQReplayEmpty      MessageCode = "DLQ_REPLAY_EMPTY"
	MsgDLQReplayPartial    MessageCode = "DLQ_REPLAY_PARTIAL"
	MsgDLQReplayFailed     MessageCode = "DLQ_REPLAY_FAILED"
	MsgDLQStatsOK          MessageCode = "DLQ_STATS_OK"
	MsgDLQStatsFailed      MessageCode = "DLQ_STATS_FAILED"
	MsgDLQNotReplayable    MessageCode = "DLQ_NOT_REPLAYABLE"
	MsgDLQStatsUnsupported MessageCode = "DLQ_STATS_UNSUPPORTED"

	// Admin auth.
	MsgUnauthorized MessageCode = "UNAUTHORIZED"

	// Generic.
	MsgInternalError MessageCode = "INTERNAL_ERROR"
)
