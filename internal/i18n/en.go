package i18n

var enMessages = map[MessageCode]string{
	// Ingest.
	MsgAccepted:         "Event accepted.",
	MsgDropped:          "Buffer is full. Event dropped.",
	MsgDisabled:         "Ingest is currently disabled.",
	MsgInvalidJSON:      "Invalid JSON in request body.",
	MsgMissingEventID:   "Field 'event_id' is required.",
	MsgMissingTenantID:  "Field 'tenant_id' is required.",
	MsgMethodNotAllowed: "Method not allowed.",
	MsgNotConfigured:    "Ingestor is not configured.",

	// Health.
	MsgHealthOK: "Service is healthy.",
	MsgReady:    "Service is ready.",
	MsgNotReady: "Service is not ready.",

	// DLQ admin.
	MsgDLQReplayOK:         "DLQ replay completed.",
	MsgDLQReplayEmpty:      "DLQ is empty. Nothing to replay.",
	MsgDLQReplayPartial:    "DLQ replay completed with some failures.",
	MsgDLQReplayFailed:     "DLQ replay failed.",
	MsgDLQStatsOK:          "DLQ stats retrieved.",
	MsgDLQStatsFailed:      "Failed to retrieve DLQ stats.",
	MsgDLQNotReplayable:    "Configured DLQ backend does not support replay.",
	MsgDLQStatsUnsupported: "Configured DLQ backend does not support stats.",

	// Generic.
	MsgInternalError: "Internal server error.",
}
