package i18n

var faMessages = map[MessageCode]string{
	// Ingest.
	MsgAccepted:         "رویداد پذیرفته شد.",
	MsgDropped:          "بافر پر است. رویداد رها شد.",
	MsgDisabled:         "دریافت رویداد در حال حاضر غیرفعال است.",
	MsgInvalidJSON:      "ساختار JSON درخواست نامعتبر است.",
	MsgMissingEventID:   "فیلد 'event_id' الزامی است.",
	MsgMethodNotAllowed: "این متد مجاز نیست.",
	MsgNotConfigured:    "سرویس دریافت\u200cکننده پیکربندی نشده است.",

	// Health.
	MsgHealthOK: "سرویس سالم است.",
	MsgReady:    "سرویس آماده است.",
	MsgNotReady: "سرویس آماده نیست.",

	// DLQ admin.
	MsgDLQReplayOK:         "بازپخش DLQ با موفقیت انجام شد.",
	MsgDLQReplayEmpty:      "DLQ خالی است. چیزی برای بازپخش وجود ندارد.",
	MsgDLQReplayPartial:    "بازپخش DLQ با برخی خطاها به پایان رسید.",
	MsgDLQReplayFailed:     "بازپخش DLQ ناموفق بود.",
	MsgDLQStatsOK:          "آمار DLQ با موفقیت دریافت شد.",
	MsgDLQStatsFailed:      "دریافت آمار DLQ ناموفق بود.",
	MsgDLQNotReplayable:    "بک\u200cاند DLQ پیکربندی\u200cشده از بازپخش پشتیبانی نمی\u200cکند.",
	MsgDLQStatsUnsupported: "بک\u200cاند DLQ پیکربندی\u200cشده از آمار پشتیبانی نمی\u200cکند.",

	// Generic.
	MsgInternalError: "خطای داخلی سرور.",
}