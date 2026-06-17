package i18n

var faMessages = map[MessageCode]string{
	// Ingest.
	MsgAccepted:         "رویداد پذیرفته شد.",
	MsgDropped:          "بافر پر است. رویداد رها شد.",
	MsgDisabled:         "دریافت رویداد در حال حاضر غیرفعال است.",
	MsgInvalidJSON:      "ساختار JSON درخواست نامعتبر است.",
	MsgMissingEventID:   "فیلد 'event_id' الزامی است.",
	MsgMethodNotAllowed: "این متد مجاز نیست.",
	MsgNotConfigured:    "سرویس دریافت‌کننده پیکربندی نشده است.",

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
	MsgDLQNotReplayable:    "بک‌اند DLQ پیکربندی‌شده از بازپخش پشتیبانی نمی‌کند.",
	MsgDLQStatsUnsupported: "بک‌اند DLQ پیکربندی‌شده از آمار پشتیبانی نمی‌کند.",

	// Generic.
	MsgInternalError: "خطای داخلی سرور.",
}