package i18n

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTranslate_English(t *testing.T) {
	got := Translate(LangEN, MsgAccepted)
	want := enMessages[MsgAccepted]
	if got != want {
		t.Errorf("Translate(en, MsgAccepted) = %q, want %q", got, want)
	}
}

func TestTranslate_Persian(t *testing.T) {
	got := Translate(LangFA, MsgAccepted)
	want := faMessages[MsgAccepted]
	if got != want {
		t.Errorf("Translate(fa, MsgAccepted) = %q, want %q", got, want)
	}
	if got == enMessages[MsgAccepted] {
		t.Error("Persian translation should differ from English")
	}
}

func TestTranslate_UnsupportedLangFallsBackToEnglish(t *testing.T) {
	got := Translate(Lang("de"), MsgAccepted)
	want := enMessages[MsgAccepted]
	if got != want {
		t.Errorf("Translate(de, MsgAccepted) = %q, want English fallback %q", got, want)
	}
}

func TestTranslate_UnknownCodeReturnsCodeItself(t *testing.T) {
	code := MessageCode("SOME_CODE_THAT_DOES_NOT_EXIST")
	got := Translate(LangEN, code)
	if got != string(code) {
		t.Errorf("Translate(en, unknown) = %q, want %q", got, string(code))
	}
}

func TestDetectLang_EmptyHeaderDefaultsToEnglish(t *testing.T) {
	if got := DetectLang(""); got != LangEN {
		t.Errorf("DetectLang(\"\") = %q, want %q", got, LangEN)
	}
}

func TestDetectLang_SimpleTag(t *testing.T) {
	if got := DetectLang("fa"); got != LangFA {
		t.Errorf("DetectLang(\"fa\") = %q, want %q", got, LangFA)
	}
}

func TestDetectLang_RegionSubtag(t *testing.T) {
	if got := DetectLang("fa-IR"); got != LangFA {
		t.Errorf("DetectLang(\"fa-IR\") = %q, want %q", got, LangFA)
	}
}

func TestDetectLang_QValuesPicksFirstSupported(t *testing.T) {
	if got := DetectLang("de-DE,fa;q=0.8,en;q=0.5"); got != LangFA {
		t.Errorf("DetectLang(multi) = %q, want %q", got, LangFA)
	}
}

func TestDetectLang_NoSupportedTagDefaultsToEnglish(t *testing.T) {
	if got := DetectLang("de-DE,fr-FR"); got != LangEN {
		t.Errorf("DetectLang(unsupported only) = %q, want %q", got, LangEN)
	}
}

func TestDetectLangFromRequest(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Language", "fa,en;q=0.8")
	if got := DetectLangFromRequest(req); got != LangFA {
		t.Errorf("DetectLangFromRequest = %q, want %q", got, LangFA)
	}
}

func TestAllCodesHaveTranslations(t *testing.T) {
	allCodes := []MessageCode{
		MsgAccepted, MsgDropped, MsgDisabled, MsgInvalidJSON, MsgMissingEventID,
		MsgMethodNotAllowed, MsgNotConfigured,
		MsgHealthOK, MsgReady, MsgNotReady,
		MsgDLQReplayOK, MsgDLQReplayEmpty, MsgDLQReplayPartial, MsgDLQReplayFailed,
		MsgDLQStatsOK, MsgDLQStatsFailed, MsgDLQNotReplayable, MsgDLQStatsUnsupported,
		MsgInternalError,
	}
	for _, code := range allCodes {
		if _, ok := enMessages[code]; !ok {
			t.Errorf("missing English translation for %s", code)
		}
		if _, ok := faMessages[code]; !ok {
			t.Errorf("missing Persian translation for %s", code)
		}
	}
}