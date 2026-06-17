package i18n

import (
	"net/http"
	"strings"
)

type Lang string

const (
	LangEN Lang = "en"
	LangFA Lang = "fa"
)

var translations = map[Lang]map[MessageCode]string{
	LangEN: enMessages,
	LangFA: faMessages,
}

func Translate(lang Lang, code MessageCode) string {
	if msgs, ok := translations[lang]; ok {
		if msg, ok := msgs[code]; ok {
			return msg
		}
	}
	if msg, ok := translations[LangEN][code]; ok {
		return msg
	}
	return string(code)
}

func DetectLang(header string) Lang {
	if header == "" {
		return LangEN
	}
	for _, part := range strings.Split(header, ",") {
		tag := strings.Split(strings.TrimSpace(part), ";")[0]
		primary := Lang(strings.ToLower(strings.Split(tag, "-")[0]))
		if _, ok := translations[primary]; ok {
			return primary
		}
	}
	return LangEN
}

func DetectLangFromRequest(r *http.Request) Lang {
	return DetectLang(r.Header.Get("Accept-Language"))
}