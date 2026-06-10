package apperr

func ClassifyHTTPStatus(status int) ErrorClass {
	switch {
	case status == 429:
		return Transient
	case status >= 400 && status < 500:
		return Permanent
	default:
		return Transient
	}
}