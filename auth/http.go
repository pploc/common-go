package auth

import "net/http"

// ParseHTTP extracts and validates claims from HTTP request headers.
func ParseHTTP(header http.Header, options Options) (Claims, error) {
	return parse(func(key string) []string { return header.Values(key) }, options)
}
