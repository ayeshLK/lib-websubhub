// Copyright 2026 Ayesh Almeida
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package websubhub

import (
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultLease                     = 10 * 24 * time.Hour
	defaultRequestBody         int64 = 64 << 10
	defaultCallbackBody        int64 = 4 << 10
	defaultDeliveryBody        int64 = 64 << 10
	defaultVerificationTimeout       = 10 * time.Second
	defaultClientTimeout             = 30 * time.Second
	defaultWorkers                   = 4
	defaultQueue                     = 1024
	maxSecretBytes                   = 199
	maxSafeReasonBytes               = 256
)

var forbiddenHeaders = map[string]struct{}{
	"Connection":          {},
	"Keep-Alive":          {},
	"Proxy-Authenticate":  {},
	"Proxy-Authorization": {},
	"Te":                  {},
	"Trailer":             {},
	"Transfer-Encoding":   {},
	"Upgrade":             {},
}

func cloneHeader(h http.Header) http.Header {
	if h == nil {
		return nil
	}
	return h.Clone()
}

func cloneValues(values url.Values) url.Values {
	if values == nil {
		return nil
	}
	out := make(url.Values, len(values))
	for key, value := range values {
		out[key] = append([]string(nil), value...)
	}
	return out
}

func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}

func validateHTTPURL(raw, name string, rejectUserinfo, rejectFragment bool) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, invalidRequest(name + " must be an absolute HTTP(S) URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, invalidRequest(name + " must use HTTP or HTTPS")
	}
	if rejectUserinfo && u.User != nil {
		return nil, invalidRequest(name + " must not contain userinfo")
	}
	if rejectFragment && u.Fragment != "" {
		return nil, invalidRequest(name + " must not contain a fragment")
	}
	return u, nil
}

func normalizeHTTPURL(raw, name string, rejectUserinfo, rejectFragment bool) (string, error) {
	if _, err := validateHTTPURL(raw, name, rejectUserinfo, rejectFragment); err != nil {
		return "", err
	}
	return decodeURLUnreserved(raw), nil
}

func decodeURLUnreserved(raw string) string {
	var normalized strings.Builder
	last := 0
	changed := false
	for index := 0; index+2 < len(raw); index++ {
		if raw[index] != '%' {
			continue
		}
		high, highOK := hexValue(raw[index+1])
		low, lowOK := hexValue(raw[index+2])
		if !highOK || !lowOK {
			continue
		}
		decoded := high<<4 | low
		if !isURLUnreserved(decoded) {
			continue
		}
		if !changed {
			normalized.Grow(len(raw))
			changed = true
		}
		normalized.WriteString(raw[last:index])
		normalized.WriteByte(decoded)
		index += 2
		last = index + 1
	}
	if !changed {
		return raw
	}
	normalized.WriteString(raw[last:])
	return normalized.String()
}

func hexValue(value byte) (byte, bool) {
	switch {
	case value >= '0' && value <= '9':
		return value - '0', true
	case value >= 'a' && value <= 'f':
		return value - 'a' + 10, true
	case value >= 'A' && value <= 'F':
		return value - 'A' + 10, true
	default:
		return 0, false
	}
}

func isURLUnreserved(value byte) bool {
	return value >= 'a' && value <= 'z' ||
		value >= 'A' && value <= 'Z' ||
		value >= '0' && value <= '9' ||
		value == '-' || value == '.' || value == '_' || value == '~'
}

func newHTTPClient(supplied *http.Client, timeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = defaultClientTimeout
	}
	if supplied != nil {
		copied := *supplied
		copied.CheckRedirect = refuseRedirect
		if copied.Timeout == 0 {
			copied.Timeout = timeout
		}
		return &copied
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	return &http.Client{
		Transport:     transport,
		Timeout:       timeout,
		CheckRedirect: refuseRedirect,
	}
}

func refuseRedirect(_ *http.Request, _ []*http.Request) error {
	return http.ErrUseLastResponse
}

func readBounded(body io.Reader, limit int64) ([]byte, error) {
	if limit <= 0 {
		return nil, invalidRequest("body limit must be positive")
	}
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, invalidRequest("body exceeds configured limit")
	}
	return data, nil
}

func parseMediaType(value string) (string, error) {
	if value == "" {
		return "", invalidRequest("Content-Type is required")
	}
	mediaType, _, err := mime.ParseMediaType(value)
	if err != nil {
		return "", invalidRequest("Content-Type is malformed")
	}
	return strings.ToLower(mediaType), nil
}

func validateFormContentType(value string) error {
	mediaType, parameters, err := mime.ParseMediaType(value)
	if err != nil || !strings.EqualFold(mediaType, "application/x-www-form-urlencoded") {
		return invalidRequest("Content-Type must be application/x-www-form-urlencoded")
	}
	if charset, ok := parameters["charset"]; ok && !strings.EqualFold(charset, "utf-8") {
		return invalidRequest("unsupported form charset")
	}
	return nil
}

func validateContentType(value string) error {
	_, err := parseMediaType(value)
	return err
}

func validateHeaders(header http.Header, reserved map[string]struct{}) error {
	for name, values := range header {
		canonical := http.CanonicalHeaderKey(name)
		if canonical == "" {
			return invalidRequest("invalid header name")
		}
		if _, blocked := forbiddenHeaders[canonical]; blocked {
			return invalidRequest("unsafe header")
		}
		if _, blocked := reserved[canonical]; blocked {
			return invalidRequest("reserved header")
		}
		for _, value := range values {
			if strings.ContainsAny(value, "\r\n") {
				return invalidRequest("unsafe header value")
			}
		}
	}
	return nil
}

func copyHeaders(dst, src http.Header) {
	for name, values := range src {
		for _, value := range values {
			dst.Add(name, value)
		}
	}
}

func responseSnapshot(response *http.Response, limit int64, operation string, class error) ([]byte, error) {
	defer response.Body.Close()
	body, err := readBounded(response.Body, limit)
	if err != nil {
		return nil, &HTTPError{
			Operation:  operation,
			StatusCode: response.StatusCode,
			Header:     cloneHeader(response.Header),
			Err:        errors.Join(class, err),
		}
	}
	return body, nil
}

func formAccepted(body []byte) bool {
	values, err := url.ParseQuery(string(body))
	return err == nil && len(values["hub.mode"]) == 1 && values.Get("hub.mode") == "accepted"
}

func safeReason(reason string) string {
	reason = strings.Map(func(r rune) rune {
		if r == '\r' || r == '\n' || r == 0 {
			return -1
		}
		return r
	}, reason)
	if len(reason) > maxSafeReasonBytes {
		reason = reason[:maxSafeReasonBytes]
	}
	return reason
}

func encodedLink(target, relation string) string {
	return fmt.Sprintf("<%s>; rel=%q", target, relation)
}
