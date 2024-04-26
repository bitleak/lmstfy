package client

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"path"
)

const (
	maxReadTimeout      = 600 // second
	maxBatchConsumeSize = 100
)

// getReq generates an HTTP request
// - ctx is the context of the request, nil if no context
// - method is the HTTP method
// - relativePath is the relative path of the request
// - query is the query parameters
// - body is the request body, nil if no body to send
func (c *LmstfyClient) getReq(ctx context.Context, method, relativePath string, query url.Values, body []byte) (
	req *http.Request, err error) {
	targetUrl := (&url.URL{
		Scheme:   c.scheme,
		Host:     c.endpoint,
		Path:     path.Join("/api", c.Namespace, relativePath),
		RawQuery: query.Encode(),
	}).String()

	var bodyReader io.Reader
	if len(body) > 0 {
		bodyReader = bytes.NewReader(body)
	}

	if ctx != nil {
		req, err = http.NewRequestWithContext(ctx, method, targetUrl, bodyReader)
	} else {
		req, err = http.NewRequest(method, targetUrl, bodyReader)
	}
	if err != nil {
		return nil, err
	}

	req.Header.Add("X-Token", c.Token)
	return req, nil
}
