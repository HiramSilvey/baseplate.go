package httpbp

import (
	"context"
	"net/http"

	"github.com/reddit/baseplate.go/internal/faults"
)

type clientFaultMiddleware struct {
	injector faults.Injector[*http.Response]
}

// NewClientFaultMiddleware creates and returns a new client-side fault
// injection middleware.
func NewClientFaultMiddleware(clientName string) clientFaultMiddleware {
	return clientFaultMiddleware{
		injector: faults.Injector[*http.Response]{
			ClientName:   clientName,
			CallerName:   "httpbp.clientFaultMiddleware",
			AbortCodeMin: 400,
			AbortCodeMax: 599,
		},
	}
}

type httpHeaders struct {
	req *http.Request
}

// Lookup returns the value of the header, if found.
func (h httpHeaders) Lookup(ctx context.Context, key string) (string, error) {
	return h.req.Header.Get(key), nil
}
