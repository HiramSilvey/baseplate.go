package thriftbp

import (
	"context"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/reddit/baseplate.go/internal/faults"
)

type clientFaultMiddleware struct {
	address  string
	injector faults.Injector[thrift.ResponseMeta]
}

// NewClientFaultMiddleware creates and returns a new client-side fault
// injection middleware.
func NewClientFaultMiddleware(clientName, address string) clientFaultMiddleware {
	return clientFaultMiddleware{
		address: address,
		injector: faults.Injector[thrift.ResponseMeta]{
			ClientName:   clientName,
			CallerName:   "thriftpb.clientFaultMiddleware",
			AbortCodeMin: thrift.UNKNOWN_TRANSPORT_EXCEPTION,
			AbortCodeMax: thrift.END_OF_FILE,
			FaultFn: func(code int, message string) (thrift.ResponseMeta, error) {
				return thrift.ResponseMeta{}, thrift.NewTTransportException(code, message)
			},
		},
	}
}

type thriftHeaders struct{}

// Lookup returns the value of the header, if found.
func (h thriftHeaders) Lookup(ctx context.Context, key string) (string, error) {
	header, ok := thrift.GetHeader(ctx, key)
	if !ok {
		return "", nil
	}
	return header, nil
}
