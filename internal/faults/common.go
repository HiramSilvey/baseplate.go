// Package faults provides common headers and client-side fault injection
// functionality.
package faults

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/reddit/baseplate.go/internal/prometheusbpint"
)

const (
	promNamespace      = "faultbp"
	clientNameLabel    = "fault_client_name"
	serviceLabel       = "fault_service"
	methodLabel        = "fault_method"
	protocolLabel      = "fault_protocol"
	successLabel       = "fault_success"
	delayInjectedLabel = "fault_injected_delay"
	abortInjectedLabel = "fault_injected_abort"
)

var (
	TotalRequests = promauto.With(prometheusbpint.GlobalRegistry).NewCounterVec(prometheus.CounterOpts{
		Namespace: promNamespace,
		Name:      "fault_requests_total",
		Help:      "Total count of requests seen by the fault injection middleware.",
	}, []string{
		clientNameLabel,
		serviceLabel,
		methodLabel,
		protocolLabel,
		successLabel,
		delayInjectedLabel,
		abortInjectedLabel,
	})
)

// Headers is an interface to be implemented by the caller to allow
// protocol-specific header lookup. Using an interface here rather than a
// function type avoids any potential closure requirements of a function.
type Headers interface {
	// Lookup returns the value of a protocol-specific header with the
	// given key.
	Lookup(ctx context.Context, key string) (string, error)
}

// ResumeFn is the function type to continue processing the protocol-specific
// request without injecting a fault.
type ResumeFn[T any] func() (T, error)

// FaultFn is the function type to inject a protocol-specific fault with the
// given code and message.
type FaultFn[T any] func(code int, message string) (T, error)

// The canonical address for a cluster-local address is <service>.<namespace>,
// without the local cluster suffix or port. The canonical address for a
// non-cluster-local address is the full original address without the port.
func getCanonicalAddress(serverAddress string) string {
	// Cluster-local address.
	if i := strings.Index(serverAddress, ".svc.cluster.local"); i != -1 {
		return serverAddress[:i]
	}
	// External host:port address.
	if i := strings.LastIndex(serverAddress, ":"); i != -1 {
		port := serverAddress[i+1:]
		// Verify this is actually a port number.
		if port != "" && port[0] >= '0' && port[0] <= '9' {
			return serverAddress[:i]
		}
	}
	// Other address, i.e. unix domain socket.
	return serverAddress
}

func parsePercentage(percentage string) (int, error) {
	if percentage == "" {
		return 100, nil
	}
	intPercentage, err := strconv.Atoi(percentage)
	if err != nil {
		return 0, fmt.Errorf("provided percentage %q is not a valid integer: %w", percentage, err)
	}
	if intPercentage < 0 || intPercentage > 100 {
		return 0, fmt.Errorf("provided percentage \"%d\" is outside the valid range of [0-100]", intPercentage)
	}
	return intPercentage, nil
}

// Global variable to allow overriding in tests.
var selected = func(percentage int) bool {
	// Use a different random integer per feature as per
	// https://github.com/grpc/proposal/blob/master/A33-Fault-Injection.md#evaluate-possibility-fraction.
	return rand.IntN(100) < percentage
}

// Global variable to allow overriding in tests.
var sleep = func(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	select {
	case <-t.C:
	case <-ctx.Done():
		t.Stop()
		return ctx.Err()
	}
	return nil
}

// Injector contains the data common across all requests needed to inject
// faults on outgoing requests.
//
// AbortCodeMin, AbortCodeMax, and FaultFn are required.
type Injector[T any] struct {
	ClientName, CallerName     string
	AbortCodeMin, AbortCodeMax int

	FaultFn FaultFn[T]
}

// Inject injects a fault on the outgoing request if it matches the header configuration.
func (i *Injector[T]) Inject(ctx context.Context, address, method string, headers Headers, resumeFn ResumeFn[T]) (T, error) {
	delayed := false
	totalReqsCounter := func(success, aborted bool) prometheus.Counter {
		return TotalRequests.WithLabelValues(
			i.ClientName,
			address,
			method,
			i.CallerName,
			strconv.FormatBool(success),
			strconv.FormatBool(delayed),
			strconv.FormatBool(aborted),
		)
	}

	infof := func(format string, args ...interface{}) {
		slog.With("caller", i.CallerName).InfoContext(ctx, fmt.Sprintf(format, args...))
	}
	warnf := func(format string, args ...interface{}) {
		slog.With("caller", i.CallerName).WarnContext(ctx, fmt.Sprintf(format, args...))
	}

	faultHeaderAddress, err := headers.Lookup(ctx, FaultServerAddressHeader)
	if err != nil {
		infof("error looking up header %q: %v", FaultServerAddressHeader, err)
		totalReqsCounter(true, false).Inc()
		return resumeFn()
	}
	requestAddress := getCanonicalAddress(address)
	if faultHeaderAddress == "" || faultHeaderAddress != requestAddress {
		infof("address %q does not match the %s header value %q", requestAddress, FaultServerAddressHeader, faultHeaderAddress)
		totalReqsCounter(true, false).Inc()
		return resumeFn()
	}

	serverMethod, err := headers.Lookup(ctx, FaultServerMethodHeader)
	if err != nil {
		infof("error looking up header %q: %v", FaultServerMethodHeader, err)
		totalReqsCounter(true, false).Inc()
		return resumeFn()
	}
	if serverMethod != "" && serverMethod != method {
		infof("method %q does not match the %s header value %q", method, FaultServerMethodHeader, serverMethod)
		totalReqsCounter(true, false).Inc()
		return resumeFn()
	}

	delayMs, err := headers.Lookup(ctx, FaultDelayMsHeader)
	if err != nil {
		infof("error looking up header %q: %v", FaultDelayMsHeader, err)
	}
	if delayMs != "" {
		percentageHeader, err := headers.Lookup(ctx, FaultDelayPercentageHeader)
		if err != nil {
			infof("error looking up header %q: %v", FaultDelayPercentageHeader, err)
		}
		percentage, err := parsePercentage(percentageHeader)
		if err != nil {
			warnf("error parsing percentage header %q: %v", FaultDelayPercentageHeader, err)
			totalReqsCounter(false, false).Inc()
			return resumeFn()
		}

		if selected(percentage) {
			delay, err := strconv.Atoi(delayMs)
			if err != nil {
				warnf("unable to convert provided delay %q to integer: %v", delayMs, err)
				totalReqsCounter(false, false).Inc()
				return resumeFn()
			}

			if err := sleep(ctx, time.Duration(delay)*time.Millisecond); err != nil {
				warnf("error when delaying request: %v", err)
				totalReqsCounter(false, false).Inc()
				return resumeFn()
			}
			delayed = true
		}
	}

	abortCode, err := headers.Lookup(ctx, FaultAbortCodeHeader)
	if err != nil {
		infof("error looking up header %q: %v", FaultAbortCodeHeader, err)
	}
	if abortCode != "" {
		percentageHeader, err := headers.Lookup(ctx, FaultAbortPercentageHeader)
		if err != nil {
			infof("error looking up header %q: %v", FaultAbortPercentageHeader, err)
		}
		percentage, err := parsePercentage(percentageHeader)
		if err != nil {
			warnf("error parsing percentage header %q: %v", FaultAbortPercentageHeader, err)
			totalReqsCounter(false, false).Inc()
			return resumeFn()
		}

		if selected(percentage) {
			code, err := strconv.Atoi(abortCode)
			if err != nil {
				warnf("unable to convert provided abort %q to integer: %v", abortCode, err)
				totalReqsCounter(false, false).Inc()
				return resumeFn()
			}
			if code < i.AbortCodeMin || code > i.AbortCodeMax {
				warnf("provided abort code \"%d\" is outside of the valid range [%d-%d]", code, i.AbortCodeMin, i.AbortCodeMax)
				totalReqsCounter(false, false).Inc()
				return resumeFn()
			}

			abortMessage, err := headers.Lookup(ctx, FaultAbortMessageHeader)
			if err != nil {
				warnf("error looking up header %q: %v", FaultAbortMessageHeader, err)
				totalReqsCounter(false, false).Inc()
				return resumeFn()
			}

			totalReqsCounter(true, true).Inc()
			return i.FaultFn(code, abortMessage)
		}
	}

	totalReqsCounter(true, false).Inc()
	return resumeFn()
}
