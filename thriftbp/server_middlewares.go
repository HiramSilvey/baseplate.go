package thriftbp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/reddit/baseplate.go/ecinterface"
	"github.com/reddit/baseplate.go/errorsbp"
	"github.com/reddit/baseplate.go/headerbp"
	"github.com/reddit/baseplate.go/internal/gen-go/reddit/baseplate"
	"github.com/reddit/baseplate.go/internal/thriftint"
	//lint:ignore SA1019 This library is internal only, not actually deprecated
	"github.com/reddit/baseplate.go/internalv2compat"
	"github.com/reddit/baseplate.go/iobp"
	"github.com/reddit/baseplate.go/log"
	"github.com/reddit/baseplate.go/prometheusbp"
	"github.com/reddit/baseplate.go/tracing"
	"github.com/reddit/baseplate.go/transport"
)

var (
	_ thrift.ProcessorMiddleware = ExtractDeadlineBudget
	_ thrift.ProcessorMiddleware = AbandonCanceledRequests
)

// DefaultProcessorMiddlewaresArgs are the args to be passed into
// BaseplateDefaultProcessorMiddlewares function to create default processor
// middlewares.
type DefaultProcessorMiddlewaresArgs struct {
	// Suppress some of the errors returned by the server before sending them to
	// the server span.
	//
	// Based on Baseplate spec, the errors defined in your thrift IDL are not
	// treated as errors, and should be suppressed here. So in most cases that's
	// what the service developer should implement as the Suppressor here.
	//
	// Note that this suppressor only affects the errors send to the span. It
	// won't affect the errors returned to the client.
	//
	// This is optional. If it's not set IDLExceptionSuppressor will be used.
	ErrorSpanSuppressor errorsbp.Suppressor

	// Report the payload size metrics with this sample rate.
	//
	// Deprecated: Prometheus payload size metrics are always 100% reported.
	ReportPayloadSizeMetricsSampleRate float64

	// The edge context implementation. Optional.
	//
	// If it's not set, the global one from ecinterface.Get will be used instead.
	EdgeContextImpl ecinterface.Interface

	// The schema name for the thrift service being served by this server.
	//
	// The Thrift compiler does not generate schema metadata that can be read at
	// runtime. So we rely on sourcing the service name used in the schema by
	// passing an un-instrumented TProcessor to GetThriftServiceName.
	ServiceName string
}

// BaseplateDefaultProcessorMiddlewares returns the default processor
// middlewares that should be used by a baseplate Thrift service.
//
// Currently they are (in order):
//
// 1. ExtractDeadlineBudget
//
// 2. InjectServerSpan
//
// 3. InjectEdgeContext
//
// 4. ReportPayloadSizeMetrics
//
// 5. PrometheusServerMiddleware
func BaseplateDefaultProcessorMiddlewares(args DefaultProcessorMiddlewaresArgs) []thrift.ProcessorMiddleware {
	return []thrift.ProcessorMiddleware{
		// Method descriptor middleware needs to be first to support proper telemetry
		ServerMethodDescriptorMiddleware(args.ServiceName),
		ExtractDeadlineBudget,
		InjectServerSpanWithArgs(MonitorServerArgs{ServiceSlug: args.ServiceName}),
		InjectEdgeContext(args.EdgeContextImpl),
		ReportPayloadSizeMetrics(0),
		PrometheusServerMiddleware,
		ServerBaseplateHeadersMiddleware(),
	}
}

// StartSpanFromThriftContext creates a server span from thrift context object.
//
// This span would usually be used as the span of the whole thrift endpoint
// handler, and the parent of the child-spans.
//
// Caller should pass in the context object they got from thrift library,
// which would have all the required headers already injected.
//
// Please note that "Sampled" header is default to false according to baseplate
// spec, so if the context object doesn't have headers injected correctly,
// this span (and all its child-spans) will never be sampled,
// unless debug flag was set explicitly later.
//
// If any of the tracing related thrift header is present but malformed,
// it will be ignored.
// The error will also be logged if InitGlobalTracer was last called with a
// non-nil logger.
// Absent tracing related headers are always silently ignored.
func StartSpanFromThriftContext(ctx context.Context, name string) (context.Context, *tracing.Span) {
	var headers tracing.Headers
	var sampled bool

	if str, ok := header(ctx, transport.HeaderTracingTrace); ok {
		headers.TraceID = str
	}
	if str, ok := header(ctx, transport.HeaderTracingSpan); ok {
		headers.SpanID = str
	}
	if str, ok := header(ctx, transport.HeaderTracingFlags); ok {
		headers.Flags = str
	}
	if str, ok := header(ctx, transport.HeaderTracingSampled); ok {
		sampled = str == transport.HeaderTracingSampledTrue
		headers.Sampled = &sampled
	}

	return tracing.StartSpanFromHeaders(ctx, name, headers)
}

func wrapErrorForServerSpan(err error, suppressor errorsbp.Suppressor) error {
	if suppressor == nil {
		suppressor = IDLExceptionSuppressor
	}
	return thriftint.WrapBaseplateError(suppressor.Wrap(err))
}

var injectServerSpanLoggingOnce sync.Once

// InjectServerSpan implements thrift.ProcessorMiddleware and injects a server
// span into the `next` context.
//
// This middleware always use the injected v2 tracing thrift server middleware.
// If there's no v2 tracing thrift server middleware injected, it's no-op.
func InjectServerSpan(_ errorsbp.Suppressor) thrift.ProcessorMiddleware {
	if mw := internalv2compat.V2TracingThriftServerMiddleware(); mw != nil {
		return mw
	}
	return func(name string, next thrift.TProcessorFunction) thrift.TProcessorFunction {
		// no-op but log for once
		injectServerSpanLoggingOnce.Do(func() {
			slog.Warn("thriftbp.InjectServerSpan: internalv2compat.V2TracingThriftServerMiddleware() returned nil")
		})
		return next
	}
}

type MonitorServerArgs struct {
	ServiceSlug string
}

func InjectServerSpanWithArgs(args MonitorServerArgs) thrift.ProcessorMiddleware {
	if mw := internalv2compat.V2TracingThriftServerMiddlewareWithArgs(internalv2compat.ServerTraceMiddlewareArgs{
		ServiceName: args.ServiceSlug,
	}); mw != nil {
		return mw
	}
	return func(name string, next thrift.TProcessorFunction) thrift.TProcessorFunction {
		// no-op but log for once
		injectServerSpanLoggingOnce.Do(func() {
			slog.Warn("thriftbp.InjectServerSpan: internalv2compat.V2TracingThriftServerMiddlewareWithArgs() returned nil")
		})
		return next
	}
}

// InitializeEdgeContext sets an edge request context created from the Thrift
// headers set on the context onto the context and configures Thrift to forward
// the edge requent context header on any Thrift calls made by the server.
func InitializeEdgeContext(ctx context.Context, impl ecinterface.Interface) context.Context {
	header, ok := header(ctx, transport.HeaderEdgeRequest)
	if !ok {
		return ctx
	}

	ctx, err := impl.HeaderToContext(ctx, header)
	if err != nil {
		log.Error("Error while parsing EdgeRequestContext: " + err.Error())
	}
	return ctx
}

// InjectEdgeContext returns a ProcessorMiddleware that injects an edge request
// context created from the Thrift headers set on the context into the `next`
// thrift.TProcessorFunction.
//
// Note, this depends on the edge context headers already being set on the
// context object.  These should be automatically injected by your
// thrift.TSimpleServer.
func InjectEdgeContext(impl ecinterface.Interface) thrift.ProcessorMiddleware {
	if impl == nil {
		impl = ecinterface.Get()
	}
	return func(name string, next thrift.TProcessorFunction) thrift.TProcessorFunction {
		return thrift.WrappedTProcessorFunction{
			Wrapped: func(ctx context.Context, seqID int32, in, out thrift.TProtocol) (bool, thrift.TException) {
				ctx = InitializeEdgeContext(ctx, impl)
				return next.Process(ctx, seqID, in, out)
			},
		}
	}
}

// ExtractDeadlineBudget is the server middleware implementing Phase 1 of
// Baseplate deadline propagation.
//
// It only sets the timeout if the passed in deadline is at least 1ms.
func ExtractDeadlineBudget(name string, next thrift.TProcessorFunction) thrift.TProcessorFunction {
	return thrift.WrappedTProcessorFunction{
		Wrapped: func(ctx context.Context, seqID int32, in, out thrift.TProtocol) (bool, thrift.TException) {
			if s, ok := header(ctx, transport.HeaderDeadlineBudget); ok {
				if v, err := strconv.ParseInt(s, 10, 64); err == nil && v >= 1 {
					timeout := time.Duration(v) * time.Millisecond

					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, timeout)
					defer cancel()

					// The dropped return here is `ok bool`, not an error.
					client, _ := header(ctx, transport.HeaderUserAgent)
					deadlineBudgetHisto.With(prometheus.Labels{
						methodLabel: name,
						clientLabel: client,
					}).Observe(timeout.Seconds())
				}
			}
			return next.Process(ctx, seqID, in, out)
		},
	}
}

// AbandonCanceledRequests transforms context.Canceled errors into
// thrift.ErrAbandonRequest errors.
//
// When using thrift compiler version >=v0.14.0, the context object will be
// canceled after the client closes the connection, and returning
// thrift.ErrAbandonRequest as the error helps the server to not try to write
// the error back to the client, but close the connection directly.
//
// Deprecated: The checking of ErrAbandonRequest happens before all the
// middlewares so doing this in a middleware will have no effect. Please do the
// context.Canceled -> thrift.ErrAbandonRequest translation in your service's
// endpoint handlers instead.
func AbandonCanceledRequests(name string, next thrift.TProcessorFunction) thrift.TProcessorFunction {
	return thrift.WrappedTProcessorFunction{
		Wrapped: func(ctx context.Context, seqID int32, in, out thrift.TProtocol) (bool, thrift.TException) {
			ok, err := next.Process(ctx, seqID, in, out)
			if errors.Is(err, context.Canceled) {
				err = thrift.WrapTException(thrift.ErrAbandonRequest)
			}
			return ok, err
		},
	}
}

// ReportPayloadSizeMetrics returns a ProcessorMiddleware that reports metrics
// (histograms) of request and response payload sizes in bytes.
//
// This middleware only works on sampled requests with the given sample rate,
// but the histograms it reports are overriding global histogram sample rate
// with 100% sample, to avoid double sampling.
// Although the overhead it adds is minimal,
// the sample rate passed in shouldn't be set too high
// (e.g. 0.01/1% is probably a good sample rate to use).
//
// It does not count the bytes on the wire directly,
// but reconstructs the request/response with the same thrift protocol.
// As a result, the numbers it reports are not exact numbers,
// but should be good enough to show the overall trend and ballpark numbers.
//
// It also only supports THeaderProtocol.
// If the request is not in THeaderProtocol it does nothing no matter what the
// sample rate is.
//
// The prometheus histograms are:
//   - thriftbp_server_request_payload_size_bytes
//   - thriftbp_server_response_payload_size_bytes
func ReportPayloadSizeMetrics(_ float64) thrift.ProcessorMiddleware {
	return func(name string, next thrift.TProcessorFunction) thrift.TProcessorFunction {
		return thrift.WrappedTProcessorFunction{
			Wrapped: func(ctx context.Context, seqID int32, in, out thrift.TProtocol) (bool, thrift.TException) {
				// Only report for THeader requests
				if ht, ok := in.Transport().(*thrift.THeaderTransport); ok {
					protoID := ht.Protocol()
					cfg := &thrift.TConfiguration{
						THeaderProtocolID: &protoID,
					}
					var itrans, otrans countingTransport
					transport := thrift.NewTHeaderTransportConf(&itrans, cfg)
					iproto := thrift.NewTHeaderProtocolConf(transport, cfg)
					in = &thrift.TDuplicateToProtocol{
						Delegate:    in,
						DuplicateTo: iproto,
					}
					transport = thrift.NewTHeaderTransportConf(&otrans, cfg)
					oproto := thrift.NewTHeaderProtocolConf(transport, cfg)
					out = &thrift.TDuplicateToProtocol{
						Delegate:    out,
						DuplicateTo: oproto,
					}

					defer func() {
						iproto.Flush(ctx)
						oproto.Flush(ctx)
						isize := float64(itrans.Size())
						osize := float64(otrans.Size())

						proto := "header-" + tHeaderProtocol2String(protoID)
						labels := prometheus.Labels{
							methodLabel: name,
							protoLabel:  proto,
						}
						serverPayloadSizeRequestBytes.With(labels).Observe(isize)
						serverPayloadSizeResponseBytes.With(labels).Observe(osize)
					}()
				}

				return next.Process(ctx, seqID, in, out)
			},
		}
	}
}

// countingTransport implements thrift.TTransport
type countingTransport struct {
	iobp.CountingSink
}

var _ thrift.TTransport = (*countingTransport)(nil)

func (countingTransport) IsOpen() bool {
	return true
}

// All other functions are unimplemented
func (countingTransport) Close() (err error) { return }

func (countingTransport) Flush(_ context.Context) (err error) { return }

func (countingTransport) Read(_ []byte) (n int, err error) { return }

func (countingTransport) RemainingBytes() (numBytes uint64) { return }

func (countingTransport) Open() (err error) { return }

func tHeaderProtocol2String(proto thrift.THeaderProtocolID) string {
	switch proto {
	default:
		return fmt.Sprintf("%v", proto)
	case thrift.THeaderProtocolCompact:
		return "compact"
	case thrift.THeaderProtocolBinary:
		return "binary"
	}
}

// RecoverPanic recovers from panics raised in the TProccessorFunction chain,
// logs them, and records a metric indicating that the endpoint recovered from a
// panic.
//
// Deprecated: This is always added as the last server middleware (with a
// different name) to ensure that other server middlewares get the correct error
// if panic happened.
func RecoverPanic(name string, next thrift.TProcessorFunction) thrift.TProcessorFunction {
	return recoverPanik(name, next)
}

// recoverPanik recovers from panics raised in the TProccessorFunction chain,
// logs them, and records a metric indicating that the endpoint recovered from a
// panic.
func recoverPanik(name string, next thrift.TProcessorFunction) thrift.TProcessorFunction {
	counter := panicRecoverCounter.With(prometheus.Labels{
		methodLabel: name,
	})
	return thrift.WrappedTProcessorFunction{
		Wrapped: func(ctx context.Context, seqId int32, in, out thrift.TProtocol) (ok bool, err thrift.TException) {
			defer func() {
				if r := recover(); r != nil {
					var rErr error
					if asErr, ok := r.(error); ok {
						rErr = asErr
					} else {
						rErr = fmt.Errorf("panic in %q: %+v", name, r)
					}
					log.C(ctx).Errorw(
						"recovered from panic:",
						"err", rErr,
						"endpoint", name,
					)
					counter.Inc()

					// changed named return values to show that the request failed and
					// return the panic value error.
					ok = false
					err = thrift.WrapTException(rErr)
				}
			}()

			return next.Process(ctx, seqId, in, out)
		},
	}
}

// PrometheusServerMiddleware returns middleware to track Prometheus metrics
// specific to the Thrift service.
//
// It emits the following prometheus metrics:
//
// * thrift_server_active_requests gauge with labels:
//
//   - thrift_method: the method of the endpoint called
//
// * thrift_server_latency_seconds histogram with labels above plus:
//
//   - thrift_success: "true" if err == nil, "false" otherwise
//
// * thrift_server_requests_total counter with all labels above plus:
//
//   - thrift_exception_type: the human-readable exception type, e.g.
//     baseplate.Error, etc
//   - thrift_baseplate_status: the numeric status code from a baseplate.Error
//     as a string if present (e.g. 404), or the empty string
//   - thrift_baseplate_status_code: the human-readable status code, e.g.
//     NOT_FOUND, or the empty string
func PrometheusServerMiddleware(method string, next thrift.TProcessorFunction) thrift.TProcessorFunction {
	process := func(ctx context.Context, seqID int32, in, out thrift.TProtocol) (success bool, err thrift.TException) {
		start := time.Now()
		activeRequestLabels := prometheus.Labels{
			methodLabel: method,
		}
		serverActiveRequests.With(activeRequestLabels).Inc()

		defer func() {
			var baseplateStatusCode, baseplateStatus string
			exceptionTypeLabel := stringifyErrorType(err)
			success := prometheusbp.BoolString(err == nil)
			if err != nil {
				var bpErr baseplateErrorCoder
				if errors.As(err, &bpErr) {
					code := bpErr.GetCode()
					baseplateStatusCode = strconv.FormatInt(int64(code), 10)
					if status := baseplate.ErrorCode(code).String(); status != "<UNSET>" {
						baseplateStatus = status
					}
				}
			}

			latencyLabels := prometheus.Labels{
				methodLabel:  method,
				successLabel: success,
			}
			serverLatencyDistribution.With(latencyLabels).Observe(time.Since(start).Seconds())

			totalRequestLabels := prometheus.Labels{
				methodLabel:              method,
				successLabel:             success,
				exceptionLabel:           exceptionTypeLabel,
				baseplateStatusLabel:     baseplateStatus,
				baseplateStatusCodeLabel: baseplateStatusCode,
			}
			serverTotalRequests.With(totalRequestLabels).Inc()
			serverActiveRequests.With(activeRequestLabels).Dec()
		}()

		return next.Process(ctx, seqID, in, out)
	}
	return thrift.WrappedTProcessorFunction{Wrapped: process}
}

// ServerBaseplateHeadersMiddleware is a middleware that extracts baseplate headers from the incoming request and adds them to the context.
func ServerBaseplateHeadersMiddleware() thrift.ProcessorMiddleware {
	return func(name string, next thrift.TProcessorFunction) thrift.TProcessorFunction {
		return thrift.WrappedTProcessorFunction{
			Wrapped: func(ctx context.Context, seqID int32, in, out thrift.TProtocol) (bool, thrift.TException) {
				readHeaderList := thrift.GetReadHeaderList(ctx)
				if len(readHeaderList) == 0 {
					return next.Process(ctx, seqID, in, out)
				}

				headers := headerbp.NewIncomingHeaders(
					headerbp.WithThriftService("" /* service */, name),
				)
				for _, k := range readHeaderList {
					v, _ := thrift.GetHeader(ctx, k)
					headers.RecordHeader(k, v)
				}
				ctx = headers.SetOnContext(ctx)
				return next.Process(ctx, seqID, in, out)
			},
		}
	}
}

var injectServerMethodDescriptorLoggingOnce sync.Once

// ServerMethodDescriptorMiddleware is a middleware that attaches thrift method descriptors to the context of the incoming request.
//
// This is used by telemetry middleware to correctly identify the current thrift service and method.
func ServerMethodDescriptorMiddleware(serverName string) thrift.ProcessorMiddleware {
	if provider := internalv2compat.GetV2ThriftMethodDescriptorMiddlewares().ServerMiddlewareProvider; provider != nil {
		return provider(serverName)
	}
	return func(name string, next thrift.TProcessorFunction) thrift.TProcessorFunction {
		// no-op but log for once
		injectServerMethodDescriptorLoggingOnce.Do(func() {
			slog.Warn("thriftbp.ServerMethodDescriptorMiddleware: internalv2compat.GetV2ThriftMethodDescriptorMiddlewares().ServerMiddlewareProvider returned nil")
		})
		return next
	}
}
