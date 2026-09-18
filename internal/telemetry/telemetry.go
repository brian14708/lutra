// Package telemetry configures Lutra's OpenTelemetry providers.
package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otellog "go.opentelemetry.io/otel/log"
	logglobal "go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

// Providers owns the process-wide providers installed by Setup.
type Providers struct {
	traces  *trace.TracerProvider
	metrics *sdkmetric.MeterProvider
	logs    *log.LoggerProvider
}

// Setup installs OTLP/HTTP trace, metric, and log providers when telemetry is
// configured. Exporters are intentionally disabled when no OTEL endpoint is
// present, so local development does not attempt to contact localhost:4318.
func Setup(ctx context.Context, serviceName string) (*Providers, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	providers := &Providers{}
	if !enabled() {
		return providers, nil
	}
	if configuredName := os.Getenv("OTEL_SERVICE_NAME"); configuredName != "" {
		serviceName = configuredName
	}
	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, err
	}
	traceExporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	metricExporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		_ = traceExporter.Shutdown(ctx)
		return nil, err
	}
	logExporter, err := otlploghttp.New(ctx)
	if err != nil {
		_ = traceExporter.Shutdown(ctx)
		_ = metricExporter.Shutdown(ctx)
		return nil, err
	}
	providers.traces = trace.NewTracerProvider(
		trace.WithBatcher(traceExporter),
		trace.WithResource(res),
	)
	providers.metrics = sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(res),
	)
	providers.logs = log.NewLoggerProvider(
		log.WithProcessor(log.NewBatchProcessor(logExporter)),
		log.WithResource(res),
	)
	otel.SetTracerProvider(providers.traces)
	otel.SetMeterProvider(providers.metrics)
	logglobal.SetLoggerProvider(providers.logs)
	return providers, nil
}

// Shutdown flushes all providers owned by p. It is safe to call for a no-op
// provider set and can be used from a deferred server shutdown path.
func (p *Providers) Shutdown(ctx context.Context) error {
	if p == nil {
		return nil
	}
	var first error
	if p.metrics != nil {
		if err := p.metrics.Shutdown(ctx); err != nil && first == nil {
			first = err
		}
	}
	if p.traces != nil {
		if err := p.traces.Shutdown(ctx); err != nil && first == nil {
			first = err
		}
	}
	if p.logs != nil {
		if err := p.logs.Shutdown(ctx); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// NewSlogHandler forwards standard-library slog records to the global OTel
// logger while retaining the supplied handler for local output.
func NewSlogHandler(base slog.Handler) slog.Handler {
	return &slogHandler{base: base, logger: logglobal.Logger("github.com/brian14708/lutra")}
}

type slogHandler struct {
	base   slog.Handler
	logger otellog.Logger
	attrs  []slog.Attr
}

func (h *slogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.base.Enabled(ctx, level)
}

func (h *slogHandler) Handle(ctx context.Context, record slog.Record) error {
	if err := h.base.Handle(ctx, record); err != nil {
		return err
	}
	var otelRecord otellog.Record
	otelRecord.SetTimestamp(record.Time)
	otelRecord.SetSeverity(slogSeverity(record.Level))
	otelRecord.SetSeverityText(record.Level.String())
	otelRecord.SetBody(attribute.StringValue(record.Message))
	attrs := make([]attribute.KeyValue, 0, len(h.attrs)+record.NumAttrs())
	for _, attr := range h.attrs {
		attrs = append(attrs, slogKeyValue(attr))
	}
	record.Attrs(func(attr slog.Attr) bool {
		attrs = append(attrs, slogKeyValue(attr))
		return true
	})
	otelRecord.AddAttributes(attrs...)
	h.logger.Emit(ctx, otelRecord)
	return nil
}

func (h *slogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	copyAttrs := append(append([]slog.Attr(nil), h.attrs...), attrs...)
	return &slogHandler{base: h.base.WithAttrs(attrs), logger: h.logger, attrs: copyAttrs}
}

func (h *slogHandler) WithGroup(name string) slog.Handler {
	return &slogHandler{base: h.base.WithGroup(name), logger: h.logger, attrs: h.attrs}
}

func slogKeyValue(attr slog.Attr) attribute.KeyValue {
	attr.Value = attr.Value.Resolve()
	if attr.Key == "" {
		return attribute.KeyValue{}
	}
	return attribute.String(attr.Key, fmt.Sprint(attr.Value.Any()))
}

func slogSeverity(level slog.Level) otellog.Severity {
	switch {
	case level >= slog.LevelError:
		return otellog.SeverityError
	case level >= slog.LevelWarn:
		return otellog.SeverityWarn
	case level < slog.LevelInfo:
		return otellog.SeverityDebug
	default:
		return otellog.SeverityInfo
	}
}

func enabled() bool {
	if strings.EqualFold(os.Getenv("OTEL_SDK_DISABLED"), "true") {
		return false
	}
	for _, key := range []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
		"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
	} {
		if os.Getenv(key) != "" {
			return true
		}
	}
	return false
}
