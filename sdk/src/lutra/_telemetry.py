"""OpenTelemetry setup shared by the SDK client and task worker."""

from __future__ import annotations

import atexit
import logging
import os
import threading
from contextlib import suppress
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from connectrpc_otel import OpenTelemetryInterceptor
from opentelemetry import context as otel_context
from opentelemetry import metrics, propagate, trace
from opentelemetry._logs import set_logger_provider  # ruff: ignore[import-private-name]
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter  # ruff: ignore[import-private-name]
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler  # ruff: ignore[import-private-name]
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor  # ruff: ignore[import-private-name]
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

if TYPE_CHECKING:
    from connectrpc.interceptor import MetadataInterceptor
    from connectrpc.request import RequestContext
    from connectrpc_otel._interceptor import Token

_lock = threading.Lock()
_configured: dict[tuple[str, bool], Telemetry] = {}


class _PathSafeOpenTelemetryInterceptor:
    """Keep telemetry compatible with ConnectRPC addresses containing paths."""

    def __init__(
        self,
        *,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
        client: bool = False,
    ) -> None:
        self._client = client
        self._delegate = OpenTelemetryInterceptor(
            tracer_provider=tracer_provider, meter_provider=meter_provider, client=client
        )

    async def on_start(self, ctx: RequestContext[object, object]) -> Token:
        original_address = ctx.server_address
        if original_address is not None and "/" in original_address:
            # connectrpc-otel expects ``host:port`` while ConnectRPC includes
            # the configured ``/api`` prefix in the client server address.
            ctx._server_address = original_address.split("/", 1)[0]  # ruff: ignore[private-member-access]
        try:
            token = await self._delegate.on_start(ctx)
        finally:
            ctx._server_address = original_address  # ruff: ignore[private-member-access]
        if self._client:
            # connectrpc-otel 0.2 injects the current context before starting
            # its client span. Replace that header with the RPC span so the
            # remote server span is its child rather than a sibling.
            _, span, _, _ = token
            propagate.inject(ctx.request_headers, context=trace.set_span_in_context(span))
        return token

    async def on_end(
        self, token: Token, ctx: RequestContext[object, object], error: Exception | None
    ) -> None:
        await self._delegate.on_end(token, ctx, error)


@dataclass(frozen=True)
class Telemetry:
    """The providers and RPC interceptor used by one Lutra process."""

    interceptor: MetadataInterceptor[Token]
    tracer: trace.Tracer
    meter: metrics.Meter
    _tracer_provider: TracerProvider | None = None
    _meter_provider: MeterProvider | None = None
    _logger_provider: LoggerProvider | None = None
    _log_handler: logging.Handler | None = None
    _shutdown_lock: threading.Lock = field(
        default_factory=threading.Lock, repr=False, compare=False
    )
    _shutdown_started: threading.Event = field(
        default_factory=threading.Event, repr=False, compare=False
    )

    def shutdown(self) -> None:
        """Flush configured providers without making shutdown failures fatal."""
        with self._shutdown_lock:
            if self._shutdown_started.is_set():
                return
            self._shutdown_started.set()
            providers = (self._logger_provider, self._meter_provider, self._tracer_provider)
            try:
                for provider in providers:
                    if provider is None:
                        continue
                    with suppress(Exception):
                        provider.shutdown()
            finally:
                if self._log_handler is not None:
                    logging.getLogger().removeHandler(self._log_handler)


def configure(service_name: str, *, logs: bool = False) -> Telemetry:
    """Configure OTLP/HTTP providers from standard OTEL environment variables.

    Returns:
        The configured providers and ConnectRPC interceptor.
    """
    key = (service_name, logs)
    with _lock:
        existing = _configured.get(key)
        if existing is not None:
            return existing

        resource = Resource.create({
            "service.name": os.environ.get("OTEL_SERVICE_NAME", service_name)
        })
        tracer_provider: TracerProvider | None = None
        meter_provider: MeterProvider | None = None
        logger_provider: LoggerProvider | None = None
        log_handler: logging.Handler | None = None
        if _enabled():
            tracer_provider = TracerProvider(resource=resource, shutdown_on_exit=False)
            tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
            trace.set_tracer_provider(tracer_provider)

            meter_provider = MeterProvider(
                resource=resource,
                metric_readers=[PeriodicExportingMetricReader(OTLPMetricExporter())],
                shutdown_on_exit=False,
            )
            metrics.set_meter_provider(meter_provider)

            if logs:
                logger_provider = LoggerProvider(resource=resource, shutdown_on_exit=False)
                logger_provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))
                set_logger_provider(logger_provider)
                log_handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
                root_logger = logging.getLogger()
                root_logger.setLevel(logging.INFO)
                root_logger.addHandler(log_handler)

        telemetry = Telemetry(
            interceptor=_PathSafeOpenTelemetryInterceptor(
                tracer_provider=tracer_provider, meter_provider=meter_provider, client=True
            ),
            tracer=trace.get_tracer("github.com/brian14708/lutra", "0.1.0"),
            meter=metrics.get_meter("github.com/brian14708/lutra", "0.1.0"),
            _tracer_provider=tracer_provider,
            _meter_provider=meter_provider,
            _logger_provider=logger_provider,
            _log_handler=log_handler,
        )
        _configured[key] = telemetry
        atexit.register(telemetry.shutdown)
        return telemetry


def attach_worker_context(context: dict[str, object]) -> object:
    """Extract a W3C parent context from the Go worker launch envelope.

    Returns:
        A context attachment token to pass to :func:`detach_worker_context`.
    """
    carrier: dict[str, str] = {}
    for key in ("traceparent", "tracestate"):
        value = context.get(key)
        if isinstance(value, str) and value:
            carrier[key] = value
    return otel_context.attach(propagate.extract(carrier))


def detach_worker_context(token: object) -> None:
    otel_context.detach(token)  # type: ignore[arg-type]


def _enabled() -> bool:
    if os.environ.get("OTEL_SDK_DISABLED", "").lower() == "true":
        return False
    return any(
        os.environ.get(name)
        for name in (
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
        )
    )
