// Loaded BEFORE anything else (node --require ./tracing.js server.js) so that it can hook
// http, express, fetch, pg and pino before they are used.
const { NodeSDK } = require('@opentelemetry/sdk-node');
const { getNodeAutoInstrumentations } = require('@opentelemetry/auto-instrumentations-node');
const { NoopSpanProcessor } = require('@opentelemetry/sdk-trace');

new NodeSDK({
  serviceName: process.env.OTEL_SERVICE_NAME,
  // The Developer Sandbox has no OpenTelemetry Collector: spans are created (and give the logs
  // their trace_id) but not exported. With a Collector, replace this with an OTLP exporter.
  spanProcessors: [new NoopSpanProcessor()],
  instrumentations: [getNodeAutoInstrumentations({
    '@opentelemetry/instrumentation-fs': { enabled: false },
  })],
}).start();
