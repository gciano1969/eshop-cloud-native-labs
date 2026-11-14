// One JSON line per event on stdout. The OpenTelemetry pino instrumentation adds trace_id and span_id.
const pino = require('pino');

module.exports = pino({
  level: process.env.LOG_LEVEL || 'info',
  base: { service: process.env.OTEL_SERVICE_NAME },
  timestamp: pino.stdTimeFunctions.isoTime,
  formatters: { level: (label) => ({ level: label }) },
});
