// RED metrics in Prometheus format on /metrics. One histogram gives Rate (_count),
// Errors (_count filtered by status) and Duration (buckets).
const client = require('prom-client');

client.collectDefaultMetrics();

const httpDuration = new client.Histogram({
  name: 'http_server_duration_seconds',
  help: 'Duration of the HTTP requests received',
  labelNames: ['method', 'route', 'status'],
  buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
});

function httpMetrics(req, res, next) {
  const end = httpDuration.startTimer();
  res.on('finish', () => end({
    method: req.method,
    // the route TEMPLATE (/orders/:id), never the path (/orders/1042): bounded cardinality
    route: req.route ? req.baseUrl + req.route.path : 'unmatched',
    status: res.statusCode,
  }));
  next();
}

async function metricsEndpoint(req, res) {
  res.set('Content-Type', client.register.contentType);
  res.end(await client.register.metrics());
}

module.exports = { client, httpMetrics, metricsEndpoint };
