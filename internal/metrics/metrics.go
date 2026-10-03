// Package metrics declares the Prometheus instruments exported on /metrics.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const ns = "pricing"

var (
	UpstreamConnected = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "upstream_connected",
		Help: "1 while the upstream feed session is authenticated and subscribed.",
	})
	UpstreamReconnects = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "upstream_reconnects_total",
		Help: "Upstream sessions that ended and were retried.",
	})
	UpstreamMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "upstream_messages_total",
		Help: "Upstream messages by event type.",
	}, []string{"event"})
	UpstreamBytes = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "upstream_bytes_total",
		Help: "Upstream payload bytes received.",
	})
	UpstreamDecodeSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "upstream_decode_seconds",
		Help:    "JSON decode time per upstream message.",
		Buckets: prometheus.ExponentialBuckets(1e-6, 4, 10), // 1µs .. 262ms
	})
	UpstreamMessageBytes = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "upstream_message_bytes",
		Help:    "Size of upstream messages.",
		Buckets: prometheus.ExponentialBuckets(64, 4, 8), // 64B .. 1MB
	})

	Ticks = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "ticks_total",
		Help: "Ticks processed by the engine, by outcome.",
	}, []string{"result"})
	EngineQueue = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "engine_queue_depth",
		Help: "Ticks waiting in the engine input queue (sampled each second).",
	})
	EngineTickSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "engine_tick_seconds",
		Help:    "Engine processing time per tick, including fan-out to subscribers.",
		Buckets: prometheus.ExponentialBuckets(250e-9, 4, 10), // 250ns .. 65ms
	})

	PersistFlushSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "persist_flush_seconds",
		Help:    "Duration of state flush transactions.",
		Buckets: prometheus.ExponentialBuckets(1e-4, 4, 8),
	})
	PersistErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "persist_errors_total",
		Help: "Failed state flushes.",
	})
	PersistBackpressure = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "persist_backpressure_total",
		Help: "Flushes deferred because the store was still writing the previous one.",
	})

	Clients = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "ws_clients",
		Help: "Connected downstream WebSocket clients.",
	})
	ClientRejects = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "ws_rejected_total",
		Help: "Downstream connections or messages rejected, by reason.",
	}, []string{"reason"})
	FramesSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "ws_frames_sent_total",
		Help: "Update frames sent to clients, by format.",
	}, []string{"format"})
	BytesSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "ws_bytes_sent_total",
		Help: "Update frame payload bytes sent to clients (before permessage-deflate), by format.",
	}, []string{"format"})
	FrameBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "ws_frame_bytes",
		Help:    "Size of update frames sent to clients.",
		Buckets: []float64{32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 10240},
	}, []string{"format"})
	Latency = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "e2e_latency_seconds",
		Help:    "Upstream receive to downstream socket write, per frame (oldest update in the frame).",
		Buckets: []float64{50e-6, 100e-6, 250e-6, 500e-6, 1e-3, 2.5e-3, 5e-3, 10e-3, 25e-3, 50e-3, 100e-3, 250e-3},
	})
)
