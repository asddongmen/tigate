// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	EventStoreReceivedEventCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "input_event_count",
			Help:      "The number of events received by event store.",
		}, []string{"type"}) // types : kv, resolved.

	EventStoreWriteBytes = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "write_bytes",
			Help:      "The number of bytes written by event store.",
		})

	EventStoreScanRequestsCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "scan_requests_count",
			Help:      "The number of scan requests received by event store.",
		})

	EventStoreScanBytes = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "scan_bytes",
			Help:      "The number of bytes scanned by event store.",
		})

	EventStoreDispatcherResolvedTsLagHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "dispatcher_resolved_ts_lag",
			Help:      "Resolved Ts lag histogram of registered dispatchers for event store.",
			Buckets:   LagBucket(),
		})

	EventStoreDispatcherWatermarkLagHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "dispatcher_watermark_lag",
			Help:      "Watermark lag histogram of registered dispatchers for event store.",
			Buckets:   LagBucket(),
		})
)

func InitEventStoreMetrics(registry *prometheus.Registry) {
	registry.MustRegister(EventStoreReceivedEventCount)
	registry.MustRegister(EventStoreWriteBytes)
	registry.MustRegister(EventStoreScanRequestsCount)
	registry.MustRegister(EventStoreScanBytes)
	registry.MustRegister(EventStoreDispatcherResolvedTsLagHist)
	registry.MustRegister(EventStoreDispatcherWatermarkLagHist)
}
