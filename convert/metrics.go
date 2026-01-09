// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
)

// ConversionStatus represents the status of a conversion job
type ConversionStatus int

const (
	StatusDownloadingBlocks    ConversionStatus = 0
	StatusConversionInProgress ConversionStatus = 1
	StatusConversionSuccess    ConversionStatus = 2
	StatusConversionFailed     ConversionStatus = 3
)

func (s ConversionStatus) String() string {
	switch s {
	case StatusDownloadingBlocks:
		return "downloading_blocks"
	case StatusConversionInProgress:
		return "conversion_in_progress"
	case StatusConversionSuccess:
		return "conversion_success"
	case StatusConversionFailed:
		return "conversion_failed"
	default:
		return "unknown"
	}
}

// ConversionTracker manages the lifecycle of conversion job metrics
type ConversionTracker struct {
	date       util.Date
	startTime  time.Time
	metricsTTL time.Duration
}

// NewConversionTracker creates a new tracker for a conversion job
func NewConversionTracker(date util.Date, metricsTTL time.Duration) *ConversionTracker {
	tracker := &ConversionTracker{
		date:       date,
		startTime:  time.Now(),
		metricsTTL: metricsTTL,
	}
	return tracker
}

// setStatus sets the conversion status, ensuring all states are reported.
func (ct *ConversionTracker) setStatus(s ConversionStatus) {
	dateStr := ct.date.String()

	statuses := []ConversionStatus{StatusDownloadingBlocks, StatusConversionInProgress, StatusConversionSuccess, StatusConversionFailed}
	for _, status := range statuses {
		value := 0
		if status == s {
			value = 1
		}
		conversionStatus.With(prometheus.Labels{
			"date":   dateStr,
			"status": status.String(),
		}).Set(float64(value))
	}
}

// cleanup removes the status metrics for the conversion after the TTL.
func (ct *ConversionTracker) cleanup() {
	if ct.metricsTTL == 0 {
		return
	}

	time.Sleep(ct.metricsTTL)

	dateStr := ct.date.String()
	statuses := []ConversionStatus{StatusDownloadingBlocks, StatusConversionInProgress, StatusConversionSuccess, StatusConversionFailed}
	for _, status := range statuses {
		conversionStatus.Delete(prometheus.Labels{
			"date":   dateStr,
			"status": status.String(),
		})
		conversionEndTime.Delete(prometheus.Labels{
			"date":   dateStr,
			"status": status.String(),
		})
		conversionDuration.Delete(prometheus.Labels{
			"date":   dateStr,
			"status": status.String(),
		})
	}
	for _, errorType := range []string{"unknown", "context_canceled", "timeout", "download_blocks_error", "conversion_error"} {
		conversionErrors.Delete(prometheus.Labels{
			"date":       dateStr,
			"error_type": errorType,
		})
	}
	activeConversions.Delete(prometheus.Labels{
		"date": dateStr,
	})
	conversionStartTime.Delete(prometheus.Labels{
		"date": dateStr,
	})
}

// MarkDownloadingBlocks sets the conversion status to downloading blocks
func (ct *ConversionTracker) MarkDownloadingBlocks(log *slog.Logger) {
	log.Info("Marking Downloading Blocks", slog.String("date", ct.date.String()))
	ct.setStatus(StatusDownloadingBlocks)
	conversionStartTime.With(prometheus.Labels{
		"date": ct.date.String(),
	}).SetToCurrentTime()
	activeConversions.With(prometheus.Labels{
		"date": ct.date.String(),
	}).Inc()
}

// MarkConversionInProgress sets the conversion status to in progress
func (ct *ConversionTracker) MarkConversionInProgress() {
	ct.setStatus(StatusConversionInProgress)
}

// MarkConversionSuccess marks the conversion as successful
func (ct *ConversionTracker) MarkConversionSuccess(log *slog.Logger) {
	log.Info("Marking Conversion Success", slog.String("date", ct.date.String()))
	defer ct.cleanup()

	duration := time.Since(ct.startTime).Seconds()
	ct.setStatus(StatusConversionSuccess)
	conversionEndTime.With(prometheus.Labels{
		"date":   ct.date.String(),
		"status": StatusConversionSuccess.String(),
	}).SetToCurrentTime()
	conversionDuration.With(prometheus.Labels{
		"date":   ct.date.String(),
		"status": StatusConversionSuccess.String(),
	}).Observe(duration)

	activeConversions.With(prometheus.Labels{
		"date": ct.date.String(),
	}).Dec()

	lastSuccessfulConvertTime.SetToCurrentTime()
}

// MarkConversionFailed marks the conversion as failed
func (ct *ConversionTracker) MarkConversionFailed(log *slog.Logger, errorType string) {
	log.Info("Marking Conversion Failed", slog.String("date", ct.date.String()), slog.String("error_type", errorType))
	defer ct.cleanup()

	duration := time.Since(ct.startTime).Seconds()

	ct.setStatus(StatusConversionFailed)
	conversionEndTime.With(prometheus.Labels{
		"date":   ct.date.String(),
		"status": StatusConversionFailed.String(),
	}).SetToCurrentTime()
	conversionDuration.With(prometheus.Labels{
		"date":   ct.date.String(),
		"status": StatusConversionFailed.String(),
	}).Observe(duration)
	conversionErrors.With(prometheus.Labels{
		"date":       ct.date.String(),
		"error_type": errorType,
	}).Inc()
	activeConversions.With(prometheus.Labels{
		"date": ct.date.String(),
	}).Dec()
}

func (ct *ConversionTracker) MarkConversionEnd(log *slog.Logger, err error) {
	log.Info("Marking Conversion End", slog.String("date", ct.date.String()), slog.Any("error", err))
	if err != nil {
		errorType := "unknown" // Default
		if errors.Is(err, context.Canceled) {
			errorType = "context_canceled"
		} else if errors.Is(err, context.DeadlineExceeded) {
			errorType = "timeout"
		} else if strings.Contains(err.Error(), "download_blocks_error") {
			errorType = "download_blocks_error"
		} else if strings.Contains(err.Error(), "conversion_error") {
			errorType = "conversion_error"
		}
		ct.MarkConversionFailed(log, errorType)
	} else {
		ct.MarkConversionSuccess(log)
	}
}

var (
	lastSuccessfulConvertTime = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "last_successful_convert_time_unix_seconds",
		Help: "The timestamp the last conversion ran successfully.",
	})

	conversionStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "thanos_parquet_conversion_status",
		Help: "Status of TSDB to Parquet conversion jobs. 1 for active status, 0 for others.",
	}, []string{
		"date",   // Date being converted (YYYY-MM-DD format)
		"status", // Status string for readability
	})

	conversionStartTime = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "thanos_parquet_conversion_start_time_unix_seconds",
		Help: "Start time of conversion jobs in Unix seconds",
	}, []string{
		"date",
	})

	activeConversions = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "thanos_parquet_active_conversions",
		Help: "Number of currently active conversion jobs",
	}, []string{
		"date",
	})

	conversionEndTime = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "thanos_parquet_conversion_end_time_unix_seconds",
		Help: "End time of conversion jobs in Unix seconds",
	}, []string{
		"date",
		"status",
	})

	conversionDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_parquet_conversion_duration_seconds",
		Help:    "Duration of TSDB to Parquet conversion jobs",
		Buckets: prometheus.ExponentialBuckets(1, 2, 15), // 1s to ~9h
	}, []string{
		"date",
		"status",
	})

	conversionErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_parquet_conversion_errors_total",
		Help: "Total number of conversion errors by type",
	}, []string{
		"date",
		"error_type",
	})
)

func RegisterMetrics(reg prometheus.Registerer) error {
	lastSuccessfulConvertTime.Set(0)

	return errors.Join(
		reg.Register(lastSuccessfulConvertTime),
		reg.Register(conversionStatus),
		reg.Register(conversionDuration),
		reg.Register(conversionStartTime),
		reg.Register(conversionEndTime),
		reg.Register(activeConversions),
		reg.Register(conversionErrors),
	)
}
