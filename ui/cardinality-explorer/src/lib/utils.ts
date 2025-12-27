import { type ClassValue, clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatNumber(num: number): string {
  if (num >= 1_000_000) {
    return `${(num / 1_000_000).toFixed(1)}M`;
  }
  if (num >= 1_000) {
    return `${(num / 1_000).toFixed(1)}K`;
  }
  return num.toLocaleString();
}

export function formatPercentage(value: number): string {
  return `${value.toFixed(1)}%`;
}

import type { DailyMetrics, AggregatedMetric, DailyLabels, LabelCardinality } from '@/types/cardinality';

export function aggregateMetrics(days: DailyMetrics[], limit?: number): AggregatedMetric[] {
  const metricMap = new Map<string, number>();

  for (const day of days) {
    for (const metric of day.metrics) {
      const current = metricMap.get(metric.metric_name) ?? 0;
      metricMap.set(metric.metric_name, current + metric.series_count);
    }
  }

  let totalSeries = 0;
  for (const count of metricMap.values()) {
    totalSeries += count;
  }

  const aggregated: AggregatedMetric[] = [];
  for (const [metric_name, series_count] of metricMap.entries()) {
    aggregated.push({
      metric_name,
      series_count,
      percentage: totalSeries > 0 ? (series_count / totalSeries) * 100 : 0,
    });
  }

  const sorted = aggregated.sort((a, b) => b.series_count - a.series_count);

  if (limit && limit > 0) {
    return sorted.slice(0, limit);
  }
  return sorted;
}

export function aggregateLabels(days: DailyLabels[], limit?: number): LabelCardinality[] {
  const labelMap = new Map<string, number>();

  for (const day of days) {
    for (const label of day.labels) {
      const current = labelMap.get(label.label_name) ?? 0;
      labelMap.set(label.label_name, Math.max(current, label.unique_values));
    }
  }

  let totalUniqueValues = 0;
  for (const count of labelMap.values()) {
    totalUniqueValues += count;
  }

  const aggregated: LabelCardinality[] = [];
  for (const [label_name, unique_values] of labelMap.entries()) {
    aggregated.push({
      label_name,
      unique_values,
      percentage: totalUniqueValues > 0 ? (unique_values / totalUniqueValues) * 100 : 0,
    });
  }

  const sorted = aggregated.sort((a, b) => b.unique_values - a.unique_values);

  if (limit && limit > 0) {
    return sorted.slice(0, limit);
  }
  return sorted;
}
