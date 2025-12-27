import { aggregateMetrics, aggregateLabels, formatNumber, formatPercentage } from './utils';
import type { DailyMetrics, DailyLabels } from '@/types/cardinality';

describe('aggregateMetrics', () => {
  it('should return empty array for empty input', () => {
    const result = aggregateMetrics([]);
    expect(result).toEqual([]);
  });

  it('should return empty array when days have no metrics', () => {
    const days: DailyMetrics[] = [
      { date: '2025-12-01', metrics: [] },
      { date: '2025-12-02', metrics: [] },
    ];
    const result = aggregateMetrics(days);
    expect(result).toEqual([]);
  });

  it('should aggregate metrics from a single day', () => {
    const days: DailyMetrics[] = [
      {
        date: '2025-12-01',
        metrics: [
          { metric_name: 'http_requests_total', series_count: 100, percentage: 50 },
          { metric_name: 'process_cpu_seconds', series_count: 100, percentage: 50 },
        ],
      },
    ];

    const result = aggregateMetrics(days);

    expect(result).toHaveLength(2);
    expect(result[0]).toEqual({
      metric_name: 'http_requests_total',
      series_count: 100,
      percentage: 50,
    });
    expect(result[1]).toEqual({
      metric_name: 'process_cpu_seconds',
      series_count: 100,
      percentage: 50,
    });
  });

  it('should sum series counts across multiple days', () => {
    const days: DailyMetrics[] = [
      {
        date: '2025-12-01',
        metrics: [
          { metric_name: 'http_requests_total', series_count: 100, percentage: 100 },
        ],
      },
      {
        date: '2025-12-02',
        metrics: [
          { metric_name: 'http_requests_total', series_count: 150, percentage: 100 },
        ],
      },
      {
        date: '2025-12-03',
        metrics: [
          { metric_name: 'http_requests_total', series_count: 50, percentage: 100 },
        ],
      },
    ];

    const result = aggregateMetrics(days);

    expect(result).toHaveLength(1);
    expect(result[0].metric_name).toBe('http_requests_total');
    expect(result[0].series_count).toBe(300); // 100 + 150 + 50
    expect(result[0].percentage).toBe(100);
  });

  it('should recalculate percentages based on aggregated totals', () => {
    const days: DailyMetrics[] = [
      {
        date: '2025-12-01',
        metrics: [
          { metric_name: 'metric_a', series_count: 100, percentage: 50 },
          { metric_name: 'metric_b', series_count: 100, percentage: 50 },
        ],
      },
      {
        date: '2025-12-02',
        metrics: [
          { metric_name: 'metric_a', series_count: 200, percentage: 100 },
          // metric_b not present on this day
        ],
      },
    ];

    const result = aggregateMetrics(days);

    // metric_a: 100 + 200 = 300
    // metric_b: 100
    // total: 400
    expect(result).toHaveLength(2);
    
    const metricA = result.find(m => m.metric_name === 'metric_a');
    const metricB = result.find(m => m.metric_name === 'metric_b');

    expect(metricA?.series_count).toBe(300);
    expect(metricA?.percentage).toBe(75); // 300/400 * 100

    expect(metricB?.series_count).toBe(100);
    expect(metricB?.percentage).toBe(25); // 100/400 * 100
  });

  it('should sort by series count descending', () => {
    const days: DailyMetrics[] = [
      {
        date: '2025-12-01',
        metrics: [
          { metric_name: 'small_metric', series_count: 10, percentage: 10 },
          { metric_name: 'large_metric', series_count: 50, percentage: 50 },
          { metric_name: 'medium_metric', series_count: 30, percentage: 30 },
        ],
      },
    ];

    const result = aggregateMetrics(days);

    expect(result[0].metric_name).toBe('large_metric');
    expect(result[1].metric_name).toBe('medium_metric');
    expect(result[2].metric_name).toBe('small_metric');
  });

  it('should handle metrics appearing on different days', () => {
    const days: DailyMetrics[] = [
      {
        date: '2025-12-01',
        metrics: [
          { metric_name: 'old_metric', series_count: 100, percentage: 100 },
        ],
      },
      {
        date: '2025-12-02',
        metrics: [
          { metric_name: 'new_metric', series_count: 200, percentage: 100 },
        ],
      },
    ];

    const result = aggregateMetrics(days);

    expect(result).toHaveLength(2);
    expect(result[0].metric_name).toBe('new_metric');
    expect(result[0].series_count).toBe(200);
    expect(result[1].metric_name).toBe('old_metric');
    expect(result[1].series_count).toBe(100);
  });

  it('should handle large numbers correctly', () => {
    const days: DailyMetrics[] = [
      {
        date: '2025-12-01',
        metrics: [
          { metric_name: 'high_cardinality', series_count: 1_000_000, percentage: 100 },
        ],
      },
      {
        date: '2025-12-02',
        metrics: [
          { metric_name: 'high_cardinality', series_count: 2_000_000, percentage: 100 },
        ],
      },
    ];

    const result = aggregateMetrics(days);

    expect(result[0].series_count).toBe(3_000_000);
  });
});

describe('formatNumber', () => {
  it('should format numbers under 1000 with locale string', () => {
    expect(formatNumber(0)).toBe('0');
    expect(formatNumber(999)).toBe('999');
  });

  it('should format thousands with K suffix', () => {
    expect(formatNumber(1000)).toBe('1.0K');
    expect(formatNumber(1500)).toBe('1.5K');
    expect(formatNumber(999999)).toBe('1000.0K');
  });

  it('should format millions with M suffix', () => {
    expect(formatNumber(1_000_000)).toBe('1.0M');
    expect(formatNumber(1_500_000)).toBe('1.5M');
    expect(formatNumber(10_000_000)).toBe('10.0M');
  });
});

describe('formatPercentage', () => {
  it('should format percentages with one decimal place', () => {
    expect(formatPercentage(0)).toBe('0.0%');
    expect(formatPercentage(50)).toBe('50.0%');
    expect(formatPercentage(99.99)).toBe('100.0%');
    expect(formatPercentage(33.333)).toBe('33.3%');
  });
});

describe('aggregateLabels', () => {
  it('should return empty array for empty input', () => {
    const result = aggregateLabels([]);
    expect(result).toEqual([]);
  });

  it('should return empty array when days have no labels', () => {
    const days: DailyLabels[] = [
      { date: '2025-12-01', labels: [] },
      { date: '2025-12-02', labels: [] },
    ];
    const result = aggregateLabels(days);
    expect(result).toEqual([]);
  });

  it('should aggregate labels from a single day', () => {
    const days: DailyLabels[] = [
      {
        date: '2025-12-01',
        labels: [
          { label_name: 'instance', unique_values: 100, percentage: 50 },
          { label_name: 'job', unique_values: 100, percentage: 50 },
        ],
      },
    ];

    const result = aggregateLabels(days);

    expect(result).toHaveLength(2);
    expect(result[0]).toEqual({
      label_name: 'instance',
      unique_values: 100,
      percentage: 50,
    });
    expect(result[1]).toEqual({
      label_name: 'job',
      unique_values: 100,
      percentage: 50,
    });
  });

  it('should take max unique values across multiple days', () => {
    const days: DailyLabels[] = [
      {
        date: '2025-12-01',
        labels: [
          { label_name: 'instance', unique_values: 100, percentage: 100 },
        ],
      },
      {
        date: '2025-12-02',
        labels: [
          { label_name: 'instance', unique_values: 150, percentage: 100 },
        ],
      },
      {
        date: '2025-12-03',
        labels: [
          { label_name: 'instance', unique_values: 50, percentage: 100 },
        ],
      },
    ];

    const result = aggregateLabels(days);

    expect(result).toHaveLength(1);
    expect(result[0].label_name).toBe('instance');
    expect(result[0].unique_values).toBe(150); // max of 100, 150, 50
  });

  it('should recalculate percentages based on aggregated totals', () => {
    const days: DailyLabels[] = [
      {
        date: '2025-12-01',
        labels: [
          { label_name: 'label_a', unique_values: 100, percentage: 50 },
          { label_name: 'label_b', unique_values: 100, percentage: 50 },
        ],
      },
      {
        date: '2025-12-02',
        labels: [
          { label_name: 'label_a', unique_values: 300, percentage: 100 },
          // label_b has lower value on this day
          { label_name: 'label_b', unique_values: 50, percentage: 0 },
        ],
      },
    ];

    const result = aggregateLabels(days);

    // label_a: max(100, 300) = 300
    // label_b: max(100, 50) = 100
    // total: 400
    expect(result).toHaveLength(2);
    
    const labelA = result.find(l => l.label_name === 'label_a');
    const labelB = result.find(l => l.label_name === 'label_b');

    expect(labelA?.unique_values).toBe(300);
    expect(labelA?.percentage).toBe(75); // 300/400 * 100

    expect(labelB?.unique_values).toBe(100);
    expect(labelB?.percentage).toBe(25); // 100/400 * 100
  });

  it('should sort by unique values descending', () => {
    const days: DailyLabels[] = [
      {
        date: '2025-12-01',
        labels: [
          { label_name: 'small_label', unique_values: 10, percentage: 10 },
          { label_name: 'large_label', unique_values: 50, percentage: 50 },
          { label_name: 'medium_label', unique_values: 30, percentage: 30 },
        ],
      },
    ];

    const result = aggregateLabels(days);

    expect(result[0].label_name).toBe('large_label');
    expect(result[1].label_name).toBe('medium_label');
    expect(result[2].label_name).toBe('small_label');
  });

  it('should apply limit when specified', () => {
    const days: DailyLabels[] = [
      {
        date: '2025-12-01',
        labels: [
          { label_name: 'label_1', unique_values: 100, percentage: 20 },
          { label_name: 'label_2', unique_values: 200, percentage: 40 },
          { label_name: 'label_3', unique_values: 50, percentage: 10 },
          { label_name: 'label_4', unique_values: 150, percentage: 30 },
        ],
      },
    ];

    const result = aggregateLabels(days, 2);

    expect(result).toHaveLength(2);
    expect(result[0].label_name).toBe('label_2'); // 200
    expect(result[1].label_name).toBe('label_4'); // 150
  });
});
