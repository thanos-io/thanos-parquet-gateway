import { z } from 'zod';

export const MATCHER_OPERATORS = [
  { name: '=', label: '=' },
  { name: '!=', label: '!=' },
  { name: '=~', label: '=~' },
  { name: '!~', label: '!~' },
];

export type MatcherOperator = '=' | '!=' | '=~' | '!~';

export const CardinalityRequestSchema = z.object({
  start: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'Invalid date format (YYYY-MM-DD)'),
  end: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'Invalid date format (YYYY-MM-DD)'),
  limit: z.number().min(1).max(1000).default(10),
}).refine(
  (data) => new Date(data.start) <= new Date(data.end),
  { message: 'Start date must be before or equal to end date' }
);

export type CardinalityRequest = z.infer<typeof CardinalityRequestSchema>;

export const MetricCardinalitySchema = z.object({
  metric_name: z.string(),
  series_count: z.number(),
  percentage: z.number(),
});

export type MetricCardinality = z.infer<typeof MetricCardinalitySchema>;

export const DailyMetricsSchema = z.object({
  date: z.string(),
  metrics: z.array(MetricCardinalitySchema),
});

export type DailyMetrics = z.infer<typeof DailyMetricsSchema>;

export interface AggregatedMetric {
  metric_name: string;
  series_count: number;
  percentage: number;
}

export const CardinalityResultSchema = z.object({
  start: z.string(),
  end: z.string(),
  days: z.array(DailyMetricsSchema),
  total_series: z.number(),
  total_metrics: z.number(),
  blocks_analyzed: z.number(),
});

export type CardinalityResult = z.infer<typeof CardinalityResultSchema>;

export const LabelsCardinalityRequestSchema = z.object({
  start: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'Invalid date format (YYYY-MM-DD)'),
  end: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'Invalid date format (YYYY-MM-DD)'),
  limit: z.number().min(1).max(1000).default(10),
  metric_name: z.string().optional(),
});

export type LabelsCardinalityRequest = z.infer<typeof LabelsCardinalityRequestSchema>;

export const LabelCardinalitySchema = z.object({
  label_name: z.string(),
  unique_values: z.number(),
  percentage: z.number(),
});

export type LabelCardinality = z.infer<typeof LabelCardinalitySchema>;

export const DailyLabelsSchema = z.object({
  date: z.string(),
  labels: z.array(LabelCardinalitySchema),
});

export type DailyLabels = z.infer<typeof DailyLabelsSchema>;

export const LabelsCardinalityPerDayResultSchema = z.object({
  start: z.string(),
  end: z.string(),
  days: z.array(DailyLabelsSchema),
  total_labels: z.number(),
  total_unique_values: z.number(),
  blocks_analyzed: z.number(),
});

export type LabelsCardinalityPerDayResult = z.infer<typeof LabelsCardinalityPerDayResultSchema>;
