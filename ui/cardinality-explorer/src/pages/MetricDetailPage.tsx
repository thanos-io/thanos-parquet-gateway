import { useMemo } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, RefreshCw } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { CardinalityProvider } from '@/providers/Cardinality';
import { ThemeToggle } from '@/components/ThemeToggle';
import { DateRangePicker } from '@/components/DateRangePicker';
import { LimitSelector } from '@/components/LimitSelector';
import { StatsCards } from '@/components/StatsCards';
import { CardinalityTable } from '@/components/CardinalityTable';
import { TimeSeriesChart } from '@/components/TimeSeriesChart';
import type { TimeSeriesDay } from '@/components/TimeSeriesChart';
import type { MetricCardinality, LabelCardinality, DailyMetrics, DailyLabels } from '@/types/cardinality';
import { LabelsTable } from '@/components/LabelsTable';
import { PanelErrorState } from '@/components/PanelErrorState';
import { useMetricsCardinality, useLabelsCardinality } from '@/hooks/useCardinality';
import { aggregateMetrics, aggregateLabels } from '@/lib/utils';
import { useLimit } from '@/stores/useCardinalityStore';

function MetricDetailContent() {
  const { metricName } = useParams<{ metricName: string }>();
  const navigate = useNavigate();
  const limit = useLimit();
  const decodedMetricName = metricName ? decodeURIComponent(metricName) : '';

  // Use metrics cardinality with metric_name filter
  const {
    data: metricsData,
    isLoading: isMetricsLoading,
    error: metricsError,
    refetch: refetchMetrics,
    isRefetching: isMetricsRefetching,
  } = useMetricsCardinality(decodedMetricName);

  const {
    data: labelsData,
    isLoading: isLabelsLoading,
    error: labelsError,
    refetch: refetchLabels,
    isRefetching: isLabelsRefetching,
  } = useLabelsCardinality(decodedMetricName);

  // Aggregate metrics for table display
  const aggregatedMetrics = useMemo(() => {
    if (!metricsData?.data.days) return [];
    return aggregateMetrics(metricsData.data.days, limit);
  }, [metricsData?.data.days, limit]);

  // Aggregate labels for table display
  const aggregatedLabels = useMemo(() => {
    if (!labelsData?.data.days) return [];
    return aggregateLabels(labelsData.data.days, limit);
  }, [labelsData?.data.days, limit]);

  const handleRefresh = () => {
    refetchMetrics();
    refetchLabels();
  };

  const isRefetching = isMetricsRefetching || isLabelsRefetching;

  return (
    <div className="min-h-screen bg-background">
      <div className="container mx-auto py-8 px-4">
        {/* Header */}
        <div className="mb-8 flex items-start justify-between">
          <div className="flex items-start gap-4">
            <Button
              variant="ghost"
              size="icon"
              onClick={() => navigate('/cardinality')}
              className="mt-1"
            >
              <ArrowLeft className="h-4 w-4" />
            </Button>
            <div>
              <h1 className="text-3xl font-bold tracking-tight">
                <code className="font-mono">{decodedMetricName}</code>
              </h1>
              <p className="mt-2 text-muted-foreground">
                Metric cardinality history and associated labels
              </p>
            </div>
          </div>
          <ThemeToggle />
        </div>

        {/* Control Bar */}
        <div className="mb-6 flex flex-wrap items-center justify-between gap-4">
          <DateRangePicker />
          <div className="flex items-center gap-2">
            <LimitSelector />
            <Button
              variant="outline"
              size="icon"
              onClick={handleRefresh}
              disabled={isMetricsLoading || isLabelsLoading || isRefetching}
            >
              <RefreshCw className={`h-4 w-4 ${isRefetching ? 'animate-spin' : ''}`} />
            </Button>
          </div>
        </div>

        {/* Stats Cards */}
        <div className="mb-6">
          <StatsCards
            data={metricsError ? undefined : metricsData?.data}
            labelsData={labelsError ? undefined : labelsData?.data}
            isLoading={isMetricsLoading}
            isLabelsLoading={isLabelsLoading}
          />
        </div>

        {/* Content - each panel handles its own error state */}
        <div className="space-y-6">
          {/* Metric Series Over Time - same component as CardinalityPage */}
          {metricsError ? (
            <PanelErrorState
              title="Metric Series Over Time"
              error={metricsError}
              onRetry={refetchMetrics}
            />
          ) : (
            <TimeSeriesChart<MetricCardinality>
              days={(metricsData?.data.days ?? []).map((d: DailyMetrics): TimeSeriesDay<MetricCardinality> => ({
                date: d.date,
                items: d.metrics,
              }))}
              getName={(m) => m.metric_name}
              getValue={(m) => m.series_count}
              title="Metric Series Over Time"
              yAxisLabel="Series Count"
              isLoading={isMetricsLoading}
            />
          )}

          {/* Cardinality Table - same component as CardinalityPage */}
          {metricsError ? (
            <PanelErrorState
              title="Metric Cardinality"
              error={metricsError}
              onRetry={refetchMetrics}
            />
          ) : (
            <CardinalityTable
              metrics={aggregatedMetrics}
              isLoading={isMetricsLoading}
              onMetricClick={() => {}} // Already on detail page, no-op
            />
          )}

          {/* Labels Cardinality Section - same structure as CardinalityPage */}
          <div className="space-y-6">
            <h2 className="text-lg font-semibold tracking-tight">Labels for this Metric</h2>

            {/* Labels Over Time - same component as CardinalityPage */}
            {labelsError ? (
              <PanelErrorState
                title="Labels Over Time (by Unique Values)"
                error={labelsError}
                onRetry={refetchLabels}
              />
            ) : (
              <TimeSeriesChart<LabelCardinality>
                days={(labelsData?.data.days ?? []).map((d: DailyLabels): TimeSeriesDay<LabelCardinality> => ({
                  date: d.date,
                  items: d.labels,
                }))}
                getName={(l) => l.label_name}
                getValue={(l) => l.unique_values}
                title="Labels Over Time (by Unique Values)"
                yAxisLabel="Unique Values"
                isLoading={isLabelsLoading}
              />
            )}

            {/* Labels Table - same component as CardinalityPage */}
            {labelsError ? (
              <PanelErrorState
                title="Top Labels by Unique Values"
                error={labelsError}
                onRetry={refetchLabels}
              />
            ) : (
              <LabelsTable
                labels={aggregatedLabels}
                isLoading={isLabelsLoading}
              />
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export function MetricDetailPage() {
  return (
    <CardinalityProvider>
      <MetricDetailContent />
    </CardinalityProvider>
  );
}
