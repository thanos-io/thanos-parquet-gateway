import { useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { RefreshCw } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { DateRangePicker } from '@/components/DateRangePicker';
import { LimitSelector } from '@/components/LimitSelector';
import { StatsCards } from '@/components/StatsCards';
import { CardinalityTable } from '@/components/CardinalityTable';
import { TimeSeriesChart } from '@/components/TimeSeriesChart';
import type { TimeSeriesDay } from '@/components/TimeSeriesChart';
import type { MetricCardinality, LabelCardinality, DailyMetrics, DailyLabels } from '@/types/cardinality';
import { LabelsTable } from '@/components/LabelsTable';
import { PanelErrorState } from '@/components/PanelErrorState';
import { LabelMatcherBuilder } from '@/components/LabelMatcherBuilder';
import { useMetricsCardinality, useLabelsCardinality } from '@/hooks/useCardinality';
import { aggregateMetrics, aggregateLabels } from '@/lib/utils';
import { useLimit } from '@/stores/useCardinalityStore';

export function CardinalityContainer() {
  const navigate = useNavigate();
  const limit = useLimit();
  const { data, isLoading, error, refetch, isRefetching } = useMetricsCardinality();
  const {
    data: labelsData,
    isLoading: isLabelsLoading,
    error: labelsError,
    refetch: refetchLabels,
    isRefetching: isLabelsRefetching,
  } = useLabelsCardinality();

  const aggregatedMetrics = useMemo(() => {
    if (!data?.data.days) return [];
    return aggregateMetrics(data.data.days, limit);
  }, [data?.data.days, limit]);

  const aggregatedLabels = useMemo(() => {
    if (!labelsData?.data.days) return [];
    return aggregateLabels(labelsData.data.days, limit);
  }, [labelsData?.data.days, limit]);

  const handleMetricClick = (metricName: string) => {
    navigate(`/cardinality/metric/${encodeURIComponent(metricName)}`);
  };

  const handleRefresh = () => {
    refetch();
    refetchLabels();
  };

  const isAnyRefetching = isRefetching || isLabelsRefetching;

  return (
    <div className="space-y-6">
      <LabelMatcherBuilder />

      <div className="flex flex-wrap items-center justify-between gap-4">
        <DateRangePicker />
        <div className="flex items-center gap-2">
          <LimitSelector />
          <Button
            variant="outline"
            size="icon"
            onClick={handleRefresh}
            disabled={isLoading || isLabelsLoading || isAnyRefetching}
          >
            <RefreshCw className={`h-4 w-4 ${isAnyRefetching ? 'animate-spin' : ''}`} />
          </Button>
        </div>
      </div>

      <StatsCards
        data={error ? undefined : data?.data}
        labelsData={labelsError ? undefined : labelsData?.data}
        isLoading={isLoading}
        isLabelsLoading={isLabelsLoading}
      />

      {error ? (
        <PanelErrorState
          title="Metrics Over Time"
          error={error}
          onRetry={refetch}
        />
      ) : (
        <TimeSeriesChart<MetricCardinality>
          days={(data?.data.days ?? []).map((d: DailyMetrics): TimeSeriesDay<MetricCardinality> => ({
            date: d.date,
            items: d.metrics,
          }))}
          getName={(m) => m.metric_name}
          getValue={(m) => m.series_count}
          title="Metrics Over Time"
          yAxisLabel="Series Count"
          isLoading={isLoading}
        />
      )}

      {error ? (
        <PanelErrorState
          title="Top Metrics by Series Count"
          error={error}
          onRetry={refetch}
        />
      ) : (
        <CardinalityTable
          metrics={aggregatedMetrics}
          isLoading={isLoading}
          onMetricClick={handleMetricClick}
        />
      )}

      <div className="space-y-6">
        <h2 className="text-lg font-semibold tracking-tight">Labels Cardinality</h2>

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
  );
}
