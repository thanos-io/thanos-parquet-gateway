import { useQuery } from '@tanstack/react-query';
import { cardinalityService } from '@/services/CardinalityService';
import { useDateRange, useLimit, useMatcherQuery } from '@/stores/useCardinalityStore';

/**
 * Convert Date to Unix timestamp in seconds.
 */
const toUnixSeconds = (date: Date): number => Math.floor(date.getTime() / 1000);

/**
 * Hook to fetch metrics cardinality per day.
 * Used for the main cardinality overview.
 */
export const useMetricsCardinality = (metricName?: string) => {
  const dateRange = useDateRange();
  const limit = useLimit();
  const matcherQuery = useMatcherQuery();

  const startTs = toUnixSeconds(dateRange.start);
  const endTs = toUnixSeconds(dateRange.end);

  return useQuery({
    queryKey: ['cardinality', 'metrics', startTs, endTs, limit, metricName, matcherQuery],
    queryFn: () =>
      cardinalityService.getMetricsCardinality(startTs, endTs, limit, metricName, matcherQuery),
    staleTime: 60_000, // 1 minute
    gcTime: 300_000, // 5 minutes
    refetchOnWindowFocus: false,
  });
};

/**
 * Hook to fetch labels cardinality per day.
 * Returns per-day data for charts and can be aggregated for tables.
 */
export const useLabelsCardinality = (metricName?: string) => {
  const dateRange = useDateRange();
  const limit = useLimit();
  const matcherQuery = useMatcherQuery();

  const startTs = toUnixSeconds(dateRange.start);
  const endTs = toUnixSeconds(dateRange.end);

  return useQuery({
    queryKey: ['cardinality', 'labels', startTs, endTs, limit, metricName, matcherQuery],
    queryFn: () =>
      cardinalityService.getLabelsCardinality(startTs, endTs, limit, metricName, matcherQuery),
    staleTime: 60_000,
    gcTime: 300_000,
    refetchOnWindowFocus: false,
  });
};

