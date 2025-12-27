import { useState } from 'react';
import { ArrowUpDown, ArrowUp, ArrowDown, TrendingUp } from 'lucide-react';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Button } from '@/components/ui/button';
import { formatNumber, formatPercentage } from '@/lib/utils';
import type { AggregatedMetric } from '@/types/cardinality';

interface CardinalityTableProps {
  metrics: AggregatedMetric[];
  isLoading: boolean;
  onMetricClick?: (metricName: string) => void;
}

type SortField = 'metric_name' | 'series_count' | 'percentage';
type SortDirection = 'asc' | 'desc';

export function CardinalityTable({ metrics, isLoading, onMetricClick }: CardinalityTableProps) {
  const [sortField, setSortField] = useState<SortField>('series_count');
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc');

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection('desc');
    }
  };

  const sortedMetrics = [...metrics].sort((a, b) => {
    const multiplier = sortDirection === 'asc' ? 1 : -1;
    if (sortField === 'metric_name') {
      return multiplier * a.metric_name.localeCompare(b.metric_name);
    }
    return multiplier * (a[sortField] - b[sortField]);
  });

  const SortIcon = ({ field }: { field: SortField }) => {
    if (sortField !== field) {
      return <ArrowUpDown className="ml-2 h-4 w-4" />;
    }
    return sortDirection === 'asc' ? (
      <ArrowUp className="ml-2 h-4 w-4" />
    ) : (
      <ArrowDown className="ml-2 h-4 w-4" />
    );
  };

  if (isLoading) {
    return (
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-[50px]">#</TableHead>
              <TableHead>Metric Name</TableHead>
              <TableHead className="w-[150px] text-right">Series Count</TableHead>
              <TableHead className="w-[200px]">Share</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {Array.from({ length: 10 }).map((_, i) => (
              <TableRow key={i}>
                <TableCell>
                  <span className="inline-block h-4 w-6 animate-pulse rounded bg-muted" />
                </TableCell>
                <TableCell>
                  <span className="inline-block h-4 w-48 animate-pulse rounded bg-muted" />
                </TableCell>
                <TableCell className="text-right">
                  <span className="inline-block h-4 w-16 animate-pulse rounded bg-muted" />
                </TableCell>
                <TableCell>
                  <span className="inline-block h-4 w-full animate-pulse rounded bg-muted" />
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    );
  }

  if (metrics.length === 0) {
    return (
      <div className="flex h-[400px] items-center justify-center rounded-md border">
        <div className="text-center">
          <TrendingUp className="mx-auto h-12 w-12 text-muted-foreground/50" />
          <p className="mt-4 text-lg font-medium">No metrics found</p>
          <p className="text-sm text-muted-foreground">
            Try adjusting your date range or filters
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="w-[50px]">#</TableHead>
            <TableHead>
              <Button
                variant="ghost"
                onClick={() => handleSort('metric_name')}
                className="-ml-4 h-8 hover:bg-transparent"
              >
                Metric Name
                <SortIcon field="metric_name" />
              </Button>
            </TableHead>
            <TableHead className="w-[150px] text-right">
              <Button
                variant="ghost"
                onClick={() => handleSort('series_count')}
                className="-mr-4 h-8 hover:bg-transparent"
              >
                Series Count
                <SortIcon field="series_count" />
              </Button>
            </TableHead>
            <TableHead className="w-[200px]">
              <Button
                variant="ghost"
                onClick={() => handleSort('percentage')}
                className="-ml-4 h-8 hover:bg-transparent"
              >
                Share
                <SortIcon field="percentage" />
              </Button>
            </TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {sortedMetrics.map((metric, index) => (
            <TableRow
              key={metric.metric_name}
              className={onMetricClick ? 'cursor-pointer' : ''}
              onClick={() => onMetricClick?.(metric.metric_name)}
            >
              <TableCell className="font-medium text-muted-foreground">{index + 1}</TableCell>
              <TableCell>
                <code className="text-sm font-mono">{metric.metric_name}</code>
              </TableCell>
              <TableCell className="text-right font-mono">
                {formatNumber(metric.series_count)}
              </TableCell>
              <TableCell>
                <div className="flex items-center gap-2">
                  <div className="h-2 flex-1 overflow-hidden rounded-full bg-secondary">
                    <div
                      className="h-full bg-primary transition-all"
                      style={{ width: `${Math.min(metric.percentage, 100)}%` }}
                    />
                  </div>
                  <span className="w-12 text-right text-xs text-muted-foreground">
                    {formatPercentage(metric.percentage)}
                  </span>
                </div>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}
