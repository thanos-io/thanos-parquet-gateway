import { useRef, useEffect, useMemo } from 'react';
import uPlot from 'uplot';
import 'uplot/dist/uPlot.min.css';
import { X, TrendingUp, TrendingDown, Minus } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { formatNumber } from '@/lib/utils';
// Simplified metric history data for display
export interface MetricHistoryData {
  metric_name: string;
  history: { date: string; series_count: number }[];
  average: number;
  min: number;
  max: number;
}

// Tooltip styling
const tooltipStyles = `
  .u-tooltip {
    position: absolute;
    z-index: 100;
    pointer-events: none;
    padding: 8px 12px;
    border-radius: 6px;
    font-size: 12px;
    background: hsl(var(--popover));
    color: hsl(var(--popover-foreground));
    border: 1px solid hsl(var(--border));
    box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1);
    white-space: nowrap;
  }
  .u-tooltip .tooltip-date {
    font-weight: 500;
    margin-bottom: 4px;
    color: hsl(var(--muted-foreground));
  }
  .u-tooltip .tooltip-value {
    font-weight: 600;
    font-variant-numeric: tabular-nums;
  }
`;

// Simple tooltip plugin for single-series chart
function tooltipPlugin(formatValue: (v: number) => string): uPlot.Plugin {
  let tooltip: HTMLDivElement | null = null;

  function init(u: uPlot) {
    tooltip = document.createElement('div');
    tooltip.className = 'u-tooltip';
    tooltip.style.display = 'none';
    u.over.appendChild(tooltip);
  }

  function setCursor(u: uPlot) {
    if (!tooltip) return;

    const { idx } = u.cursor;

    if (idx == null) {
      tooltip.style.display = 'none';
      return;
    }

    const xVal = u.data[0][idx];
    const yVal = u.data[1]?.[idx];
    if (xVal == null || yVal == null) {
      tooltip.style.display = 'none';
      return;
    }

    // Format date
    const date = new Date(xVal * 1000);
    const dateStr = date.toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
    });

    tooltip.innerHTML = `
      <div class="tooltip-date">${dateStr}</div>
      <div class="tooltip-value">Series Count: ${formatValue(yVal)}</div>
    `;
    tooltip.style.display = 'block';

    // Position tooltip near cursor but keep in bounds
    const { left, top } = u.cursor;
    if (left == null || top == null) return;

    const overRect = u.over.getBoundingClientRect();
    const tooltipRect = tooltip.getBoundingClientRect();

    let x = left + 15;
    let y = top - 10;

    // Keep tooltip in bounds
    if (x + tooltipRect.width > overRect.width) {
      x = left - tooltipRect.width - 15;
    }
    if (y + tooltipRect.height > overRect.height) {
      y = overRect.height - tooltipRect.height - 5;
    }
    if (y < 0) y = 5;

    tooltip.style.left = x + 'px';
    tooltip.style.top = y + 'px';
  }

  return {
    hooks: {
      init,
      setCursor,
    },
  };
}

interface MetricHistoryChartProps {
  data: MetricHistoryData | undefined;
  isLoading: boolean;
  onClose: () => void;
}

type TrendType = 'increasing' | 'stable' | 'decreasing';

// Calculate trend based on first and last values
function calculateTrend(history: { series_count: number }[]): TrendType {
  if (history.length < 2) return 'stable';
  const first = history[0].series_count;
  const last = history[history.length - 1].series_count;
  const change = (last - first) / Math.max(first, 1);
  if (change > 0.1) return 'increasing';
  if (change < -0.1) return 'decreasing';
  return 'stable';
}

const TrendIcon = ({ trend }: { trend: TrendType }) => {
  switch (trend) {
    case 'increasing':
      return <TrendingUp className="h-4 w-4" />;
    case 'decreasing':
      return <TrendingDown className="h-4 w-4" />;
    default:
      return <Minus className="h-4 w-4" />;
  }
};

const trendVariant = (trend: TrendType) => {
  switch (trend) {
    case 'increasing':
      return 'destructive' as const;
    case 'decreasing':
      return 'success' as const;
    default:
      return 'secondary' as const;
  }
};

export function MetricHistoryChart({ data, isLoading, onClose }: MetricHistoryChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<uPlot | null>(null);

  // Transform data into uPlot format
  const { plotData, seriesConfig } = useMemo(() => {
    if (!data?.history.length) {
      return { plotData: [[]] as uPlot.AlignedData, seriesConfig: [{}] };
    }

    // Sort by date
    const sorted = [...data.history].sort((a, b) => a.date.localeCompare(b.date));

    // Timestamps (x-axis) - uPlot uses seconds
    const timestamps: number[] = sorted.map(item => new Date(item.date).getTime() / 1000);

    // Series values
    const values: (number | null)[] = sorted.map(item => item.series_count);

    const plotData: uPlot.AlignedData = [timestamps, values];

    const seriesConfig: uPlot.Series[] = [
      {}, // x-axis placeholder
      {
        label: 'Series Count',
        stroke: 'hsl(var(--primary))',
        width: 2,
        fill: 'hsla(var(--primary), 0.1)',
        value: (_: uPlot, rawValue: number | null) =>
          rawValue == null ? '-' : formatNumber(rawValue),
      },
    ];

    return { plotData, seriesConfig };
  }, [data]);

  // Create/update chart
  useEffect(() => {
    if (!containerRef.current || isLoading || !plotData[0]?.length) {
      return;
    }

    // Destroy previous chart instance
    if (chartRef.current) {
      chartRef.current.destroy();
      chartRef.current = null;
    }

    const container = containerRef.current;
    const width = container.clientWidth;
    const height = 300;

    const opts: uPlot.Options = {
      width,
      height,
      scales: {
        x: {
          time: true,
        },
        y: {
          range: (_u, dataMin, dataMax) => {
            const padding = (dataMax - dataMin) * 0.1 || dataMax * 0.1;
            return [Math.max(0, dataMin - padding), dataMax + padding];
          },
        },
      },
      axes: [
        {
          label: 'Date',
          labelSize: 20,
          labelFont: 'bold 12px system-ui, sans-serif',
          labelGap: 8,
          font: '12px system-ui, sans-serif',
          stroke: 'rgba(128, 128, 128, 0.8)',
          grid: { show: true, stroke: 'rgba(128, 128, 128, 0.1)' },
          ticks: { stroke: 'rgba(128, 128, 128, 0.3)', size: 5 },
        },
        {
          label: 'Series Count',
          labelSize: 20,
          labelFont: 'bold 12px system-ui, sans-serif',
          labelGap: 8,
          font: '12px system-ui, sans-serif',
          stroke: 'rgba(128, 128, 128, 0.8)',
          grid: { show: true, stroke: 'rgba(128, 128, 128, 0.1)' },
          ticks: { stroke: 'rgba(128, 128, 128, 0.3)', size: 5 },
          size: 80,
          values: (_, splits) => splits.map(v => formatNumber(v)),
        },
      ],
      series: seriesConfig,
      legend: {
        show: false,
      },
      cursor: {
        drag: {
          x: true,
          y: false,
        },
        focus: {
          prox: 30,
        },
      },
      plugins: [
        tooltipPlugin(formatNumber),
      ],
    };

    chartRef.current = new uPlot(opts, plotData, container);

    // Handle resize
    const resizeObserver = new ResizeObserver(entries => {
      for (const entry of entries) {
        if (chartRef.current && entry.contentRect.width > 0) {
          chartRef.current.setSize({
            width: entry.contentRect.width,
            height
          });
        }
      }
    });
    resizeObserver.observe(container);

    return () => {
      resizeObserver.disconnect();
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
    };
  }, [plotData, seriesConfig, isLoading]);

  if (isLoading) {
    return (
      <Card>
        <CardHeader className="flex flex-row items-center justify-between pb-2">
          <div className="h-6 w-48 animate-pulse rounded bg-muted" />
          <Button variant="ghost" size="icon" onClick={onClose}>
            <X className="h-4 w-4" />
          </Button>
        </CardHeader>
        <CardContent>
          <div className="h-[300px] animate-pulse rounded bg-muted" />
        </CardContent>
      </Card>
    );
  }

  if (!data) {
    return null;
  }

  const trend = calculateTrend(data.history);

  return (
    <Card>
      <style>{tooltipStyles}</style>
      <CardHeader className="flex flex-row items-center justify-between pb-2">
        <div className="space-y-1">
          <CardTitle className="flex items-center gap-2">
            <code className="text-sm font-mono">{data.metric_name}</code>
            <Badge variant={trendVariant(trend)} className="gap-1">
              <TrendIcon trend={trend} />
              {trend}
            </Badge>
          </CardTitle>
          <div className="flex gap-4 text-sm text-muted-foreground">
            <span>Avg: {formatNumber(data.average)}</span>
            <span>Min: {formatNumber(data.min)}</span>
            <span>Max: {formatNumber(data.max)}</span>
          </div>
        </div>
        <Button variant="ghost" size="icon" onClick={onClose}>
          <X className="h-4 w-4" />
        </Button>
      </CardHeader>
      <CardContent>
        <div ref={containerRef} className="w-full" />
      </CardContent>
    </Card>
  );
}
