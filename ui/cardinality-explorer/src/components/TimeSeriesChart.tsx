import { useRef, useEffect, useMemo } from 'react';
import uPlot from 'uplot';
import 'uplot/dist/uPlot.min.css';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';

const chartStyles = `
  .u-tooltip {
    position: absolute;
    z-index: 100;
    pointer-events: none;
    font-size: 12px;
    font-family: ui-monospace, monospace;
    line-height: 1.5;
    padding: 8px 10px;
    background: var(--color-popover);
    color: var(--color-popover-foreground);
    border: 1px solid var(--color-border);
    border-radius: 6px;
    box-shadow: 0 4px 12px rgb(0 0 0 / 0.15);
  }
  .u-tooltip-date {
    font-size: 11px;
    color: var(--color-muted-foreground);
    margin-bottom: 6px;
    font-family: system-ui, sans-serif;
  }
  .u-tooltip-item {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 2px 0;
  }
  .u-tooltip-marker {
    width: 10px;
    height: 3px;
    border-radius: 1px;
    flex-shrink: 0;
  }
  .u-tooltip-label {
    flex: 1;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 200px;
  }
  .u-tooltip-value {
    font-weight: 600;
    font-variant-numeric: tabular-nums;
    color: var(--color-foreground);
  }
`;

function tooltipPlugin(formatValue: (v: number) => string): uPlot.Plugin {
  let tooltip: HTMLDivElement | null = null;
  let seriesColors: string[] = [];
  let seriesLabels: string[] = [];
  let seriesIdx: number | null = null;
  let dataIdx: number | null = null;

  function hideTooltip() {
    if (tooltip) {
      tooltip.style.display = 'none';
    }
  }

  function showTooltip(u: uPlot) {
    if (!tooltip || seriesIdx == null || dataIdx == null) {
      hideTooltip();
      return;
    }

    const xVal = u.data[0][dataIdx];
    const focusedVal = u.data[seriesIdx]?.[dataIdx];
    
    if (xVal == null || focusedVal == null) {
      hideTooltip();
      return;
    }

    const date = new Date(xVal * 1000);
    const dateStr = date.toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
    });

    const matchingSeries: { color: string; label: string; value: number }[] = [];
    for (let i = 1; i < u.data.length; i++) {
      const val = u.data[i]?.[dataIdx];
      if (val != null && val === focusedVal) {
        matchingSeries.push({
          color: seriesColors[i - 1] ?? '#888',
          label: seriesLabels[i - 1] ?? '',
          value: val,
        });
      }
    }

    const itemsHtml = matchingSeries.map(s => `
      <div class="u-tooltip-item">
        <span class="u-tooltip-marker" style="background:${s.color}"></span>
        <span class="u-tooltip-label">${s.label}</span>
        <span class="u-tooltip-value">${formatValue(s.value)}</span>
      </div>
    `).join('');

    tooltip.innerHTML = `
      <div class="u-tooltip-date">${dateStr}</div>
      ${itemsHtml}
    `;
    tooltip.style.display = 'block';

    const xPos = u.valToPos(xVal, 'x');
    const yPos = u.valToPos(focusedVal, 'y');
    const overRect = u.over.getBoundingClientRect();
    const tooltipRect = tooltip.getBoundingClientRect();

    let x = xPos + 12;
    let y = yPos - tooltipRect.height / 2;

    if (x + tooltipRect.width > overRect.width) {
      x = xPos - tooltipRect.width - 12;
    }
    if (y + tooltipRect.height > overRect.height) {
      y = overRect.height - tooltipRect.height - 4;
    }
    if (y < 4) y = 4;

    tooltip.style.left = x + 'px';
    tooltip.style.top = y + 'px';
  }

  function init(u: uPlot) {
    tooltip = document.createElement('div');
    tooltip.className = 'u-tooltip';
    tooltip.style.display = 'none';
    u.over.appendChild(tooltip);

    seriesColors = u.series.slice(1).map((s) => {
      const stroke = s.stroke;
      if (typeof stroke === 'function') return '#888';
      return String(stroke ?? '#888');
    });
    seriesLabels = u.series.slice(1).map((s) => {
      const label = s.label;
      if (typeof label === 'string') return label;
      return '';
    });
  }

  return {
    hooks: {
      init,
      setSeries: [
        (u: uPlot, sidx: number | null) => {
          if (seriesIdx !== sidx) {
            seriesIdx = sidx;
            if (sidx == null) {
              hideTooltip();
            } else if (dataIdx != null) {
              showTooltip(u);
            }
          }
        },
      ],
      setCursor: [
        (u: uPlot) => {
          const idx = u.cursor.idx;
          if (dataIdx !== idx) {
            dataIdx = idx ?? null;
            if (idx == null) {
              hideTooltip();
            } else if (seriesIdx != null) {
              showTooltip(u);
            }
          }
        },
      ],
    },
  };
}

export interface TimeSeriesDay<T> {
  date: string;
  items: T[];
}

export interface TimeSeriesChartProps<T> {
  days: TimeSeriesDay<T>[];
  getName: (item: T) => string;
  getValue: (item: T) => number;
  title: string;
  yAxisLabel: string;
  isLoading: boolean;
}

const COLORS = [
  '#3b82f6',
  '#ef4444',
  '#22c55e',
  '#f59e0b',
  '#8b5cf6',
  '#ec4899',
  '#06b6d4',
  '#f97316',
  '#84cc16',
  '#6366f1',
];

function formatNumber(num: number): string {
  if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`;
  if (num >= 1_000) return `${(num / 1_000).toFixed(1)}K`;
  return num.toLocaleString();
}

export function TimeSeriesChart<T>({ 
  days,
  getName,
  getValue,
  title,
  yAxisLabel,
  isLoading,
}: TimeSeriesChartProps<T>) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<uPlot | null>(null);

  const { data, seriesConfig, itemNames } = useMemo(() => {
    if (!days.length) {
      return { data: [[]] as uPlot.AlignedData, seriesConfig: [{}], itemNames: [] };
    }

    // Build a map of date -> name -> value
    const byDate = new Map<string, Map<string, number>>();
    
    for (const day of days) {
      const dateItems = new Map<string, number>();
      for (const item of day.items) {
        dateItems.set(getName(item), getValue(item));
      }
      byDate.set(day.date, dateItems);
    }

    // Calculate total value per name across all dates for ranking
    const nameTotals = new Map<string, number>();
    for (const [, dateItems] of byDate) {
      for (const [name, value] of dateItems) {
        nameTotals.set(name, (nameTotals.get(name) ?? 0) + value);
      }
    }

    // Get all items sorted by total value (descending)
    const allItems = Array.from(nameTotals.entries())
      .sort((a, b) => b[1] - a[1])
      .map(([name]) => name);

    // Sort dates chronologically
    const sortedDates = Array.from(byDate.keys()).sort();

    // Build uPlot data arrays
    // First array is timestamps (x-axis)
    // Parse date strings as local time (not UTC) to avoid timezone offset issues
    const timestamps: number[] = sortedDates.map(date => {
      const [year, month, day] = date.split('-').map(Number);
      const d = new Date(year, month - 1, day); // Local midnight
      return d.getTime() / 1000; // uPlot uses seconds
    });

    // Create series arrays for all items
    const seriesArrays: (number | null)[][] = allItems.map(itemName => {
      return sortedDates.map(date => {
        const dateItems = byDate.get(date);
        return dateItems?.get(itemName) ?? null;
      });
    });

    // uPlot data format: [timestamps, series1, series2, ...]
    const plotData: uPlot.AlignedData = [timestamps, ...seriesArrays];

    // Series configuration
    const series: uPlot.Series[] = [
      {}, // x-axis series (time)
      ...allItems.map((name, i) => ({
        label: name.length > 30 ? name.slice(0, 27) + '...' : name,
        stroke: COLORS[i % COLORS.length],
        width: 2,
      })),
    ];

    return { data: plotData, seriesConfig: series, itemNames: allItems };
  }, [days, getName, getValue]);

  // Create/update chart
  useEffect(() => {
    if (!containerRef.current || isLoading || !data[0]?.length) {
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
          range: (_u, _dataMin, dataMax) => {
            // Add 10% padding to top
            return [0, dataMax * 1.1];
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
          label: yAxisLabel,
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
        live: false,
        isolate: true,
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
      focus: {
        alpha: 0.3,
      },
      plugins: [
        tooltipPlugin(formatNumber),
      ],
    };

    chartRef.current = new uPlot(opts, data, container);

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
  }, [data, seriesConfig, yAxisLabel, isLoading]);

  if (isLoading) {
    return (
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base font-medium">
            {title}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-[300px] w-full animate-pulse rounded bg-muted" />
        </CardContent>
      </Card>
    );
  }

  if (!days.length || !itemNames.length) {
    return null;
  }

  return (
    <Card>
      <style>{chartStyles}</style>
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-medium">
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div ref={containerRef} className="w-full" />
      </CardContent>
    </Card>
  );
}
