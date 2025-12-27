import { useRef, useEffect, useMemo } from 'react';
import uPlot from 'uplot';
import 'uplot/dist/uPlot.min.css';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { formatNumber } from '@/lib/utils';
import type { LabelCardinality } from '@/types/cardinality';

interface LabelsBarChartProps {
  labels: LabelCardinality[];
  isLoading: boolean;
}

const COLORS = [
  '#3b82f6', // blue
  '#22c55e', // green
  '#8b5cf6', // purple
  '#f97316', // orange
  '#ec4899', // pink
  '#06b6d4', // cyan
  '#eab308', // yellow
  '#a855f7', // violet
  '#14b8a6', // teal
  '#ef4444', // red
];

export function LabelsBarChart({ labels, isLoading }: LabelsBarChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<uPlot | null>(null);

  // Transform data for horizontal bar chart (using uPlot bands/bars plugin approach)
  const { plotData, seriesConfig, yLabels } = useMemo(() => {
    if (!labels.length) {
      return { plotData: [[]] as uPlot.AlignedData, seriesConfig: [{}], yLabels: [] };
    }

    // Reverse to show highest at top
    const sorted = [...labels].reverse();
    const yLabels = sorted.map(l => l.label_name);
    
    // X-axis: bar positions (0, 1, 2, ...)
    const positions: number[] = sorted.map((_, i) => i);
    
    // Y-axis: values
    const values: number[] = sorted.map(l => l.unique_values);

    const plotData: uPlot.AlignedData = [positions, values];

    const seriesConfig: uPlot.Series[] = [
      {}, // x-axis placeholder
      {
        label: 'Unique Values',
        stroke: COLORS[0],
        fill: COLORS[0],
        width: 0,
        paths: barsBuilder(0.6),
        points: { show: false },
        value: (_: uPlot, rawValue: number | null) =>
          rawValue == null ? '-' : formatNumber(rawValue),
      },
    ];

    return { plotData, seriesConfig, yLabels };
  }, [labels]);

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
    const barCount = yLabels.length;
    const barHeight = 28;
    const height = Math.max(200, barCount * barHeight + 80);

    const opts: uPlot.Options = {
      width,
      height,
      scales: {
        x: {
          time: false,
          range: [-0.5, barCount - 0.5],
        },
        y: {
          range: (_u, _dataMin, dataMax) => [0, dataMax * 1.1],
        },
      },
      axes: [
        {
          label: 'Label Name',
          labelSize: 20,
          labelFont: 'bold 12px system-ui, sans-serif',
          labelGap: 8,
          font: '12px system-ui, sans-serif',
          stroke: 'rgba(128, 128, 128, 0.8)',
          grid: { show: false },
          ticks: { show: false },
          side: 2,
          size: 100,
          values: (_, splits) => splits.map(v => {
            const idx = Math.round(v);
            const name = yLabels[idx] ?? '';
            return name.length > 12 ? name.slice(0, 10) + '...' : name;
          }),
          gap: 5,
        },
        {
          label: 'Unique Values',
          labelSize: 20,
          labelFont: 'bold 12px system-ui, sans-serif',
          labelGap: 8,
          font: '12px system-ui, sans-serif',
          stroke: 'rgba(128, 128, 128, 0.8)',
          grid: { show: true, stroke: 'rgba(128, 128, 128, 0.1)' },
          ticks: { stroke: 'rgba(128, 128, 128, 0.3)', size: 5 },
          size: 60,
          values: (_, splits) => splits.map(v => formatNumber(v)),
        },
      ],
      series: seriesConfig,
      legend: {
        show: false,
      },
      cursor: {
        y: false,
        points: {
          show: false,
        },
      },
      hooks: {
        drawSeries: [
          (u: uPlot, seriesIdx: number) => {
            if (seriesIdx !== 1) return;
            // Draw colored bars
            const ctx = u.ctx;
            const data = u.data;
            const xScale = u.scales.x;
            const yScale = u.scales.y;

            if (!xScale || !yScale) return;

            const barWidth = Math.abs(u.valToPos(0.3, 'x') - u.valToPos(0, 'x'));

            for (let i = 0; i < (data[0]?.length ?? 0); i++) {
              const x = data[0]?.[i];
              const y = data[1]?.[i];
              if (x == null || y == null) continue;

              const xPos = u.valToPos(x, 'x');
              const yPos = u.valToPos(y, 'y');
              const y0Pos = u.valToPos(0, 'y');

              ctx.fillStyle = COLORS[i % COLORS.length];
              ctx.fillRect(xPos - barWidth / 2, yPos, barWidth, y0Pos - yPos);
            }
          },
        ],
      },
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
  }, [plotData, seriesConfig, yLabels, isLoading]);

  if (isLoading) {
    return (
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base font-medium">Labels by Unique Values</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-[300px] w-full animate-pulse rounded bg-muted" />
        </CardContent>
      </Card>
    );
  }

  if (!labels.length) {
    return null;
  }

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-medium">Top {labels.length} Labels (by Unique Values)</CardTitle>
      </CardHeader>
      <CardContent>
        <div ref={containerRef} className="w-full" />
      </CardContent>
    </Card>
  );
}

// Custom bar path builder for uPlot
function barsBuilder(barWidthFactor: number): uPlot.Series.PathBuilder {
  return (u: uPlot, seriesIdx: number) => {
    const series = u.series[seriesIdx];
    if (!series) return null;

    const xData = u.data[0];
    const yData = u.data[seriesIdx];
    if (!xData || !yData) return null;

    const barWidth = Math.abs(u.valToPos(barWidthFactor / 2, 'x') - u.valToPos(0, 'x')) * 2;

    const path = new Path2D();

    for (let i = 0; i < xData.length; i++) {
      const x = xData[i];
      const y = yData[i];
      if (x == null || y == null) continue;

      const xPos = u.valToPos(x, 'x');
      const yPos = u.valToPos(y, 'y');
      const y0Pos = u.valToPos(0, 'y');

      path.rect(xPos - barWidth / 2, yPos, barWidth, y0Pos - yPos);
    }

    return {
      stroke: path,
      fill: path,
    };
  };
}
