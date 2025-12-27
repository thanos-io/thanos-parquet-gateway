import { BarChart3, Hash, Calendar, Tags, ListTree } from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';
import { formatNumber } from '@/lib/utils';
import type { CardinalityResult, LabelsCardinalityPerDayResult } from '@/types/cardinality';

interface StatsCardsProps {
  data: CardinalityResult | undefined;
  labelsData?: LabelsCardinalityPerDayResult | undefined;
  isLoading: boolean;
  isLabelsLoading?: boolean;
}

export function StatsCards({ data, labelsData, isLoading, isLabelsLoading = false }: StatsCardsProps) {
  const metricsStats = [
    {
      title: 'Total Series',
      value: data?.total_series ?? 0,
      icon: BarChart3,
      color: 'text-blue-500',
      bgColor: 'bg-blue-500/10',
      loading: isLoading,
    },
    {
      title: 'Total Metrics',
      value: data?.total_metrics ?? 0,
      icon: Hash,
      color: 'text-green-500',
      bgColor: 'bg-green-500/10',
      loading: isLoading,
    },
    {
      title: 'Blocks Analyzed',
      value: data?.blocks_analyzed ?? 0,
      icon: Calendar,
      color: 'text-purple-500',
      bgColor: 'bg-purple-500/10',
      loading: isLoading,
    },
  ];

  const labelsStats = labelsData !== undefined ? [
    {
      title: 'Total Labels',
      value: labelsData?.total_labels ?? 0,
      icon: Tags,
      color: 'text-orange-500',
      bgColor: 'bg-orange-500/10',
      loading: isLabelsLoading,
    },
    {
      title: 'Total Unique Values',
      value: labelsData?.total_unique_values ?? 0,
      icon: ListTree,
      color: 'text-cyan-500',
      bgColor: 'bg-cyan-500/10',
      loading: isLabelsLoading,
    },
  ] : [];

  const stats = [...metricsStats, ...labelsStats];

  return (
    <div className={`grid gap-4 ${stats.length > 3 ? 'md:grid-cols-5' : 'md:grid-cols-3'}`}>
      {stats.map((stat) => (
        <Card key={stat.title}>
          <CardContent className="p-4">
            <div className="flex items-center gap-4">
              <div className={`rounded-lg p-2 ${stat.bgColor}`}>
                <stat.icon className={`h-5 w-5 ${stat.color}`} />
              </div>
              <div>
                <p className="text-sm text-muted-foreground">{stat.title}</p>
                <p className="text-2xl font-bold">
                  {stat.loading ? (
                    <span className="inline-block h-7 w-16 animate-pulse rounded bg-muted" />
                  ) : (
                    formatNumber(stat.value)
                  )}
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
