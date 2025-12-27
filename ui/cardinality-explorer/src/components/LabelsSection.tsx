import { LabelsBarChart } from '@/components/LabelsBarChart';
import { LabelsTable } from '@/components/LabelsTable';
import type { LabelCardinality } from '@/types/cardinality';

interface LabelsSectionProps {
  labels: LabelCardinality[];
  isLoading: boolean;
  title?: string;
}

export function LabelsSection({ labels, isLoading, title }: LabelsSectionProps) {
  return (
    <div className="space-y-6">
      {title && (
        <h2 className="text-lg font-semibold tracking-tight">{title}</h2>
      )}
      <LabelsBarChart
        labels={labels}
        isLoading={isLoading}
      />
      <LabelsTable
        labels={labels}
        isLoading={isLoading}
      />
    </div>
  );
}
