import { useState } from 'react';
import { ArrowUpDown, ArrowUp, ArrowDown, Tags } from 'lucide-react';
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
import type { LabelCardinality } from '@/types/cardinality';

interface LabelsTableProps {
  labels: LabelCardinality[];
  isLoading: boolean;
}

type SortField = 'label_name' | 'unique_values' | 'percentage';
type SortDirection = 'asc' | 'desc';

function getSortIcon(currentField: SortField, sortField: SortField, sortDirection: SortDirection) {
  if (sortField !== currentField) {
    return <ArrowUpDown className="ml-2 h-4 w-4" />;
  }
  return sortDirection === 'asc' ? (
    <ArrowUp className="ml-2 h-4 w-4" />
  ) : (
    <ArrowDown className="ml-2 h-4 w-4" />
  );
}

export function LabelsTable({ labels, isLoading }: LabelsTableProps) {
  const [sortField, setSortField] = useState<SortField>('unique_values');
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc');

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection('desc');
    }
  };

  const sortedLabels = [...labels].sort((a, b) => {
    const multiplier = sortDirection === 'asc' ? 1 : -1;
    if (sortField === 'label_name') {
      return multiplier * a.label_name.localeCompare(b.label_name);
    }
    return multiplier * (a[sortField] - b[sortField]);
  });

  if (isLoading) {
    return (
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-[50px]">#</TableHead>
              <TableHead>Label Name</TableHead>
              <TableHead className="w-[150px] text-right">Unique Values</TableHead>
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
                  <span className="inline-block h-4 w-32 animate-pulse rounded bg-muted" />
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

  if (labels.length === 0) {
    return (
      <div className="flex h-[400px] items-center justify-center rounded-md border">
        <div className="text-center">
          <Tags className="mx-auto h-12 w-12 text-muted-foreground/50" />
          <p className="mt-4 text-lg font-medium">No labels found</p>
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
                onClick={() => handleSort('label_name')}
                className="-ml-4 h-8 hover:bg-transparent"
              >
                Label Name
                {getSortIcon('label_name', sortField, sortDirection)}
              </Button>
            </TableHead>
            <TableHead className="w-[150px] text-right">
              <Button
                variant="ghost"
                onClick={() => handleSort('unique_values')}
                className="-mr-4 h-8 hover:bg-transparent"
              >
                Unique Values
                {getSortIcon('unique_values', sortField, sortDirection)}
              </Button>
            </TableHead>
            <TableHead className="w-[200px]">
              <Button
                variant="ghost"
                onClick={() => handleSort('percentage')}
                className="-ml-4 h-8 hover:bg-transparent"
              >
                Share
                {getSortIcon('percentage', sortField, sortDirection)}
              </Button>
            </TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {sortedLabels.map((label, index) => (
            <TableRow key={label.label_name}>
              <TableCell className="font-medium text-muted-foreground">{index + 1}</TableCell>
              <TableCell>
                <code className="text-sm font-mono">{label.label_name}</code>
              </TableCell>
              <TableCell className="text-right font-mono">
                {formatNumber(label.unique_values)}
              </TableCell>
              <TableCell>
                <div className="flex items-center gap-2">
                  <div className="h-2 flex-1 overflow-hidden rounded-full bg-secondary">
                    <div
                      className="h-full bg-primary transition-all"
                      style={{ width: `${Math.min(label.percentage, 100)}%` }}
                    />
                  </div>
                  <span className="w-12 text-right text-xs text-muted-foreground">
                    {formatPercentage(label.percentage)}
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
