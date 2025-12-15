import * as React from 'react';
import { format, subDays, startOfWeek, startOfMonth } from 'date-fns';
import { CalendarIcon } from 'lucide-react';
import { type DateRange } from 'react-day-picker';
import { Button } from '@/components/ui/button';
import { Calendar } from '@/components/ui/calendar';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { useDateRange, useCardinalityActions } from '@/stores/useCardinalityStore';
import { cn } from '@/lib/utils';

type DateRangePreset = 'last7days' | 'last14days' | 'last30days' | 'thisWeek' | 'thisMonth';

const presets: { label: string; value: DateRangePreset }[] = [
  { label: 'Last 7 days', value: 'last7days' },
  { label: 'Last 14 days', value: 'last14days' },
  { label: 'Last 30 days', value: 'last30days' },
  { label: 'This week', value: 'thisWeek' },
  { label: 'This month', value: 'thisMonth' },
];

function getPresetRange(preset: DateRangePreset): { start: Date; end: Date } {
  const today = new Date();
  switch (preset) {
    case 'last7days':
      return { start: subDays(today, 7), end: today };
    case 'last14days':
      return { start: subDays(today, 14), end: today };
    case 'last30days':
      return { start: subDays(today, 30), end: today };
    case 'thisWeek':
      return { start: startOfWeek(today, { weekStartsOn: 1 }), end: today };
    case 'thisMonth':
      return { start: startOfMonth(today), end: today };
  }
}

export function DateRangePicker() {
  const dateRange = useDateRange();
  const { updateDateRange } = useCardinalityActions();
  const [open, setOpen] = React.useState(false);
  
  // Local state for selection - only commits to store when popover closes
  const [localRange, setLocalRange] = React.useState<DateRange>({
    from: dateRange.start,
    to: dateRange.end,
  });

  // Sync local state when store changes (e.g., from other sources)
  React.useEffect(() => {
    if (!open) {
      setLocalRange({
        from: dateRange.start,
        to: dateRange.end,
      });
    }
  }, [dateRange.start, dateRange.end, open]);

  const handleOpenChange = (isOpen: boolean) => {
    if (!isOpen && localRange.from && localRange.to) {
      // Popover closing - commit the selection to store (triggers API call)
      updateDateRange(localRange.from, localRange.to);
    }
    if (isOpen) {
      // Reset local state to current store state when opening
      setLocalRange({
        from: dateRange.start,
        to: dateRange.end,
      });
    }
    setOpen(isOpen);
  };

  const handleSelect = (range: DateRange | undefined) => {
    // Only update local state, don't trigger store/API
    if (range?.from) {
      setLocalRange({
        from: range.from,
        to: range.to ?? range.from,
      });
    }
  };

  const handlePresetClick = (preset: DateRangePreset) => {
    const { start, end } = getPresetRange(preset);
    // Commit directly to store and close
    updateDateRange(start, end);
    setOpen(false);
  };

  // Display uses local range when open, store range when closed
  const displayRange = open ? localRange : { from: dateRange.start, to: dateRange.end };

  return (
    <div className="flex flex-wrap items-center gap-3">
      <Popover open={open} onOpenChange={handleOpenChange}>
        <PopoverTrigger asChild>
          <Button
            variant="outline"
            className={cn(
              'w-[280px] justify-start text-left font-normal',
              !displayRange.from && 'text-muted-foreground'
            )}
          >
            <CalendarIcon className="mr-2 h-4 w-4" />
            {displayRange.from ? (
              displayRange.to ? (
                <>
                  {format(displayRange.from, 'LLL dd, y')} -{' '}
                  {format(displayRange.to, 'LLL dd, y')}
                </>
              ) : (
                format(displayRange.from, 'LLL dd, y')
              )
            ) : (
              <span>Pick a date range</span>
            )}
          </Button>
        </PopoverTrigger>
        <PopoverContent className="w-auto p-0" align="start">
          <Calendar
            mode="range"
            defaultMonth={localRange.from}
            selected={localRange}
            onSelect={handleSelect}
            numberOfMonths={2}
            disabled={{ after: new Date() }}
          />
          <div className="border-t p-3">
            <div className="flex flex-wrap gap-1">
              {presets.map((preset) => (
                <Button
                  key={preset.value}
                  variant="ghost"
                  size="sm"
                  onClick={() => handlePresetClick(preset.value)}
                  className="text-xs"
                >
                  {preset.label}
                </Button>
              ))}
            </div>
          </div>
        </PopoverContent>
      </Popover>
    </div>
  );
}
