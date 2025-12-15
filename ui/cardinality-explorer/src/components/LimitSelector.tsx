import { Select } from '@/components/ui/select';
import { useLimit, useCardinalityActions } from '@/stores/useCardinalityStore';

const limitOptions = [
  { value: 10, label: 'Top 10' },
  { value: 20, label: 'Top 20' },
  { value: 50, label: 'Top 50' },
  { value: 100, label: 'Top 100' },
];

export function LimitSelector() {
  const limit = useLimit();
  const { updateLimit } = useCardinalityActions();

  return (
    <Select
      value={limit}
      onChange={(e) => updateLimit(Number(e.target.value))}
      options={limitOptions}
      className="w-[120px]"
    />
  );
}
