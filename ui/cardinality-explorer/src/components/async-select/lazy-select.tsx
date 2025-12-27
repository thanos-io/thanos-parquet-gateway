import Select, {
  components,
  type InputActionMeta,
  type InputProps,
  type SingleValue,
} from 'react-select';
import CreatableSelect from 'react-select/creatable';
import { useState, useCallback, useMemo } from 'react';
import './styles.css';

export interface Option {
  value: string;
}

interface LazySelectProps {
  /**
   * Function to load all options. Called once on first focus.
   * Returns a promise resolving to an array of options.
   */
  loadOptions: () => Promise<Option[]>;
  value?: string;
  onChange?: (value: Option | null) => void;
  placeholder?: string;
  isDisabled?: boolean;
  isClearable?: boolean;
  isCreatable?: boolean;
  className?: string;
  /** Maximum number of options to display in dropdown (default: 100) */
  maxDisplayedOptions?: number;
  /** Values to exclude from the dropdown (filtered at display time, not load time) */
  excludeValues?: Set<string>;
}

const Input = (props: InputProps<Option, boolean>) => (
  <components.Input {...props} isHidden={false} />
);

/**
 * LazySelect - A performant async select for large datasets.
 *
 * Key characteristics:
 * - Fetches ALL options once on first focus, stores in state
 * - Filters options client-side from stored state
 * - Only renders limited options (default 100) for performance
 * - No re-fetching on subsequent focus or typing
 */
export function LazySelect({
  loadOptions,
  value: defaultValue = '',
  onChange,
  placeholder,
  isDisabled,
  isClearable = false,
  isCreatable = false,
  className,
  maxDisplayedOptions = 100,
  excludeValues,
}: LazySelectProps) {
  // Selected value
  const [value, setValue] = useState<SingleValue<Option>>(
    defaultValue ? { value: defaultValue } : null
  );
  // Input text (what user is typing)
  const [inputValue, setInputValue] = useState<string>(defaultValue);
  // ALL fetched options stored for client-side filtering
  const [allOptions, setAllOptions] = useState<Option[]>([]);
  // Loading state
  const [isLoading, setIsLoading] = useState(false);
  // Track if data has been loaded
  const [hasLoaded, setHasLoaded] = useState(false);

  // Fetch all options on first focus only
  const handleFocus = useCallback(async () => {
    if (hasLoaded) return; // Already loaded, skip
    setIsLoading(true);
    try {
      const opts = await loadOptions();
      setAllOptions(opts);
      setHasLoaded(true);
    } finally {
      setIsLoading(false);
    }
  }, [loadOptions, hasLoaded]);

  // Compute displayed options: filtered from allOptions, limited to maxDisplayedOptions
  const displayedOptions = useMemo(() => {
    const searchTerm = inputValue.trim().toLowerCase();
    const currentValue = value?.value?.toLowerCase() ?? '';

    // Start with all options, then apply filters
    let filtered = allOptions;

    // Filter out excluded values (e.g., labels already selected in other rules)
    if (excludeValues && excludeValues.size > 0) {
      filtered = filtered.filter((opt) => !excludeValues.has(opt.value));
    }

    // If no search term or input equals current selected value, show first N items
    if (!searchTerm || searchTerm === currentValue) {
      return filtered.slice(0, maxDisplayedOptions);
    }

    // Filter by search term (case-insensitive contains), then limit
    return filtered
      .filter((opt) => opt.value.toLowerCase().includes(searchTerm))
      .slice(0, maxDisplayedOptions);
  }, [allOptions, inputValue, value, maxDisplayedOptions, excludeValues]);

  // Disable react-select's built-in filtering - we handle it ourselves
  const filterOption = useCallback(() => true, []);

  const onInputChange = (newValue: string, { action }: InputActionMeta) => {
    if (action === 'input-blur') {
      // On blur, reset input to selected value
      setInputValue(value ? value.value : '');
    }

    if (action === 'input-change') {
      // Update input for filtering
      setInputValue(newValue);
    }
  };

  const handleChange = (newValue: SingleValue<Option>) => {
    setValue(newValue);
    setInputValue(newValue ? newValue.value : '');
    if (onChange) {
      onChange(newValue);
    }
  };

  const selectProps = {
    options: displayedOptions,
    value,
    inputValue,
    onInputChange,
    filterOption,
    controlShouldRenderValue: false,
    components: { Input },
    isMulti: false as const,
    onChange: handleChange,
    onFocus: handleFocus,
    isClearable,
    isLoading,
    getOptionLabel: (o: Option) => o.value,
    getOptionValue: (o: Option) => o.value,
    placeholder,
    isDisabled,
    className,
    classNamePrefix: 'react-select',
    classNames: {
      control: (state: { isFocused: boolean }) =>
        `!border-border !bg-background !min-h-9 ${state.isFocused ? '!border-ring !ring-1 !ring-ring' : ''
        }`,
      menu: () => '!bg-popover !border-border !border !mt-1 !z-50',
      option: (state: { isSelected: boolean }) =>
        `hover:!bg-accent hover:!text-accent-foreground ${state.isSelected ? '!bg-accent !text-accent-foreground' : '!text-popover-foreground'
        }`,
      menuList: () => '!py-0',
      placeholder: () => '!text-muted-foreground',
      singleValue: () => '!text-foreground',
      input: () => '!text-foreground',
      indicatorSeparator: () => '!hidden',
      dropdownIndicator: () => '!hidden',
    },
  };

  if (isCreatable) {
    return (
      <CreatableSelect
        {...selectProps}
        allowCreateWhileLoading={true}
        createOptionPosition="first"
      />
    );
  }

  return <Select {...selectProps} />;
}
