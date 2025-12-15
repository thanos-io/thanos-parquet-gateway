import Select, {
  components,
  type InputActionMeta,
  type InputProps,
  type SingleValue,
} from 'react-select';
import CreatableSelect from 'react-select/creatable';
import { useState } from 'react';
import './styles.css';
import { useDebounced } from './lib';

export interface Option {
  value: string;
}

interface AsyncSelectProps {
  loadOptions: (inputValue: string) => Promise<Option[]>;
  value?: string;
  onChange?: (value: Option | null) => void;
  isDisabled?: boolean;
  isClearable?: boolean;
  isCreatable?: boolean;
  className?: string;
  placeholder?: string;
  debounceDelay?: number;
}

const Input = (props: InputProps<Option, boolean>) => (
  <components.Input {...props} isHidden={false} />
);

export function AsyncSelect({
  loadOptions,
  value: defaultValue = '',
  onChange,
  isDisabled,
  isClearable = false,
  isCreatable = true,
  className,
  placeholder,
  debounceDelay = 300,
}: AsyncSelectProps) {
  const [value, setValue] = useState<SingleValue<Option>>({ value: defaultValue });
  const [inputValue, setInputValue] = useState<string>(defaultValue);
  const [options, setOptions] = useState<Option[] | undefined>([]);
  const [isLoading, setIsLoading] = useState<boolean>(false);

  const debounceLoad = useDebounced(async (query: string) => {
    return loadOptions(query);
  }, debounceDelay);

  const handleMenuOpen = async () => {
    setIsLoading(true);
    const opts = await loadOptions(inputValue);
    setOptions(opts);
    setIsLoading(false);
  };

  const onInputChange = async (newValue: string, { action }: InputActionMeta) => {
    // onBlur => setInputValue to last selected value
    if (action === 'input-blur') {
      setInputValue(value ? value.value : '');
    }

    // onInputChange => update newValue
    if (action === 'input-change') {
      setInputValue(newValue);
      setIsLoading(true);
      setOptions([]);
      const opts = await debounceLoad(newValue);
      setOptions(opts);
      setIsLoading(false);
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
    options,
    value,
    inputValue,
    onInputChange,
    controlShouldRenderValue: false,
    components: { Input },
    isMulti: false as const,
    onChange: handleChange,
    onMenuOpen: handleMenuOpen,
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
        `!border-border !bg-background !min-h-9 ${
          state.isFocused ? '!border-ring !ring-1 !ring-ring' : ''
        }`,
      menu: () => '!bg-popover !border-border !border !mt-1 !z-50',
      option: (state: { isSelected: boolean }) =>
        `hover:!bg-accent hover:!text-accent-foreground ${
          state.isSelected ? '!bg-accent !text-accent-foreground' : '!text-popover-foreground'
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
