import Select, {
  components,
  createFilter,
  type FilterOptionOption,
  type InputActionMeta,
  type InputProps,
  type SingleValue,
} from 'react-select';
import './styles.css';
import { useCallback, useState } from 'react';

export interface Option {
  value: string;
}

interface StaticSelectProps {
  options: Option[];
  value?: string;
  onChange?: (value: Option | null) => void;
  placeholder?: string;
  isDisabled?: boolean;
  isClearable?: boolean;
  className?: string;
}

const Input = (props: InputProps<Option, boolean>) => (
  <components.Input {...props} isHidden={false} />
);

export function StaticSelect({
  options,
  value: defaultValue = '',
  onChange,
  placeholder,
  isDisabled,
  isClearable = false,
  className,
}: StaticSelectProps) {
  const [value, setValue] = useState<SingleValue<Option>>({ value: defaultValue });
  const [inputValue, setInputValue] = useState<string | undefined>(defaultValue);
  const [isEditing, setIsEditing] = useState(false);

  const onInputChange = (newValue: string, { action }: InputActionMeta) => {
    // onBlur => setInputValue to last selected value
    if (action === 'input-blur') {
      setInputValue(value ? value.value : '');
      setIsEditing(false);
    }

    // onInputChange => update newValue
    if (action === 'input-change') {
      setInputValue(newValue);
      setIsEditing(true);
    }
  };

  const handleChange = (newValue: SingleValue<Option>) => {
    setValue(newValue);
    setInputValue(newValue ? newValue.value : '');
    if (onChange) {
      onChange(newValue);
    }
    setIsEditing(false);
  };

  // initially show all options when editing starts
  const filterOption = useCallback(
    (candidate: FilterOptionOption<Option>, input: string) => {
      if (!isEditing && input === value?.value) return true;

      return createFilter()(candidate, input);
    },
    [isEditing, value]
  );

  return (
    <Select
      options={options}
      value={value}
      filterOption={filterOption}
      inputValue={inputValue}
      onInputChange={onInputChange}
      controlShouldRenderValue={false}
      components={{ Input }}
      onChange={handleChange}
      isMulti={false}
      isClearable={isClearable}
      getOptionLabel={(o) => o.value}
      getOptionValue={(o) => o.value}
      placeholder={placeholder}
      isDisabled={isDisabled}
      className={className}
      classNamePrefix="react-select"
      classNames={{
        control: (state) =>
          `!border-border !bg-background !min-h-9 ${
            state.isFocused ? '!border-ring !ring-1 !ring-ring' : ''
          }`,
        menu: () => '!bg-popover !border-border !border !mt-1 !z-50',
        option: (state) =>
          `hover:!bg-accent hover:!text-accent-foreground ${
            state.isSelected ? '!bg-accent !text-accent-foreground' : '!text-popover-foreground'
          }`,
        menuList: () => '!py-0',
        placeholder: () => '!text-muted-foreground',
        singleValue: () => '!text-foreground',
        input: () => '!text-foreground',
        indicatorSeparator: () => '!hidden',
        dropdownIndicator: () => '!hidden',
      }}
    />
  );
}
