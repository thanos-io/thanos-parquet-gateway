import { useCallback, useMemo, useEffect } from 'react';
import {
  QueryBuilder,
  type RuleGroupType,
  type RuleType,
  type Field,
  type FullOption,
  type ValueEditorProps,
  type FieldSelectorProps,
  type OperatorSelectorProps,
  type ActionProps,
} from 'react-querybuilder';
import { X, HelpCircle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover';
import { LazySelect } from '@/components/async-select/lazy-select';
import { StaticSelect } from '@/components/async-select/single';
import { useMatcherQuery, useCardinalityActions, useDateRange } from '@/stores/useCardinalityStore';
import { MATCHER_OPERATORS } from '@/types/cardinality';
import { cardinalityService } from '@/services/CardinalityService';
import 'react-querybuilder/dist/query-builder.css';

const fields: Field[] = [];

const createEmptyRule = (): RuleType => ({
  id: `rule-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
  field: '',
  operator: '=',
  value: '',
});

function CustomAddRuleAction() {
  return null;
}

function CustomAddGroupAction() {
  return null;
}

function CustomRemoveRuleAction({ handleOnClick, disabled }: ActionProps) {
  return (
    <Button
      type="button"
      variant="ghost"
      onClick={handleOnClick}
      disabled={disabled}
      className="text-xs h-9"
    >
      <X className="h-4 w-4" />
    </Button>
  );
}

function CustomRemoveGroupAction() {
  return null;
}

function CustomCombinatorSelector() {
  return null;
}

interface LabelMatcherBuilderProps {
  className?: string;
}

export function LabelMatcherBuilder({ className }: LabelMatcherBuilderProps) {
  const query = useMatcherQuery();
  const { setMatcherQuery, clearMatchers } = useCardinalityActions();
  const dateRange = useDateRange();

  const handleQueryChange = useCallback(
    (newQuery: RuleGroupType) => {
      setMatcherQuery(newQuery);
    },
    [setMatcherQuery]
  );

  useEffect(() => {
    const rules = query.rules as RuleType[];
    const hasIncompleteRule = rules.some((r) => !r.field || !r.value);

    if (!hasIncompleteRule) {
      setMatcherQuery({
        ...query,
        rules: [...rules, createEmptyRule()],
      });
    }
  }, [query, setMatcherQuery]);

  const startTs = Math.floor(dateRange.start.getTime() / 1000);
  const endTs = Math.floor(dateRange.end.getTime() / 1000);

  const getOtherRulesQuery = useCallback(
    (currentRuleId?: string): RuleGroupType | undefined => {
      const otherRules = query.rules.filter(
        (r): r is RuleType =>
          'field' in r &&
          r.id !== currentRuleId &&
          Boolean(r.field) &&
          r.value !== undefined &&
          r.value !== ''
      );
      if (otherRules.length === 0) return undefined;
      return { combinator: 'and', rules: otherRules };
    },
    [query.rules]
  );

  const getUsedLabelNames = useCallback(
    (currentRuleId?: string): Set<string> => {
      const usedLabels = new Set<string>();
      query.rules.forEach((r) => {
        if ('field' in r && r.id !== currentRuleId && r.field) {
          usedLabels.add(r.field);
        }
      });
      return usedLabels;
    },
    [query.rules]
  );

  const CustomFieldSelector = useMemo(
    () =>
      function FieldSelector({ handleOnChange, rule }: FieldSelectorProps) {
        const otherRulesQuery = getOtherRulesQuery(rule?.id);
        const usedLabels = getUsedLabelNames(rule?.id);
        return (
          <LazySelect
            loadOptions={() =>
              cardinalityService.searchLabelNames('', startTs, endTs, otherRulesQuery)
            }
            excludeValues={usedLabels}
            value={rule?.field || ''}
            onChange={(o) => handleOnChange(o?.value || '')}
            placeholder="label name"
            className="min-w-36"
          />
        );
      },
    [startTs, endTs, getOtherRulesQuery, getUsedLabelNames]
  );

  const CustomValueEditor = useMemo(
    () =>
      function ValueEditor({ value, handleOnChange, field, rule }: ValueEditorProps) {
        const otherRulesQuery = getOtherRulesQuery(rule?.id);
        return (
          <LazySelect
            loadOptions={() =>
              cardinalityService.getLabelValues(field, startTs, endTs, otherRulesQuery)
            }
            value={value || ''}
            onChange={(o) => handleOnChange(o?.value || '')}
            placeholder="value"
            className="min-w-48"
          />
        );
      },
    [startTs, endTs, getOtherRulesQuery]
  );

  const CustomOperatorSelector = useCallback(
    ({ value, handleOnChange, options }: OperatorSelectorProps) => (
      <StaticSelect
        options={(options as FullOption[]).map((o) => ({ value: o.name }))}
        value={value}
        onChange={(o) => handleOnChange(o?.value || '')}
        className="w-20"
      />
    ),
    []
  );

  return (
    <Card className={className}>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <CardTitle className="text-base">Label Filters</CardTitle>
            <MatcherHelp />
          </div>
          {(query.rules as RuleType[]).some((r) => r.field) && (
            <Button
              variant="ghost"
              size="sm"
              onClick={clearMatchers}
              className="text-xs"
            >
              Clear all
            </Button>
          )}
        </div>
      </CardHeader>
      <CardContent>
        <QueryBuilder
          query={query}
          onQueryChange={handleQueryChange}
          fields={fields}
          operators={MATCHER_OPERATORS}
          controlElements={{
            fieldSelector: CustomFieldSelector,
            operatorSelector: CustomOperatorSelector,
            valueEditor: CustomValueEditor,
            combinatorSelector: CustomCombinatorSelector,
            addRuleAction: CustomAddRuleAction,
            addGroupAction: CustomAddGroupAction,
            removeRuleAction: CustomRemoveRuleAction,
            removeGroupAction: CustomRemoveGroupAction,
          }}
          controlClassnames={{
            queryBuilder: '',
            body: '!gap-0',
            ruleGroup:
              'border border-border rounded-md !pt-1 !p-0 !bg-transparent !border-transparent',
            rule: 'flex items-center gap-2 p-2 rounded bg-muted/50',
            combinators: 'flex gap-1 p-1',
            addRule: '',
            addGroup: '',
            removeRule: '',
            removeGroup: '',
          }}
          resetOnFieldChange={false}
        />


      </CardContent>
    </Card>
  );
}

function MatcherHelp() {
  return (
    <Popover>
      <PopoverTrigger asChild>
        <Button variant="ghost" size="icon" className="h-6 w-6">
          <HelpCircle className="h-4 w-4" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-80">
        <div className="space-y-2">
          <h4 className="font-medium">Label Matcher Operators</h4>
          <div className="space-y-1 text-sm">
            <p>
              <code className="font-mono bg-muted px-1 rounded">=</code> Exact
              string match
            </p>
            <p>
              <code className="font-mono bg-muted px-1 rounded">!=</code> Not
              equal to value
            </p>
            <p>
              <code className="font-mono bg-muted px-1 rounded">=~</code> Regex
              match
            </p>
            <p>
              <code className="font-mono bg-muted px-1 rounded">!~</code>{' '}
              Negative regex match
            </p>
          </div>
          <div className="border-t pt-2 mt-2">
            <p className="text-sm font-medium">Examples:</p>
            <ul className="text-sm text-muted-foreground space-y-1 mt-1">
              <li>
                <code className="font-mono">__name__ = http_requests_total</code>
              </li>
              <li>
                <code className="font-mono">namespace =~ prod-.*</code>
              </li>
              <li>
                <code className="font-mono">job != test</code>
              </li>
              <li>
                <code className="font-mono">instance !~ .*:9090</code>
              </li>
            </ul>
          </div>
        </div>
      </PopoverContent>
    </Popover>
  );
}
