import { createStore } from 'zustand';
import { useStoreWithEqualityFn } from 'zustand/traditional';
import { devtools } from 'zustand/middleware';
import { immer } from 'zustand/middleware/immer';
import { subDays } from 'date-fns';
import type { RuleGroupType } from 'react-querybuilder';
import { useCardinalityStoreContext } from '@/providers/Cardinality';

export interface CardinalityDateRange {
  start: Date;
  end: Date;
}

export interface CardinalityState {
  dateRange: CardinalityDateRange;
  limit: number;
  selectedMetric: string | null;
  matcherQuery: RuleGroupType;
}

interface CardinalityActions {
  updateDateRange: (start: Date, end: Date) => void;
  updateLimit: (limit: number) => void;
  selectMetric: (metricName: string | null) => void;
  setMatcherQuery: (query: RuleGroupType) => void;
  clearMatchers: () => void;
  resetFilters: () => void;
}

export interface CardinalityStore extends CardinalityState {
  actions: CardinalityActions;
}

export type CardinalityStoreApi = ReturnType<typeof createCardinalityStore>;

const createEmptyQuery = (): RuleGroupType => ({
  combinator: 'and',
  rules: [],
});

const createInitialState = (): CardinalityState => ({
  dateRange: {
    start: subDays(new Date(), 3),
    end: new Date(),
  },
  limit: 20,
  selectedMetric: null,
  matcherQuery: createEmptyQuery(),
});

export const createCardinalityStore = () => {
  return createStore<CardinalityStore>()(
    devtools(
      immer((set) => ({
        ...createInitialState(),

        actions: {
          updateDateRange: (start: Date, end: Date) =>
            set(
              (state) => {
                state.dateRange.start = start;
                state.dateRange.end = end;
              },
              false,
              'cardinality/updateDateRange'
            ),

          updateLimit: (limit: number) =>
            set(
              (state) => {
                state.limit = limit;
              },
              false,
              'cardinality/updateLimit'
            ),

          selectMetric: (metricName: string | null) =>
            set(
              (state) => {
                state.selectedMetric = metricName;
              },
              false,
              'cardinality/selectMetric'
            ),

          setMatcherQuery: (query: RuleGroupType) =>
            set(
              (state) => {
                state.matcherQuery = query;
              },
              false,
              'cardinality/setMatcherQuery'
            ),

          clearMatchers: () =>
            set(
              (state) => {
                state.matcherQuery = createEmptyQuery();
              },
              false,
              'cardinality/clearMatchers'
            ),

          resetFilters: () =>
            set(
              () => ({
                ...createInitialState(),
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                actions: undefined as any,
              }),
              true,
              'cardinality/resetFilters'
            ),
        },
      })),
      { name: 'CardinalityStore' }
    )
  );
};

const useCardinalityStore = <T,>(
  selector: (state: CardinalityStore) => T,
  equalityFn?: (a: T, b: T) => boolean
): T => {
  const store = useCardinalityStoreContext();
  return useStoreWithEqualityFn(store, selector, equalityFn);
};

export const useDateRange = () => useCardinalityStore((state) => state.dateRange);
export const useLimit = () => useCardinalityStore((state) => state.limit);
export const useSelectedMetric = () => useCardinalityStore((state) => state.selectedMetric);
export const useMatcherQuery = () => useCardinalityStore((state) => state.matcherQuery);

export const useCardinalityActions = () => useCardinalityStore((state) => state.actions);
