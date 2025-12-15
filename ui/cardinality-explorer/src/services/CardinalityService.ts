import { client } from '@/api/api';
import { APIError, type SuccessResponse } from '@/types/api';
import type {
  CardinalityResult,
  LabelsCardinalityPerDayResult,
} from '@/types/cardinality';
import type { AxiosError } from 'axios';
import type { RuleGroupType } from 'react-querybuilder';
import { rulesToMatchSelectors } from '@/lib/matchers';

export class CardinalityService {
  async getMetricsCardinality(
    start: number,
    end: number,
    limit: number,
    metricName?: string,
    matcherQuery?: RuleGroupType
  ): Promise<SuccessResponse<CardinalityResult>> {
    try {
      const params = new URLSearchParams({
        start: start.toString(),
        end: end.toString(),
        limit: limit.toString(),
      });
      // Legacy metric_name support (for metric detail page backward compat)
      if (metricName) {
        params.set('metric_name', metricName);
      }

      // Add match[] parameters from query builder
      if (matcherQuery) {
        const selectors = rulesToMatchSelectors(matcherQuery);
        selectors.forEach((s) => params.append('match[]', s));
      }

      const response = await client.get<{ status: string; data: CardinalityResult }>(
        `/cardinality/metrics?${params.toString()}`
      );
      return { status: response.status, data: response.data.data };
    } catch (error) {
      const axiosError = error as AxiosError<{ error: { code: string; message: string } }>;
      throw new APIError({
        status: axiosError.response?.status || 500,
        error: axiosError.response?.data?.error || {
          code: 'UNKNOWN_ERROR',
          message: 'Failed to fetch metrics cardinality',
        },
      });
    }
  }

  async getLabelsCardinality(
    start: number,
    end: number,
    limit: number,
    metricName?: string,
    matcherQuery?: RuleGroupType
  ): Promise<SuccessResponse<LabelsCardinalityPerDayResult>> {
    try {
      const params = new URLSearchParams({
        start: start.toString(),
        end: end.toString(),
        limit: limit.toString(),
      });
      if (metricName) {
        params.set('metric_name', metricName);
      }

      if (matcherQuery) {
        const selectors = rulesToMatchSelectors(matcherQuery);
        selectors.forEach((s) => params.append('match[]', s));
      }

      const response = await client.get<{ status: string; data: LabelsCardinalityPerDayResult }>(
        `/cardinality/labels?${params.toString()}`
      );
      return { status: response.status, data: response.data.data };
    } catch (error) {
      const axiosError = error as AxiosError<{ error: { code: string; message: string } }>;
      throw new APIError({
        status: axiosError.response?.status || 500,
        error: axiosError.response?.data?.error || {
          code: 'UNKNOWN_ERROR',
          message: 'Failed to fetch labels cardinality',
        },
      });
    }
  }

  async searchLabelNames(
    prefix: string,
    start: number,
    end: number,
    matcherQuery?: RuleGroupType
  ): Promise<{ value: string }[]> {
    try {
      const params = new URLSearchParams({
        start: start.toString(),
        end: end.toString(),
      });

      if (matcherQuery) {
        const selectors = rulesToMatchSelectors(matcherQuery);
        selectors.forEach((s) => params.append('match[]', s));
      }

      const response = await client.get<{ status: string; data: string[] }>(
        `/labels?${params.toString()}`
      );

      const labels = response.data.data || [];
      return labels
        .filter((label) => label.toLowerCase().includes(prefix.toLowerCase()))
        .map((label) => ({ value: label }));
    } catch {
      return [];
    }
  }

  async getLabelValues(
    labelName: string,
    start: number,
    end: number,
    matcherQuery?: RuleGroupType,
  ): Promise<{ value: string }[]> {
    if (!labelName) return [];

    try {
      const params = new URLSearchParams({
        start: start.toString(),
        end: end.toString(),
      });

      if (matcherQuery) {
        const selectors = rulesToMatchSelectors(matcherQuery);
        selectors.forEach((s) => params.append('match[]', s));
      }

      const response = await client.get<{ status: string; data: string[] }>(
        `/label/${encodeURIComponent(labelName)}/values?${params.toString()}`
      );

      const values = response.data.data || [];
      return values.map((value) => ({ value }));
    } catch {
      return [];
    }
  }
}

export const cardinalityService = new CardinalityService();
