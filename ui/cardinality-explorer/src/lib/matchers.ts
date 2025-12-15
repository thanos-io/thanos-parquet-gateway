import type { RuleGroupType, RuleType } from 'react-querybuilder';

/**
 * Convert react-querybuilder rules to match[] selector strings.
 * All rules are combined into a single selector with AND semantics.
 *
 * Filtering logic:
 * - Skips rules that are not complete (missing field or value)
 * - Escapes quotes in values to prevent injection
 * - Returns empty array if no valid rules exist
 */
export function rulesToMatchSelectors(query: RuleGroupType): string[] {
  // Filter to only RuleType (not nested RuleGroupType) and only complete rules
  const validRules = query.rules.filter(
    (r): r is RuleType =>
      'field' in r &&
      Boolean(r.field) &&
      r.value !== undefined &&
      r.value !== ''
  );

  if (validRules.length === 0) return [];

  const matchers = validRules.map((rule) => {
    // Escape quotes in value for safety
    const escapedValue = String(rule.value).replace(/"/g, '\\"');
    return `${rule.field}${rule.operator}"${escapedValue}"`;
  });

  // All rules combined into single selector: {matcher1,matcher2,...}
  return [`{${matchers.join(',')}}`];
}

/**
 * Check if query has any active matchers.
 */
export function hasActiveMatchers(query: RuleGroupType): boolean {
  return query.rules.some((r) => 'field' in r && r.field && r.value);
}

/**
 * Create a matcher rule for a specific metric name.
 * Used when navigating to metric detail page.
 */
export function createMetricNameQuery(metricName: string): RuleGroupType {
  return {
    combinator: 'and',
    rules: [{ field: '__name__', operator: '=', value: metricName }],
  };
}
