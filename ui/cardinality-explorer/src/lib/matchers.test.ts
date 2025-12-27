import type { RuleGroupType } from 'react-querybuilder';
import {
  rulesToMatchSelectors,
  hasActiveMatchers,
  createMetricNameQuery,
} from './matchers';

describe('rulesToMatchSelectors', () => {
  const testCases: Array<{
    name: string;
    query: RuleGroupType;
    expected: string[];
  }> = [
    // Empty and invalid cases
    {
      name: 'empty rules array',
      query: { combinator: 'and', rules: [] },
      expected: [],
    },
    {
      name: 'rule with empty field',
      query: {
        combinator: 'and',
        rules: [{ field: '', operator: '=', value: 'test' }],
      },
      expected: [],
    },
    {
      name: 'rule with empty value',
      query: {
        combinator: 'and',
        rules: [{ field: '__name__', operator: '=', value: '' }],
      },
      expected: [],
    },
    {
      name: 'rule with undefined value',
      query: {
        combinator: 'and',
        rules: [{ field: '__name__', operator: '=', value: undefined } as { field: string; operator: string; value: unknown }],
      },
      expected: [],
    },
    {
      name: 'rule with null value treated as string',
      query: {
        combinator: 'and',
        rules: [{ field: '__name__', operator: '=', value: null }],
      },
      expected: ['{__name__="null"}'],
    },

    // Single rule cases with different operators
    {
      name: 'single rule with exact match operator',
      query: {
        combinator: 'and',
        rules: [{ field: '__name__', operator: '=', value: 'http_requests_total' }],
      },
      expected: ['{__name__="http_requests_total"}'],
    },
    {
      name: 'single rule with not-equal operator',
      query: {
        combinator: 'and',
        rules: [{ field: 'job', operator: '!=', value: 'test' }],
      },
      expected: ['{job!="test"}'],
    },
    {
      name: 'single rule with regex match operator',
      query: {
        combinator: 'and',
        rules: [{ field: 'namespace', operator: '=~', value: 'prod-.*' }],
      },
      expected: ['{namespace=~"prod-.*"}'],
    },
    {
      name: 'single rule with negative regex operator',
      query: {
        combinator: 'and',
        rules: [{ field: 'instance', operator: '!~', value: '.*:9090' }],
      },
      expected: ['{instance!~".*:9090"}'],
    },

    // Multiple rules
    {
      name: 'multiple rules combined into single selector',
      query: {
        combinator: 'and',
        rules: [
          { field: '__name__', operator: '=', value: 'http_requests_total' },
          { field: 'namespace', operator: '=~', value: 'prod-.*' },
          { field: 'job', operator: '!=', value: 'test' },
        ],
      },
      expected: ['{__name__="http_requests_total",namespace=~"prod-.*",job!="test"}'],
    },
    {
      name: 'skips incomplete rules but processes valid ones',
      query: {
        combinator: 'and',
        rules: [
          { field: '__name__', operator: '=', value: 'valid_metric' },
          { field: '', operator: '=', value: 'no_field' },
          { field: 'no_value', operator: '=', value: '' },
          { field: 'also_valid', operator: '=', value: 'yes' },
        ],
      },
      expected: ['{__name__="valid_metric",also_valid="yes"}'],
    },

    // Escaping and special characters
    {
      name: 'escapes double quotes in values',
      query: {
        combinator: 'and',
        rules: [{ field: 'label', operator: '=', value: 'has"quote' }],
      },
      expected: ['{label="has\\"quote"}'],
    },
    {
      name: 'escapes multiple double quotes',
      query: {
        combinator: 'and',
        rules: [{ field: 'label', operator: '=', value: '"start"middle"end"' }],
      },
      expected: ['{label="\\"start\\"middle\\"end\\""}'],
    },
    {
      name: 'handles backslash in value',
      query: {
        combinator: 'and',
        rules: [{ field: 'path', operator: '=', value: 'C:\\Users\\test' }],
      },
      expected: ['{path="C:\\Users\\test"}'],
    },
    {
      name: 'handles newlines in value',
      query: {
        combinator: 'and',
        rules: [{ field: 'multiline', operator: '=', value: 'line1\nline2' }],
      },
      expected: ['{multiline="line1\nline2"}'],
    },
    {
      name: 'handles unicode characters',
      query: {
        combinator: 'and',
        rules: [{ field: 'label', operator: '=', value: '日本語' }],
      },
      expected: ['{label="日本語"}'],
    },
    {
      name: 'handles emoji in value',
      query: {
        combinator: 'and',
        rules: [{ field: 'status', operator: '=', value: 'ok ✓' }],
      },
      expected: ['{status="ok ✓"}'],
    },

    // Edge cases with field names
    {
      name: 'handles label name with underscores',
      query: {
        combinator: 'and',
        rules: [{ field: 'my_label_name', operator: '=', value: 'value' }],
      },
      expected: ['{my_label_name="value"}'],
    },
    {
      name: 'handles __name__ special label',
      query: {
        combinator: 'and',
        rules: [{ field: '__name__', operator: '=~', value: 'http_.*' }],
      },
      expected: ['{__name__=~"http_.*"}'],
    },

    // Numeric values
    {
      name: 'handles numeric value as number type',
      query: {
        combinator: 'and',
        rules: [{ field: 'port', operator: '=', value: 8080 }],
      },
      expected: ['{port="8080"}'],
    },
    {
      name: 'handles zero value',
      query: {
        combinator: 'and',
        rules: [{ field: 'count', operator: '=', value: 0 }],
      },
      expected: ['{count="0"}'],
    },

    // Combinator is ignored (all rules combined with AND semantics)
    {
      name: 'ignores combinator value (always uses AND)',
      query: {
        combinator: 'or',
        rules: [
          { field: 'a', operator: '=', value: '1' },
          { field: 'b', operator: '=', value: '2' },
        ],
      },
      expected: ['{a="1",b="2"}'],
    },

    // Nested groups are filtered out
    {
      name: 'skips nested rule groups',
      query: {
        combinator: 'and',
        rules: [
          { field: 'valid', operator: '=', value: 'rule' },
          {
            combinator: 'or',
            rules: [{ field: 'nested', operator: '=', value: 'ignored' }],
          } as unknown as { field: string; operator: string; value: string },
        ],
      },
      expected: ['{valid="rule"}'],
    },
  ];

  test.each(testCases)('$name', ({ query, expected }) => {
    expect(rulesToMatchSelectors(query)).toEqual(expected);
  });
});

describe('hasActiveMatchers', () => {
  it('should return false for empty rules', () => {
    const query: RuleGroupType = {
      combinator: 'and',
      rules: [],
    };
    expect(hasActiveMatchers(query)).toBe(false);
  });

  it('should return false when rules have no field or value', () => {
    const query: RuleGroupType = {
      combinator: 'and',
      rules: [
        { field: '', operator: '=', value: '' },
      ],
    };
    expect(hasActiveMatchers(query)).toBe(false);
  });

  it('should return true when at least one rule is complete', () => {
    const query: RuleGroupType = {
      combinator: 'and',
      rules: [
        { field: '__name__', operator: '=', value: 'metric' },
      ],
    };
    expect(hasActiveMatchers(query)).toBe(true);
  });
});

describe('createMetricNameQuery', () => {
  it('should create a query with __name__ matcher', () => {
    const query = createMetricNameQuery('http_requests_total');
    expect(query).toEqual({
      combinator: 'and',
      rules: [
        { field: '__name__', operator: '=', value: 'http_requests_total' },
      ],
    });
  });
});
