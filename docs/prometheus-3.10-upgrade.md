# Upgrade to prometheus 3.10 (ringerc/thanos-prometheus-patches)

**Date**: 2026-03-23

## Background

The gateway previously pinned `github.com/prometheus/prometheus` to
`github.com/thanos-io/thanos-prometheus` (`nocopy_03060rc0` branch, based on
prometheus 3.6.0-rc.0). This fork adds two patches not in upstream prometheus:

- `index.SeriesNoCopy` — zero-copy series reading used in `convert/convert.go`
- `index.Reader.LabelValueFor` / `Decoder.LabelValueFor` — label lookup helpers

`thanos-io/promql-engine` main branch bumped its prometheus dependency to 3.8
(commit `dd52237`), making the gateway's 3.6-based fork incompatible. To
unblock updating promql-engine, the prometheus fork needed to be rebased to a
newer upstream version.

## Changes

### go.mod — replace directives

Three `replace` directives were added/updated:

```
replace (
    github.com/prometheus/prometheus =>
        github.com/ringerc/thanos-prometheus-patches v0.0.0-20260323025352-061110462a8f
    github.com/thanos-io/promql-engine =>
        /home/craig/projects/EDB/UPM/upm-stack/thanos-promql-engine
    github.com/thanos-io/thanos =>
        /home/craig/projects/EDB/UPM/upm-stack/thanos
)
```

| Replace | Reason |
|---------|--------|
| `prometheus` → ringerc fork | Fork rebased on prometheus 3.10.0, retaining `SeriesNoCopy` and `LabelValueFor` patches; replaced the old 3.6-based `thanos-prometheus` fork |
| `promql-engine` → local | Local branch includes prometheus 3.10 API compatibility fixes (see below) |
| `thanos` → local | Local branch includes prometheus 3.10 API compatibility fixes (see below) |

### thanos-parquet-gateway/db/iterator.go — `AtST()` method

Prometheus 3.10 added `AtST() int64` to the `chunkenc.Iterator` interface
(returns the start timestamp of the current sample, used for native histograms
spanning multiple time steps). Three iterator types needed the new method:

- `errSeriesIterator.AtST()` — returns `0`
- `chunkSeriesIterator.AtST()` — delegates to `it.cur.AtST()`
- `boundedSeriesIterator.AtST()` — delegates to `it.it.AtST()`

### thanos/pkg/block/fetcher.go — `relabel.Process` → `relabel.ProcessBuilder`

Prometheus 3.10 removed `relabel.Process(labels.Labels, cfgs...)` and replaced
it with `relabel.ProcessBuilder(*labels.Builder, cfgs...) bool`. Updated the
one call site in `LabelSelectorFilter`.

### thanos/pkg/compact/downsample/downsample.go — `Append`/`AppendFloatHistogram` signatures

Prometheus 3.10 added a start-timestamp (`st int64`) parameter to:

- `chunkenc.Appender.Append(t, st int64, v float64)` — was `(t int64, v float64)`
- `chunkenc.Appender.AppendFloatHistogram(app, t, st int64, fh, bool)` — was `(app, t int64, fh, bool)`

For downsampled/aggregated data there is no separate start timestamp, so `st`
is set equal to `t` at each call site. Nine `Append` calls and one
`AppendFloatHistogram` call were updated.

Additionally, `ApplyCounterResetsSeriesIterator` was missing the new `AtST()`
method from the `chunkenc.Iterator` interface; added `AtST()` returning
`it.lastT`.

### thanos/pkg/block/writer.go, pkg/compactv2/compactor.go, pkg/receive/expandedpostingscache/tsdb.go, internal/cortex/util/metrics_helper.go — `tsdb/errors` removal

Prometheus commit `8d491cc` ("tsdb: Migrate multi-errors to errors package",
2026-02-04) removed the `tsdb/errors` package, replacing it with stdlib
`errors.Join` and a package-private `closeAll` helper.

Four thanos files imported `tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"`.
Each was migrated:

| Pattern | Replacement |
|---------|-------------|
| `tsdb_errors.NewMulti(a, b).Err()` | `errors.Join(a, b)` |
| `tsdb_errors.CloseAll(cs)` | local `closeAll(cs)` using `errors.Join` |
| `errs := tsdb_errors.NewMulti(); errs.Add(x); errs.Err()` | `var errs []error; errs = append(errs, x); errors.Join(errs...)` |

A `closeAll([]io.Closer) error` helper was added to `writer.go` and
`compactor.go`. Files that already imported stdlib `errors` used it directly;
files that imported `github.com/pkg/errors` used an `stderrors` alias for
stdlib.

### thanos-promql-engine/execution/function/histogram.go — `BucketQuantile` and `NewHistogramQuantileForcedMonotonicityInfo`

Between prometheus 3.8 and 3.10:

- `BucketQuantile` return values changed from `(float64, bool, bool)` to
  `(float64, bool, bool, float64, float64, float64)` — the three new values
  are `minBucket`, `maxBucket`, `maxDiff`.
- `NewHistogramQuantileForcedMonotonicityInfo` signature changed from
  `(metricName, pos)` to `(metricName, pos, ts, minBucket, maxBucket, maxDiff)`.

The call in `processInputSeries` was updated to capture and forward the three
new values, using the step vector timestamp `vector.T` for `ts`.

### thanos-prometheus fork — merge conflict resolution

The `ringerc/thanos-prometheus-patches` fork (`nocopy` branch) was created by
merging prometheus v3.10.0 into the old `nocopy_03060rc0` branch. Five conflict
hunks were resolved:

| File | Conflict | Resolution |
|------|----------|------------|
| `tsdb/index/index.go` (×2) | `LabelValueFor(Reader)` + `SeriesParam`/`SeriesNoCopy` vs. deleted | Kept fork (preserved NoCopy patches) |
| `tsdb/index/index_test.go` | `labelIndicesTable` test vs. deleted | Accepted upstream (test relied on removed v1 format internals) |
| `tsdb/querier_test.go` (×2) | `Series(...SeriesParam)` vs. `Series()` | Kept fork (mock must match the patched interface) |
