// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package schema

import (
	"fmt"
	"path"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/oklog/ulid/v2"
	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

const (
	MetaFile   = "meta.pb"
	StreamFile = "stream.pb"
	dateFormat = "%04d/%02d/%02d"
)

type ExternalLabelsHash uint64

func (h ExternalLabelsHash) String() string {
	return fmt.Sprintf("%d", uint64(h))
}

// DateString represents date in "YYYY/MM/DD" format.
type DateString string

type ExternalLabels map[string]string

var xxhPool = sync.Pool{
	New: func() any {
		return xxhash.New()
	},
}

func (h ExternalLabels) Hash() ExternalLabelsHash {
	if len(h) == 0 {
		return 0
	}
	var xxh = xxhPool.Get().(*xxhash.Digest)
	defer func() {
		xxh.Reset()
		xxhPool.Put(xxh)
	}()

	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		_, _ = xxh.WriteString(k)
		_, _ = xxh.WriteString(h[k])
	}

	return ExternalLabelsHash(xxh.Sum64())
}

// StreamDescriptor describes a Parquet block stream. Typical structure:
// <ext_labels_hash>/stream.pb
// <ext_labels_hash>/<date>/0.chunks.parquet
// <ext_labels_hash>/<date>/0.labels.parquet
// ...
type StreamDescriptor struct {
	ExternalLabels ExternalLabels
}

type ParquetBlocksStream struct {
	StreamDescriptor
	Metas []Meta

	DiscoveredDays map[util.Date]struct{}
}

type TSDBBlocksStream struct {
	StreamDescriptor
	Metas []metadata.Meta

	DiscoveredDays map[util.Date]struct{}
}

type Meta struct {
	Version            int
	Date               util.Date
	Mint, Maxt         int64
	Shards             int64
	ColumnsForName     map[string][]string
	ConvertedFromBLIDs map[ulid.ULID]struct{}
}

var blockPathRE = regexp.MustCompile(
	`^((?P<hash>[0-9]{1,64})/)?(?P<year>[0-9]{4})-(?P<month>[0-9]{1,2})-(?P<day>[0-9]{1,2})/(?P<file>[^/]+)$`,
)

var streamPathRE = regexp.MustCompile(
	fmt.Sprintf(`^(?P<hash>[0-9]{1,64})/%s$`, regexp.QuoteMeta(StreamFile)),
)

func SplitStreamPath(path string) (ExternalLabelsHash, bool) {
	m := streamPathRE.FindStringSubmatch(path)
	if m == nil {
		return 0, false
	}
	eh, err := strconv.ParseUint(m[1], 10, 64)
	if err != nil {
		eh = 0
	}

	return ExternalLabelsHash(eh), true
}

func SplitBlockPath(path string) (util.Date, string, ExternalLabelsHash, bool) {
	var (
		file          string
		extLabelsHash ExternalLabelsHash
	)

	m := blockPathRE.FindStringSubmatch(path)
	if m == nil {
		return util.Date{}, "", 0, false
	}

	eh, err := strconv.ParseUint(m[2], 10, 64)
	if err != nil {
		eh = 0
	}
	extLabelsHash = ExternalLabelsHash(eh)

	y, err := strconv.Atoi(m[3])
	if err != nil {
		return util.Date{}, "", 0, false
	}
	mo, err := strconv.Atoi(m[4])
	if err != nil {
		return util.Date{}, "", 0, false
	}
	if mo == 0 {
		return util.Date{}, "", 0, false
	}
	d, err := strconv.Atoi(m[5])
	if err != nil {
		return util.Date{}, "", 0, false
	}
	file = m[6]

	return util.NewDate(y, time.Month(mo), d), file, extLabelsHash, true
}

func DayFromBlockName(blk string) (time.Time, error) {
	var (
		year, month, day int
	)
	n, err := fmt.Sscanf(blk, dateFormat, &year, &month, &day)
	if err != nil {
		return time.Time{}, fmt.Errorf("unable to read timestamp from block name: %w", err)
	}
	if n != 3 {
		return time.Time{}, fmt.Errorf("unexpected number of date atoms parsed: %d != 3", n)
	}

	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC), nil
}

func BlockNameForDay(d util.Date) string {
	year, month, day := d.ToTime().Date()
	return fmt.Sprintf(dateFormat, year, int(month), day)
}

func LabelsPfileNameForShard(extLabelsHash ExternalLabelsHash, date util.Date, shard int) string {
	return fmt.Sprintf("%s/%s/%d.%s", extLabelsHash.String(), date.String(), shard, "labels.parquet")
}
func ChunksPfileNameForShard(extLabelsHash ExternalLabelsHash, date util.Date, shard int) string {
	return fmt.Sprintf("%s/%s/%d.%s", extLabelsHash.String(), date.String(), shard, "chunks.parquet")
}

func MetaFileNameForBlock(date util.Date, extLabelsHash ExternalLabelsHash) string {
	if extLabelsHash == 0 {
		return path.Join(date.String(), MetaFile)
	}
	return path.Join(extLabelsHash.String(), date.String(), MetaFile)
}

func StreamDescriptorFileNameForBlock(extLabelsHash ExternalLabelsHash) string {
	return path.Join(extLabelsHash.String(), StreamFile)
}
