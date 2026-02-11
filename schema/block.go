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
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
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
	Version        int
	Name           string // Block name (e.g., "2025/01/19" or "2025/01/19/parts/00-02")
	Date           util.Date
	Partition      *util.Partition // Non-nil for partition blocks
	Mint, Maxt     int64
	Shards         int64
	ColumnsForName map[string][]string
}

var streamPathRE = regexp.MustCompile(
	fmt.Sprintf(`^(?P<hash>[0-9]{1,64})/%s$`, regexp.QuoteMeta(StreamFile)),
)

func SplitStreamPath(p string) (ExternalLabelsHash, bool) {
	m := streamPathRE.FindStringSubmatch(p)
	if m == nil {
		return 0, false
	}
	eh, err := strconv.ParseUint(m[1], 10, 64)
	if err != nil {
		eh = 0
	}

	return ExternalLabelsHash(eh), true
}

// SplitBlockPath splits a block path into its components.
// Supported formats:
//   - <hash>/YYYY/MM/DD/<file> (daily with hash)
//   - YYYY/MM/DD/<file> (daily without hash)
//   - <hash>/YYYY/MM/DD/parts/HH-HH/<file> (partition with hash)
//   - YYYY/MM/DD/parts/HH-HH/<file> (partition without hash)
//
// Returns: date, file, extLabelsHash, ok
func SplitBlockPath(p string) (util.Date, string, ExternalLabelsHash, bool) {
	parts := strings.Split(p, "/")
	if len(parts) < 4 {
		return util.Date{}, "", 0, false
	}

	// Try parsing without hash first (YYYY/MM/DD/... format)
	// Check if parts[0] looks like a year (1900-9999)
	if y, err := strconv.Atoi(parts[0]); err == nil && y >= 1900 && y <= 9999 {
		return splitBlockPathWithOffset(parts, 0, 0)
	}

	// If parts[0] doesn't look like a year, try parsing it as a hash
	if eh, err := strconv.ParseUint(parts[0], 10, 64); err == nil {
		if len(parts) >= 5 {
			return splitBlockPathWithOffset(parts, 1, ExternalLabelsHash(eh))
		}
	}

	return util.Date{}, "", 0, false
}

// splitBlockPathWithOffset parses block path components starting from offset.
func splitBlockPathWithOffset(parts []string, offset int, extLabelsHash ExternalLabelsHash) (util.Date, string, ExternalLabelsHash, bool) {
	// Need at least YYYY/MM/DD/<file> after offset
	if len(parts) < offset+4 {
		return util.Date{}, "", 0, false
	}

	// Parse date components
	y, err := strconv.Atoi(parts[offset])
	if err != nil || y < 1900 || y > 9999 {
		return util.Date{}, "", 0, false
	}
	mo, err := strconv.Atoi(parts[offset+1])
	if err != nil || mo < 1 || mo > 12 {
		return util.Date{}, "", 0, false
	}
	d, err := strconv.Atoi(parts[offset+2])
	if err != nil || d < 1 || d > 31 {
		return util.Date{}, "", 0, false
	}

	date := util.NewDate(y, time.Month(mo), d)

	// Check for partition format: YYYY/MM/DD/parts/HH-HH/<file>
	if len(parts) >= offset+6 && parts[offset+3] == "parts" && isHourRangeFormat(parts[offset+4]) {
		file := parts[offset+5]
		return date, file, extLabelsHash, true
	}

	// Daily format: YYYY/MM/DD/<file>
	file := parts[offset+3]
	return date, file, extLabelsHash, true
}

// isHourRangeFormat checks if a string matches the HH-HH partition format.
// Examples: "00-02", "08-16", "22-24"
func isHourRangeFormat(s string) bool {
	if len(s) != 5 || s[2] != '-' {
		return false
	}
	startHour, err := strconv.Atoi(s[:2])
	if err != nil || startHour < 0 || startHour > 23 {
		return false
	}
	endHour, err := strconv.Atoi(s[3:])
	if err != nil || endHour < 1 || endHour > 24 {
		return false
	}
	return endHour > startHour
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

// BlockNameForPartition returns the block name for a partition.
func BlockNameForPartition(partition util.Partition) string {
	return partition.String()
}

// IsPartition returns true if the block name represents a partition block (sub-daily).
// Partition blocks have the format: YYYY/MM/DD/parts/HH-HH
// Daily blocks have the format: YYYY/MM/DD
func IsPartition(name string) bool {
	parts := strings.Split(name, "/")
	// Partition format: YYYY/MM/DD/parts/HH-HH (5 parts)
	// With hash: <hash>/YYYY/MM/DD/parts/HH-HH (6 parts)
	if len(parts) == 5 && parts[3] == "parts" && isHourRangeFormat(parts[4]) {
		return true
	}
	if len(parts) == 6 && parts[4] == "parts" && isHourRangeFormat(parts[5]) {
		return true
	}
	return false
}

// DailyBlockNameForPartition returns the daily block name that would cover the same date.
// For "2025/12/02/parts/08-10" returns "2025/12/02"
// For "<hash>/2025/12/02/parts/08-10" returns "<hash>/2025/12/02"
func DailyBlockNameForPartition(name string) string {
	parts := strings.Split(name, "/")
	// Without hash: YYYY/MM/DD/parts/HH-HH -> YYYY/MM/DD
	if len(parts) == 5 && parts[3] == "parts" {
		return path.Join(parts[0], parts[1], parts[2])
	}
	// With hash: <hash>/YYYY/MM/DD/parts/HH-HH -> <hash>/YYYY/MM/DD
	if len(parts) == 6 && parts[4] == "parts" {
		return path.Join(parts[0], parts[1], parts[2], parts[3])
	}
	return name
}

// blockNameFromDateOrPartition returns the appropriate block name for a date or partition.
func blockNameFromDateOrPartition(date util.Date, partition *util.Partition) string {
	if partition != nil {
		return partition.String()
	}
	return date.String()
}

func LabelsPfileNameForShard(extLabelsHash ExternalLabelsHash, date util.Date, partition *util.Partition, shard int) string {
	blockName := blockNameFromDateOrPartition(date, partition)
	if extLabelsHash == 0 {
		return fmt.Sprintf("%s/%d.%s", blockName, shard, "labels.parquet")
	}
	return fmt.Sprintf("%s/%s/%d.%s", extLabelsHash.String(), blockName, shard, "labels.parquet")
}

func ChunksPfileNameForShard(extLabelsHash ExternalLabelsHash, date util.Date, partition *util.Partition, shard int) string {
	blockName := blockNameFromDateOrPartition(date, partition)
	if extLabelsHash == 0 {
		return fmt.Sprintf("%s/%d.%s", blockName, shard, "chunks.parquet")
	}
	return fmt.Sprintf("%s/%s/%d.%s", extLabelsHash.String(), blockName, shard, "chunks.parquet")
}

func MetaFileNameForBlock(date util.Date, partition *util.Partition, extLabelsHash ExternalLabelsHash) string {
	blockName := blockNameFromDateOrPartition(date, partition)
	if extLabelsHash == 0 {
		return path.Join(blockName, MetaFile)
	}
	return path.Join(extLabelsHash.String(), blockName, MetaFile)
}

func StreamDescriptorFileNameForBlock(extLabelsHash ExternalLabelsHash) string {
	return path.Join(extLabelsHash.String(), StreamFile)
}
