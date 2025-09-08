// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package schema

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
)

const (
	MetaFile   = "meta.pb"
	dateFormat = "%04d/%02d/%02d"
)

type Meta struct {
	Version        int
	Name           string
	Mint, Maxt     int64
	Shards         int64
	ColumnsForName map[string][]string
}

func SplitBlockPath(p string) (string, string, bool) {
	parts := strings.Split(p, "/")
	if len(parts) < 4 {
		return "", "", false
	}
	// Validate date parts.
	if _, err := strconv.Atoi(parts[0]); err != nil {
		return "", "", false
	}
	if _, err := strconv.Atoi(parts[1]); err != nil {
		return "", "", false
	}
	if _, err := strconv.Atoi(parts[2]); err != nil {
		return "", "", false
	}

	// Hashed form: yyyy/mm/dd/<hash>/<file>
	if len(parts) >= 5 {
		return filepath.Join(parts[0], parts[1], parts[2], parts[3]), parts[len(parts)-1], true
	}

	// Plain form: yyyy/mm/dd/<file>
	return filepath.Join(parts[0], parts[1], parts[2]), parts[3], true
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

func BlockNameForDay(t time.Time, externalLabelValue string) (string, error) {
	if t.Location() != time.UTC {
		return "", fmt.Errorf("block start time %s must be in UTC", t)
	}
	if !t.Equal(util.BeginOfDay(t)) {
		return "", fmt.Errorf("block start time %s must be aligned to a day", t)
	}
	year, month, day := t.Date()
	if externalLabelValue == "" {
		// Use the original format if no external label is provided
		return fmt.Sprintf(dateFormat, year, int(month), day), nil
	}
	// Include the hashed label in the block name
	return fmt.Sprintf(dateFormat, year, int(month), day) + "/" + externalLabelValue, nil
}

func LabelsPfileNameForShard(name string, shard int) string {
	return fmt.Sprintf("%s/%d.%s", name, shard, "labels.parquet")
}
func ChunksPfileNameForShard(name string, shard int) string {
	return fmt.Sprintf("%s/%d.%s", name, shard, "chunks.parquet")
}

func MetaFileNameForBlock(name string) string {
	return fmt.Sprintf("%s/%s", name, MetaFile)
}
