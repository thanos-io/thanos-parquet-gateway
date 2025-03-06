// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package schema

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/cloudflare/parquet-tsdb-poc/internal/util"
)

const (
	MetaFile   = "meta.pb"
	dateFormat = "%04d/%02d/%02d"
)

func SplitBlockPath(name string) (string, string, bool) {
	var (
		year, month, day int
		file             string
	)
	n, err := fmt.Sscanf(name, dateFormat+"/%s", &year, &month, &day, &file)
	if err != nil {
		return "", "", false
	}
	if n != 4 {
		return "", "", false
	}
	return filepath.Dir(name), file, true
}

func BlockNameForDay(t time.Time) (string, error) {
	if t.Location() != time.UTC {
		return "", fmt.Errorf("block start time %s must be in UTC", t)
	}
	if !t.Equal(util.BeginOfDay(t)) {
		return "", fmt.Errorf("block start time %s must be aligned to a day", t)
	}
	year, month, day := t.Date()
	return fmt.Sprintf(dateFormat, year, int(month), day), nil
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
