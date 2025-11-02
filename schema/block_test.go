// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package schema

import (
	"testing"
	"time"

	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
)

func TestBlockNameForDay(t *testing.T) {
	t.Run("", func(t *testing.T) {
		b := BlockNameForDay(util.NewDate(1970, time.January, 1))
		want := "1970/01/01"
		got := b
		if want != got {
			t.Fatalf("wanted %q, got %q", want, got)
		}
	})
	t.Run("", func(t *testing.T) {
		b := BlockNameForDay(util.NewDate(2024, time.November, 23))
		want := "2024/11/23"
		got := b
		if want != got {
			t.Fatalf("wanted %q, got %q", want, got)
		}
	})
}
