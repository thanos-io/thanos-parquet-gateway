// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package schema

import (
	"testing"
	"time"
)

func TestBlockNameForDay(t *testing.T) {
	t.Run("", func(t *testing.T) {
		d := time.Unix(10, 0).UTC()
		if _, err := BlockNameForDay(d); err == nil {
			t.Fatal("expected error, got none")
		}
	})
	t.Run("", func(t *testing.T) {
		d := time.Unix(0, 0).UTC()
		b, err := BlockNameForDay(d)
		if err != nil {
			t.Fatal("unexpected error: ", err)
		}
		want := "1970/01/01"
		got := b
		if want != got {
			t.Fatalf("wanted %q, got %q", want, got)
		}
	})
	t.Run("", func(t *testing.T) {
		d := time.Unix(1732320000, 0).UTC()
		b, err := BlockNameForDay(d)
		if err != nil {
			t.Fatal("unexpected error: ", err)
		}
		want := "2024/11/23"
		got := b
		if want != got {
			t.Fatalf("wanted %q, got %q", want, got)
		}
	})
}
