// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package encoding

import (
	"slices"
	"testing"

	fuzz "github.com/AdaLogics/go-fuzz-headers"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func FuzzEncodeLabelColumnIndex(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		fz := fuzz.NewConsumer(data)

		var (
			in []int
		)
		fz.CreateSlice(&in)

		decoded, err := DecodeLabelColumnIndex(EncodeLabelColumnIndex(in))
		if err != nil {
			t.Fatalf("unable to decode label column index: %s", err)
		}
		if slices.Compare(decoded, in) != 0 {
			t.Fatalf("decoded %q did not match expected %q", decoded, in)
		}
	})
}
