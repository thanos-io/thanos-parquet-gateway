// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package slogerrcapture

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/efficientgo/core/errors"
)

type doFunc func() error

// Do is making sure we log every error, even those from best effort tiny functions.
func Do(logger *slog.Logger, doer doFunc, format string, a ...any) {
	derr := doer()
	if derr == nil {
		return
	}

	// For os closers, it's a common case to double close. From reliability purpose this is not a problem it may only indicate
	// surprising execution path.
	if errors.Is(derr, os.ErrClosed) {
		return
	}

	logger.Error("detected do error", slog.String("err", errors.Wrap(derr, fmt.Sprintf(format, a...)).Error()))
}
