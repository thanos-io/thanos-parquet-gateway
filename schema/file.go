// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package schema

import (
	"context"
	"io"

	"github.com/parquet-go/parquet-go"
)

type FileWithReader struct {
	f *parquet.File

	// We smuggle a bucket reader here so we can create a prepared ReaderAt
	// for chunks that is scoped to a query and can be traced.
	rdrCtx func(ctx context.Context) io.ReaderAt
}

func NewFileWithReader(f *parquet.File, rdrF func(ctx context.Context) io.ReaderAt) (*FileWithReader, error) {
	return &FileWithReader{f: f, rdrCtx: rdrF}, nil
}

func (f *FileWithReader) File() *parquet.File {
	return f.f
}

func (f *FileWithReader) Reader(ctx context.Context) io.ReaderAt {
	return f.rdrCtx(ctx)
}

func (f *FileWithReader) DictionaryPageBounds(rgIdx, colIdx int) (uint64, uint64) {
	colMeta := f.f.Metadata().RowGroups[rgIdx].Columns[colIdx].MetaData
	return uint64(colMeta.DictionaryPageOffset), uint64(colMeta.DataPageOffset - colMeta.DictionaryPageOffset)
}
