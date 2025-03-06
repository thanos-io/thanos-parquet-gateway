// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"container/heap"
	"fmt"
	"io"
	"slices"
	"sort"

	"github.com/parquet-go/parquet-go"
)

type sortingWriter struct {
	in io.ReadWriteSeeker

	schema *parquet.Schema
	buffer *parquet.RowBuffer[any]
	writer *parquet.GenericWriter[any]
	cols   []parquet.SortingColumn

	n              int
	flushThreshold int
}

func newSortingWriter(in io.ReadWriteSeeker, p parquet.BufferPool, schema *parquet.Schema, flushThreshold int, cols ...parquet.SortingColumn) *sortingWriter {
	return &sortingWriter{
		in:             in,
		schema:         schema,
		cols:           cols,
		flushThreshold: flushThreshold,
		buffer: parquet.NewRowBuffer[any](schema, parquet.SortingRowGroupConfig(
			parquet.SortingColumns(cols...),
			parquet.SortingBuffers(p),
		),
		),
		writer: parquet.NewGenericWriter[any](in, schema, parquet.SortingWriterConfig(
			parquet.SortingColumns(cols...),
			parquet.SortingBuffers(p),
		),
		),
	}
}

var _ parquet.RowWriter = &sortingWriter{}

func (w *sortingWriter) WriteRows(buf []parquet.Row) (int, error) {
	n, err := w.buffer.WriteRows(buf)
	if err != nil {
		return 0, err
	}
	w.n += n
	if w.n > w.flushThreshold {
		sort.Sort(w.buffer)
		rows := w.buffer.Rows()
		defer rows.Close()
		if _, err := parquet.CopyRows(w.writer, rows); err != nil {
			return 0, err
		}
		if err := w.writer.Flush(); err != nil {
			return 0, err
		}
		w.buffer.Reset()
		w.n = 0
	}
	return n, nil
}

func (w *sortingWriter) Flush() error {
	sort.Sort(w.buffer)
	rows := w.buffer.Rows()
	defer rows.Close()
	if _, err := parquet.CopyRows(w.writer, rows); err != nil {
		return err
	}
	return w.writer.Close()
}

func (w *sortingWriter) RowReader() (parquet.RowReader, error) {
	if _, err := w.in.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	sz, err := sizeFromSeeker(w.in)
	if err != nil {
		return nil, err
	}
	pf, err := parquet.OpenFile(newReaderAt(w.in), sz)
	if err != nil {
		return nil, err
	}
	rrs := make([]parquet.RowReader, 0)
	for _, rg := range pf.RowGroups() {
		rrs = append(rrs, rg.Rows())
	}
	return mergeRowReaders(rrs, w.schema.Comparator(w.cols...)), nil
}

// Taken from https://github.com/parquet-go/parquet-go/blob/main/merge.go
// This was necessary to fix corruption that happened because the head was not cloned, though maybe we have been using the library wrong here and this is not actually necessary.
type mergedRowReader struct {
	compare     func(parquet.Row, parquet.Row) int
	readers     []*bufferedRowReader
	initialized bool
}

func mergeRowReaders(readers []parquet.RowReader, compare func(parquet.Row, parquet.Row) int) *mergedRowReader {
	return &mergedRowReader{
		compare: compare,
		readers: makeBufferedRowReaders(len(readers), func(i int) parquet.RowReader { return readers[i] }),
	}
}

func makeBufferedRowReaders(numReaders int, readerAt func(int) parquet.RowReader) []*bufferedRowReader {
	buffers := make([]bufferedRowReader, numReaders)
	readers := make([]*bufferedRowReader, numReaders)

	for i := range readers {
		buffers[i].rows = readerAt(i)
		readers[i] = &buffers[i]
	}
	return readers
}

func (m *mergedRowReader) initialize() error {
	for i, r := range m.readers {
		switch err := r.read(); err {
		case nil:
		case io.EOF:
			m.readers[i] = nil
		default:
			m.readers = nil
			return err
		}
	}

	n := 0
	for _, r := range m.readers {
		if r != nil {
			m.readers[n] = r
			n++
		}
	}

	toclear := m.readers[n:]
	for i := range toclear {
		toclear[i] = nil
	}

	m.readers = m.readers[:n]
	heap.Init(m)
	return nil
}

func (m *mergedRowReader) Close() {
	for _, r := range m.readers {
		r.close()
	}
	m.readers = nil
}

func (m *mergedRowReader) ReadRows(rows []parquet.Row) (n int, err error) {
	if !m.initialized {
		m.initialized = true

		if err := m.initialize(); err != nil {
			return 0, err
		}
	}
	for n < len(rows) && len(m.readers) != 0 {
		r := m.readers[0]
		h := r.head().Clone()

		rows[n] = slices.Grow(rows[n], len(h))[:len(h)]
		copy(rows[n], h)
		n++

		if err := r.next(); err != nil {
			if err != io.EOF {
				return n, err
			}
			heap.Pop(m)
		} else {
			heap.Fix(m, 0)
		}
	}

	if len(m.readers) == 0 {
		err = io.EOF
	}
	return n, err
}

func (m *mergedRowReader) Less(i, j int) bool {
	return m.compare(m.readers[i].head(), m.readers[j].head()) < 0
}

func (m *mergedRowReader) Len() int {
	return len(m.readers)
}

func (m *mergedRowReader) Swap(i, j int) {
	m.readers[i], m.readers[j] = m.readers[j], m.readers[i]
}

func (m *mergedRowReader) Push(_ interface{}) {
	panic("NOT IMPLEMENTED")
}

func (m *mergedRowReader) Pop() interface{} {
	i := len(m.readers) - 1
	r := m.readers[i]
	m.readers = m.readers[:i]
	return r
}

type bufferedRowReader struct {
	rows parquet.RowReader
	off  int32
	end  int32
	buf  [64]parquet.Row
}

func (r *bufferedRowReader) head() parquet.Row {
	return r.buf[r.off]
}

func (r *bufferedRowReader) next() error {
	if r.off++; r.off == r.end {
		r.off = 0
		r.end = 0
		return r.read()
	}
	return nil
}

func (r *bufferedRowReader) read() error {
	if r.rows == nil {
		return io.EOF
	}
	n, err := r.rows.ReadRows(r.buf[r.end:])
	if err != nil && n == 0 {
		return err
	}
	r.end += int32(n)
	return nil
}

func (r *bufferedRowReader) close() {
	r.rows = nil
	r.off = 0
	r.end = 0
}

func sizeFromSeeker(seek io.Seeker) (int64, error) {
	pos, err := seek.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	end, err := seek.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	size := end - pos
	pos1, err := seek.Seek(pos, io.SeekStart)
	if err != nil {
		return 0, err
	}
	if pos1 != pos {
		return 0, fmt.Errorf("unable to restore seek position: %d != %d", pos1, pos)
	}
	return size, nil
}

type readerAt struct {
	reader io.ReadSeeker
	offset int64
}

func (r *readerAt) ReadAt(b []byte, off int64) (int, error) {
	if r.offset < 0 || off != r.offset {
		off, err := r.reader.Seek(off, io.SeekStart)
		if err != nil {
			return 0, err
		}
		r.offset = off
	}
	n, err := r.reader.Read(b)
	r.offset += int64(n)
	return n, err
}

func newReaderAt(r io.ReadSeeker) io.ReaderAt {
	if rr, ok := r.(io.ReaderAt); ok {
		return rr
	}
	return &readerAt{reader: r, offset: -1}
}
