package alitablestore

import (
	"context"
	"io"

	"gocloud.dev/docstore/driver"
)

type documentIterator struct {
	rows []map[string]interface{}
	idx int
}


func (it *documentIterator) Next(ctx context.Context, doc driver.Document) error {
	if it.idx >= len(it.rows) {
		return io.EOF
	}
	err := doc.Decode(decoder{val: it.rows[it.idx]})
	if err != nil {
		return err
	}
	it.idx++
	return nil
}

func (it *documentIterator) Stop() {

}

func (it *documentIterator) As(i interface{}) bool {
	return true
}