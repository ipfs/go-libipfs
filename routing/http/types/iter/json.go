package iter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// FromReaderJSON returns an iterator over the given reader that reads whitespace-delimited JSON values.
func FromReaderJSON[T any](ctx context.Context, r io.Reader) *JSONIter[T] {
	return &JSONIter[T]{Decoder: json.NewDecoder(r), Reader: r, ctx: ctx}
}

// JSONIter iterates over whitespace-delimited JSON values of a byte stream.
// This closes the reader if it is a closer, to faciliate easy reading of HTTP responses.
type JSONIter[T any] struct {
	Decoder *json.Decoder
	Reader  io.Reader

	ctx  context.Context
	done bool
}

func (j *JSONIter[T]) Next() (T, bool, error) {
	var val T

	if j.done {
		return val, false, nil
	}

	if ctxErr := j.ctx.Err(); ctxErr != nil {
		j.close()
		return val, true, ctxErr
	}

	err := j.Decoder.Decode(&val)
	if errors.Is(err, io.EOF) {
		j.close()
		return val, false, nil
	}
	if err != nil {
		j.close()
		return val, true, fmt.Errorf("json iterator: %w", err)
	}

	return val, true, err
}

func (j *JSONIter[T]) close() {
	j.done = true
	if closer, ok := j.Reader.(io.Closer); ok {
		closer.Close()
	}
}
