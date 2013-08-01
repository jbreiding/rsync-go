// Single-Buffer backing for readers.
//
// Use when reading from a large Reader when only small defined
// sequential slices are needed. Uses a single buffer for reading.
package sbuffer

import (
	"errors"
	"io"
)

var (
	ErrNeedCap     = errors.New("Requested more then buffer size.")
	ErrUsedTooMuch = errors.New("Used more then requested.")
)

type buffer struct {
	io.Reader
	backer []byte

	tail, head int
}

type Buffer interface {
	// Get the next slice of data. Will return at least "needed", but may return more.
	// The returned slice should not be used after Next or Used is called.
	// The returned slice may be shorter then needed if the inner read returns a short
	// read (for example, returns an io.EOF).
	Next(needed int) ([]byte, error)

	// Called after using the slice returned from Next. This frees
	// the underlying buffer for more data.
	Used(used int)
}

// Read from read for more data.
// The bufferSize should be several times the max read size to prevent excessive copying.
func NewBuffer(read io.Reader, bufferSize int) Buffer {
	return &buffer{
		Reader: read,
		backer: make([]byte, bufferSize),
	}
}

func (b *buffer) Next(needed int) ([]byte, error) {
	if needed > len(b.backer) {
		panic(ErrNeedCap)
	}
	if b.tail+needed > len(b.backer) {
		// Copy end of tail to beginning of buffer.
		block := b.backer[b.tail:b.head]
		copy(b.backer, block)
		b.tail = 0
		b.head = len(block)
	}
	var err error
	var n int
	for b.tail+needed > b.head {
		// Read more data.
		n, err = b.Read(b.backer[b.head:])
		b.head += n
		if err != nil {
			break
		}
	}
	return b.backer[b.tail:min(b.tail+needed, b.head)], err
}

func (b *buffer) Used(used int) {
	b.tail += used
	if b.tail > b.head {
		panic(ErrUsedTooMuch)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
