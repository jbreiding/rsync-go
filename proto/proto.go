// A binary protocol for rsync.
package proto

import (
	"bitbucket.org/kardianos/rsync"
	"bitbucket.org/kardianos/rsync/sbuffer"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type Type byte
type Comp byte

const (
	// Unique key written in every header to identify and confirm the stream.
	RsyncMagic uint32 = 0x72730136
)

const (
	TypeSignature Type = 1
	TypeDelta     Type = 2
	TypePatch     Type = 3
)

const (
	CompNone Comp = 0
	CompGZip Comp = 1
)

var (
	ErrBadMagic           = errors.New("Corrupt or incorrect data: bad magic value in stream.")
	ErrUnknownCompression = errors.New("Unknown compression.")
	ErrInvalidCall        = errors.New("Cannot call function while reading from set type.")
	ErrHeaderOnce         = errors.New("Must call Header only once.")
	ErrBadVarintRead      = errors.New("Bad varint read.")
)

type ErrIncorrectType struct {
	Expecting, Actual Type
}

func (err ErrIncorrectType) Error() string {
	return fmt.Sprintf("Incorrect type. Expecting %d, got %d", err.Expecting, err.Actual)
}

type ErrHashTooLong int

func (err ErrHashTooLong) Error() string {
	return fmt.Sprintf("Hash length too long. Length: %d, max: %d.", int(err), maxHashLength)
}

type ErrDataTooLong int

func (err ErrDataTooLong) Error() string {
	return fmt.Sprintf("Hash length too long. Length: %d, max: %d.", int(err), maxDataLength)
}

const (
	maxHashLength = 1024 * 6
	maxDataLength = 1024 * 1024
)

func writeHeader(w io.Writer, blockSize int, t Type, compression Comp) error {
	var err error
	err = binary.Write(w, binary.BigEndian, RsyncMagic)
	if err != nil {
		return err
	}
	err = binary.Write(w, binary.BigEndian, byte(t))
	if err != nil {
		return err
	}
	err = binary.Write(w, binary.BigEndian, byte(compression))
	if err != nil {
		return err
	}
	err = binary.Write(w, binary.BigEndian, uint32(blockSize))
	if err != nil {
		return err
	}
	return nil
}

func readHeader(r io.Reader, expect Type) (blockSize int, compression Comp, err error) {
	var magic uint32
	var t, comp byte
	err = binary.Read(r, binary.BigEndian, &magic)
	if err != nil {
		return
	}

	if magic != RsyncMagic {
		err = ErrBadMagic
		return
	}

	err = binary.Read(r, binary.BigEndian, &t)
	if err != nil {
		return
	}
	if t != byte(expect) {
		err = ErrIncorrectType{
			Expecting: expect,
			Actual:    Type(t),
		}
		return
	}

	err = binary.Read(r, binary.BigEndian, &comp)
	if err != nil {
		return
	}
	compression = Comp(comp)

	var blockSizeRead uint32
	err = binary.Read(r, binary.BigEndian, &blockSizeRead)
	if err != nil {
		return
	}
	blockSize = int(blockSizeRead)
	return
}

// Write the protocol to a stream by setting the Writer
// and calling its methods subsequently.
type Writer struct {
	io.Writer

	hasHeader bool
	t         Type
	body      io.Writer
}

func (w *Writer) Close() error {
	if closer, ok := w.body.(io.WriteCloser); ok {
		return closer.Close()
	}
	return nil
}

// The header must be written before any content may be written.
func (w *Writer) Header(t Type, compression Comp, blockSize int) error {
	if w.hasHeader {
		return ErrHeaderOnce
	}
	w.t = t
	w.hasHeader = true
	var err error
	err = writeHeader(w, blockSize, t, compression)
	if err != nil {
		return err
	}

	switch compression {
	case CompNone:
		w.body = w.Writer
	case CompGZip:
		w.body, err = gzip.NewWriterLevel(w.Writer, gzip.BestCompression)
		if err != nil {
			return err
		}
	default:
		return ErrUnknownCompression
	}
	return nil
}

// Return a signature writer. The call itself does not write anything.
// Use with a Signature header.
func (w *Writer) SignatureWriter() rsync.SignatureWriter {
	if w.t != TypeSignature {
		// This is a program structure issue, so panic.
		panic(ErrInvalidCall)
	}
	buffer := make([]byte, 2048)
	return func(block rsync.BlockHash) error {
		var n int
		var err error
		n = binary.PutUvarint(buffer, block.Index)
		binary.BigEndian.PutUint32(buffer[n:], block.WeakHash)
		n += 4
		n += binary.PutUvarint(buffer[n:], uint64(len(block.StrongHash)))

		_, err = w.body.Write(buffer[:n])
		if err != nil {
			return err
		}
		_, err = w.body.Write(block.StrongHash)
		if err != nil {
			return err
		}
		return nil
	}
}

// Return an operation writer. The call itself does not write anything.
// Use with a Delta header.
func (w *Writer) OperationWriter() rsync.OperationWriter {
	if w.t != TypeDelta {
		// This is a program structure issue, so panic.
		panic(ErrInvalidCall)
	}
	buffer := make([]byte, 2048)
	return func(op rsync.Operation) error {
		var n int
		var err error
		buffer[0] = byte(op.Type)
		n = 1
		switch op.Type {
		case rsync.OpBlock:
			n += binary.PutUvarint(buffer[n:], op.BlockIndex)
			_, err = w.body.Write(buffer[:n])
			if err != nil {
				return err
			}
		case rsync.OpBlockRange:
			n += binary.PutUvarint(buffer[n:], op.BlockIndex)
			n += binary.PutUvarint(buffer[n:], op.BlockIndexEnd)
			_, err = w.body.Write(buffer[:n])
			if err != nil {
				return err
			}
		case rsync.OpHash:
			fallthrough
		case rsync.OpData:
			n += binary.PutUvarint(buffer[n:], uint64(len(op.Data)))
			_, err = w.body.Write(buffer[:n])
			if err != nil {
				return err
			}
			_, err = w.body.Write(op.Data)
			if err != nil {
				return err
			}
		default:
			panic("Unreachable.")
		}
		return nil
	}
}

// Initialize by setting the Reader. Call Header to initialize stream.
type Reader struct {
	io.Reader

	hasHeader bool
	t         Type
	body      io.Reader
}

func (r *Reader) Close() error {
	if closer, ok := r.body.(io.ReadCloser); ok {
		return closer.Close()
	}
	return nil
}

func (r *Reader) Header(t Type) (blockSize int, err error) {
	if r.hasHeader {
		err = ErrHeaderOnce
		return
	}
	r.t = t
	r.hasHeader = true

	var comp Comp
	blockSize, comp, err = readHeader(r, t)
	if err != nil {
		return blockSize, err
	}

	switch comp {
	case CompNone:
		r.body = r.Reader
	case CompGZip:
		r.body, err = gzip.NewReader(r.Reader)
		if err != nil {
			return
		}
	default:
		err = ErrUnknownCompression
		return
	}

	return
}

func (r *Reader) ReadAllSignatures() ([]rsync.BlockHash, error) {
	if r.t != TypeSignature {
		// This is a program structure issue, so panic.
		panic(ErrInvalidCall)
	}

	bb := make([]rsync.BlockHash, 0, 10)

	// Approx size of header, which uses varint encoding.
	leaveTail := 24

	var v uint64
	var n, at, hashLen int

	reader := sbuffer.NewBuffer(r.body, 32*1024)

	loop := true
	for loop {
		buff, err := reader.Next(leaveTail)
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			loop = false
		}
		if len(buff) == 0 {
			break
		}

		v, n = binary.Uvarint(buff)
		if n <= 0 {
			panic(ErrBadVarintRead)
		}
		at = n

		block := rsync.BlockHash{
			Index:    v,
			WeakHash: binary.BigEndian.Uint32(buff[at:]),
		}
		at += 4
		v, n = binary.Uvarint(buff[at:])
		if n <= 0 {
			panic(ErrBadVarintRead)
		}
		at += n

		hashLen = int(v)
		if hashLen > maxHashLength {
			err = ErrHashTooLong(hashLen)
			panic(err)
		}
		reader.Used(at)

		buff, err = reader.Next(hashLen)
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			loop = false
		}
		block.StrongHash = make([]byte, hashLen)
		copy(block.StrongHash, buff[:hashLen])

		reader.Used(hashLen)

		bb = append(bb, block)
	}

	return bb, nil
}

func (r *Reader) ReadOperations(ops chan rsync.Operation, hashOps chan rsync.Operation) error {
	if r.t != TypeDelta {
		// This is a program structure issue, so panic.
		panic(ErrInvalidCall)
	}

	var v uint64
	var n, at, dataLen int

	reader := sbuffer.NewBuffer(r.body, 32*1024)

	loop := true
	for loop {
		buff, err := reader.Next(10)
		if len(buff) == 0 {
			return nil
		}
		if err != nil {
			if err != io.EOF {
				return err
			}
			loop = false
		}

		op := rsync.Operation{
			Type: rsync.OpType(buff[0]),
		}

		at = 1
		switch op.Type {
		case rsync.OpBlock:
			v, n = binary.Uvarint(buff[at:])
			if n <= 0 {
				panic(ErrBadVarintRead)
			}
			at += n
			op.BlockIndex = v
			reader.Used(at)
		case rsync.OpBlockRange:
			v, n = binary.Uvarint(buff[at:])
			if n <= 0 {
				panic(ErrBadVarintRead)
			}
			at += n
			op.BlockIndex = v

			v, n = binary.Uvarint(buff[at:])
			if n <= 0 {
				panic(ErrBadVarintRead)
			}
			at += n
			op.BlockIndexEnd = v
			reader.Used(at)
		case rsync.OpHash:
			fallthrough
		case rsync.OpData:
			v, n = binary.Uvarint(buff[at:])
			if n <= 0 {
				panic(ErrBadVarintRead)
			}
			at += n
			dataLen = int(v)
			if dataLen > maxDataLength {
				err = ErrDataTooLong(dataLen)
				panic(err)
			}
			reader.Used(at)

			buff, err := reader.Next(dataLen)
			if err != nil {
				if err != io.EOF {
					return err
				}
				loop = false
			}
			op.Data = make([]byte, dataLen)
			copy(op.Data, buff[:dataLen])

			reader.Used(dataLen)
		default:
			panic("Unreachable.")
		}

		if op.Type == rsync.OpHash {
			hashOps <- op
		} else {
			ops <- op
		}
	}

	return nil
}
