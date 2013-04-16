// RSync/RDiff implementation.
//
// Algorithm found at: http://www.samba.org/~tridge/phd_thesis.pdf
//
// Definitions
//   Source: The final content.
//   Target: The content to be made into final content.
//   Signature: The sequence of hashes used to identify the content.
package rsync

import (
	"bytes"
	"crypto/md5"
	"hash"
	"io"
)

// If no BlockSize is specified in the RSync instance, this value is used.
const DefaultBlockSize = 1024 * 64
const DefaultMaxDataOp = DefaultBlockSize * 10

// Internal constant used in rolling checksum.
const _M = 1 << 16

// Operation Types.
type OpType byte

const (
	BLOCK OpType = iota
	DATA
)

// Instruction to mutate target to align to source.
type Operation struct {
	Type       OpType
	BlockIndex uint64
	Data       []byte
}

// Signature hash item generated from target.
type BlockHash struct {
	Index      uint64
	StrongHash []byte
	WeakHash   uint32
}

// Write signatures as they are generated.
type SignatureWriter func(bl BlockHash) error

// Properties to use while working with the rsync algorithm.
type RSync struct {
	BlockSize int
	MaxDataOp int

	// If this is nil an MD5 hash is used.
	UniqueHasher hash.Hash
}

// If the target length is known the number of hashes in the
// signature can be determined.
func (r *RSync) BlockHashCount(targetLength int) (count int) {
	if r.BlockSize <= 0 {
		r.BlockSize = DefaultBlockSize
	}
	count = (targetLength / r.BlockSize)
	if targetLength%r.BlockSize != 0 {
		count++
	}
	return
}

// Calculate the signature of target.
func (r *RSync) CreateSignature(target io.Reader, sw SignatureWriter) error {
	if r.BlockSize <= 0 {
		r.BlockSize = DefaultBlockSize
	}
	if r.UniqueHasher == nil {
		r.UniqueHasher = md5.New()
	}
	var err error
	var n int
	buffer := make([]byte, r.BlockSize)
	var block []byte
	loop := true
	var index uint64
	for loop {
		n, err = io.ReadAtLeast(target, buffer, r.BlockSize)
		if err != nil {
			// n == 0
			if err == io.EOF {
				return nil
			}
			if err != io.ErrUnexpectedEOF {
				return err
			}
			// n > 0
			loop = false
		}
		block = buffer[:n]
		weak, _, _ := βhash(block)
		err = sw(BlockHash{StrongHash: r.uniqueHash(block), WeakHash: weak, Index: index})
		if err != nil {
			return err
		}
		index++
	}
	return nil
}

// Apply the difference to the target.
func (r *RSync) ApplyDelta(alignedTarget io.Writer, target io.ReadSeeker, ops chan Operation) error {
	if r.BlockSize <= 0 {
		r.BlockSize = DefaultBlockSize
	}
	var err error
	var n int
	var block []byte

	buffer := make([]byte, r.BlockSize)
	for op := range ops {
		switch op.Type {
		case BLOCK:
			target.Seek(int64(r.BlockSize*int(op.BlockIndex)), 0)
			n, err = io.ReadAtLeast(target, buffer, r.BlockSize)
			if err != nil {
				if err == io.EOF {
					break
				}
				if err != io.ErrUnexpectedEOF {
					return err
				}
			}
			block = buffer[:n]
			_, err = alignedTarget.Write(block)
			if err != nil {
				return err
			}
		case DATA:
			_, err = alignedTarget.Write(op.Data)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Create the operation list to mutate the target signature into the source.
func (r *RSync) CreateDelta(source io.Reader, signature []BlockHash, ops chan Operation) error {
	if r.BlockSize <= 0 {
		r.BlockSize = DefaultBlockSize
	}
	if r.MaxDataOp <= 0 {
		r.MaxDataOp = DefaultMaxDataOp
	}
	if r.UniqueHasher == nil {
		r.UniqueHasher = md5.New()
	}
	// A single β hashes may correlate with a many unique hashes.
	hashLookup := make(map[uint32][]BlockHash, len(signature))
	defer close(ops)
	for _, h := range signature {
		key := h.WeakHash
		hashLookup[key] = append(hashLookup[key], h)
	}

	type section struct {
		tail int
		head int
	}

	var err error
	var data, sum section
	var n, validTo, prevValidTo int
	var αPop, αPush, β, β1, β2 uint32
	var blockIndex uint64
	var rolling, dirty, lastRun, foundHash bool

	var buffer = make([]byte, (r.BlockSize*3)+r.MaxDataOp)

	for !lastRun {
		// First check for any data segments which have wrapped.
		if sum.head >= validTo {
			if validTo+r.BlockSize > len(buffer) {
				// Before wrapping the buffer, record the previous valid data section.
				prevValidTo = validTo

				// Now wrap the buffer.
				validTo = 0
				sum.tail = 0
			}
			n, err = io.ReadAtLeast(source, buffer[validTo:validTo+r.BlockSize], r.BlockSize)
			validTo += n
			if err != nil {
				if err != io.EOF && err != io.ErrUnexpectedEOF {
					return err
				}
				lastRun = true

				// Send any remaining data. This will be smaller then
				// a block size so it will never need to hash.
				dirty = true

				// May trigger "data wrap".
				data.head = validTo
			}
			if n == 0 {
				break
			}
		}
		if data.tail > data.head {
			ops <- Operation{Type: DATA, Data: buffer[data.tail:prevValidTo]}
			data.tail = 0
		}
		sum.head = min(sum.tail+r.BlockSize, validTo)

		if !rolling {
			β, β1, β2 = βhash(buffer[sum.tail:sum.head])
			rolling = true
		} else {
			αPush = uint32(buffer[sum.head-1])
			β1 = (β1 - αPop + αPush) % _M
			β2 = (β2 - uint32(sum.head-sum.tail)*αPop + β1) % _M
			β = β1 + _M*β2
		}
		foundHash = false
		if hh, ok := hashLookup[β]; ok && !lastRun {
			blockIndex, foundHash = findUniqueHash(hh, r.uniqueHash(buffer[sum.tail:sum.head]))
		}
		if dirty && (foundHash || data.head-data.tail >= r.MaxDataOp || lastRun) {
			ops <- Operation{Type: DATA, Data: buffer[data.tail:data.head]}
			dirty = false
			data.tail = data.head
		}
		if foundHash {
			ops <- Operation{Type: BLOCK, BlockIndex: blockIndex}
			data.tail = sum.head
			rolling = false
			sum.tail += r.BlockSize

			// May trigger "data wrap".
			data.head = sum.tail
		} else {
			dirty = true

			// The following is for the next loop iteration, so don't try to calculate if last.
			if !lastRun {
				αPop = uint32(buffer[sum.tail])
			}
			sum.tail += 1

			// May trigger "data wrap".
			data.head = sum.tail
		}
	}
	return nil
}

// Use a more unique way to identify a set of bytes.
func (r *RSync) uniqueHash(v []byte) []byte {
	r.UniqueHasher.Reset()
	r.UniqueHasher.Write(v)
	return r.UniqueHasher.Sum(nil)
}

// Searches for a given strong hash among all strong hashes in this bucket.
func findUniqueHash(hh []BlockHash, hashValue []byte) (uint64, bool) {
	if len(hashValue) == 0 {
		return 0, false
	}
	for _, block := range hh {
		if bytes.Equal(block.StrongHash, hashValue) {
			return block.Index, true
		}
	}
	return 0, false
}

// Use a faster way to identify a set of bytes.
func βhash(v []byte) (β uint32, β1 uint32, β2 uint32) {
	var a, b uint32
	for i := range v {
		a += uint32(v[i])
		b += (uint32(len(v)-1) - uint32(i) + 1) * uint32(v[i])
	}
	β = (a % _M) + (_M * (b % _M))
	β1 = a % _M
	β2 = b % _M
	return
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
