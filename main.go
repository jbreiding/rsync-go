// RDiff clone.
//
// A replacement for the aging http://librsync.sourcefrog.net
// rdiff utility.
package main

import (
	"bitbucket.org/kardianos/rdiff/rsync"

	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	// "github.com/dchest/blake2b"
)

func main() {
	flag.Parse()

	var verb = strings.ToLower(flag.Arg(0))
	if len(verb) == 0 {
		log.Printf("Error: Must provide a verb.")
		printHelp()
		os.Exit(1)
	}
	var err error
	switch verb {
	case "signature":
		err = signature(flag.Arg(1), flag.Arg(2))
	case "delta":
		err = delta(flag.Arg(1), flag.Arg(2), flag.Arg(3))
	case "patch":
		err = patch(flag.Arg(1), flag.Arg(2), flag.Arg(3))
	case "test":
		err = test(flag.Arg(1), flag.Arg(2))
	default:
		log.Printf("Error: Unrecognized verb: %s", verb)
		printHelp()
		os.Exit(1)
	}
	if err != nil {
		log.Printf("Error running %s: %s", verb, err)
		os.Exit(2)
	}
}
func printHelp() {
	fmt.Printf(`
%s signature BASIS SIGNATURE
%s delta SIGNATURE NEWFILE DELTA
%s patch BASIS DELTA NEWFILE
%s test BASIS BASISv2
`, os.Args[0], os.Args[0], os.Args[0], os.Args[0])
	flag.PrintDefaults()
}

func getRsync() *rsync.RSync {
	return &rsync.RSync{
		BlockSize: 1024 * 6,
	}
}

func signature(basis, signature string) error {
	rs := getRsync()
	basisFile, err := os.Open(basis)
	if err != nil {
		return err
	}
	defer basisFile.Close()

	sigFile, err := os.Create(signature)
	if err != nil {
		return err
	}
	defer sigFile.Close()

	sigEncode := gob.NewEncoder(sigFile)

	return rs.CreateSignature(basisFile, func(block rsync.BlockHash) error {
		// Save signature hash list to file.
		return sigEncode.Encode(block)
	})
}

func delta(signature, newfile, delta string) error {
	rs := getRsync()
	sigFile, err := os.Open(signature)
	if err != nil {
		return err
	}
	defer sigFile.Close()

	nfFile, err := os.Open(newfile)
	if err != nil {
		return err
	}
	defer nfFile.Close()

	deltaFile, err := os.Create(delta)
	if err != nil {
		return err
	}
	defer deltaFile.Close()

	// Load signature hash list.
	hl := make([]rsync.BlockHash, 0)
	sigDecode := gob.NewDecoder(sigFile)
	for {
		bl := rsync.BlockHash{}
		err = sigDecode.Decode(&bl)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		hl = append(hl, bl)
	}

	ops := make(chan rsync.Operation)
	// Save operations to file.
	opsEncode := gob.NewEncoder(deltaFile)
	go func() {
		for op := range ops {
			opsEncode.Encode(op)
		}
	}()

	return rs.CreateDelta(nfFile, hl, ops)
}

func patch(basis, delta, newfile string) error {
	rs := getRsync()
	basisFile, err := os.Open(basis)
	if err != nil {
		return err
	}
	defer basisFile.Close()

	deltaFile, err := os.Open(delta)
	if err != nil {
		return err
	}
	defer deltaFile.Close()

	fsFile, err := os.Create(newfile)
	if err != nil {
		return err
	}
	defer fsFile.Close()

	ops := make(chan rsync.Operation)
	// Load operations from file.
	var decodeError error
	go func() {
		defer close(ops)
		deltaDecode := gob.NewDecoder(deltaFile)
		for {
			op := rsync.Operation{}
			err = deltaDecode.Decode(&op)
			if err == io.EOF {
				break
			}
			if err != nil {
				decodeError = err
				return
			}
			ops <- op
		}
	}()

	err = rs.ApplyDelta(fsFile, basisFile, ops)
	if err != nil {
		return err
	}
	if decodeError != nil {
		return decodeError
	}
	return nil
}

func test(basis1, basis2 string) error {
	basis1File, err := os.Open(basis1)
	if err != nil {
		return err
	}
	defer basis1File.Close()

	basis2File, err := os.Open(basis2)
	if err != nil {
		return err
	}
	defer basis2File.Close()

	basis1Stat, err := basis1File.Stat()
	if err != nil {
		return err
	}
	basis2Stat, err := basis2File.Stat()
	if err != nil {
		return err
	}

	if basis1Stat.Size() != basis2Stat.Size() {
		log.Printf("FAIL: File size different.")
		return nil
	}

	type resetBuffer struct {
		orig, buf []byte
	}

	bufferFount := make(chan resetBuffer, 30)

	b1Source := make(chan resetBuffer, 10)
	b2Source := make(chan resetBuffer, 10)

	for i := 0; i < cap(bufferFount); i++ {
		b := make([]byte, 32*1024)

		bufferFount <- resetBuffer{
			orig: b,
			buf:  b,
		}
	}

	reader := func(f io.Reader, source chan resetBuffer) {
		for {
			buffer := <-bufferFount
			buffer.buf = buffer.orig
			n, err := f.Read(buffer.orig)
			if n == 0 {
				bufferFount <- buffer
			} else {
				buffer.buf = buffer.orig[:n]
				source <- buffer
			}
			if err != nil {
				if err == io.EOF {
					close(source)
					return
				}
				log.Fatalf("Error reading file: %s", err)
			}
		}
	}

	go reader(basis1File, b1Source)
	go reader(basis2File, b2Source)

	location := 0
	var b1Buffer resetBuffer
	var b2Buffer resetBuffer
	var ok bool
	for {
		if len(b1Buffer.buf) == 0 {
			if b1Buffer.buf != nil {
				bufferFount <- b1Buffer
			}
			b1Buffer, ok = <-b1Source
			if !ok {
				return nil
			}
		}
		if len(b2Buffer.buf) == 0 {
			if b2Buffer.buf != nil {
				bufferFount <- b2Buffer
			}
			b2Buffer, ok = <-b2Source
			if !ok {
				return nil
			}
		}
		size := min(len(b1Buffer.buf), len(b2Buffer.buf))

		for i := 0; i < size; i++ {
			if b1Buffer.buf[i] != b2Buffer.buf[i] {
				log.Printf("FAIL: Bytes differ at %d.", location)
				return nil
			}
			location++
		}
		b1Buffer.buf = b1Buffer.buf[size:]
		b2Buffer.buf = b2Buffer.buf[size:]
	}
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
