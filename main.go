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
`, os.Args[0], os.Args[0], os.Args[0])
	flag.PrintDefaults()
}

func getRsync() *rsync.RSync {
	return &rsync.RSync{}
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
