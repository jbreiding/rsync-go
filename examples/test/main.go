package main

import (
	"io"
	"os"

	"github.com/d4z3x/rsync-go"
)

func main() {
	srcReader, _ := os.Open("data.old") // original file
	defer srcReader.Close()

	rs := &rsync.RSync{}

	// here we store the whole signature in a byte slice,
	// but it could just as well be sent over a network connection for example
	sig := make([]rsync.BlockHash, 0, 10)
	writeSignature := func(bl rsync.BlockHash) error {
		sig = append(sig, bl)
		return nil
	}

	rs.CreateSignature(srcReader, writeSignature)
	targetReader, err := os.Open("data.new") // updated file
	if err != nil {
		panic(err)
	}
	opsOut := make(chan rsync.Operation)
	writeOperation := func(op rsync.Operation) error {
		opsOut <- op
		// log.Printf("%+q", op)
		return nil
	}

	go func() {
		defer close(opsOut)
		rs.CreateDelta(targetReader, sig, writeOperation)
	}()

	srcWriter, err := os.Create("data-reconstructed")
	if err != nil {
		panic(err)
	}

	srcReader.Seek(0, io.SeekStart)
	rs.ApplyDelta(srcWriter, srcReader, opsOut)
	defer srcWriter.Close()
}
