package main

import (
	"fmt"
	"github.com/muirglacier/id"
	"os"
	"strconv"
)

func main() {
	argsWithProg := os.Args

	n, err := strconv.Atoi(argsWithProg[1])
	if err != nil {
		panic("Number of node given is not a number")
	}

	f, err := os.Create("../build/keys")
	if err != nil {
		panic(fmt.Sprintf("error creating file: %v", err))
	}

	defer func() {
		f.Sync()
		if err := f.Close(); err != nil {
			panic(err)
		}
	}()

	for i:=0; i<n; i++ {
		key := id.NewPrivKey()
		if _, err := f.Write([]byte(fmt.Sprintf("%v,%v,%v\n", key.D.String(), key.X.String(), key.Y.String()))); err != nil {
			panic(fmt.Sprintf("error writing to file: %v", err))
		}
	}
}
