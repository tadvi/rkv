package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/tadvi/rkv"
)

var usage = `
  Use redirection < to move CSV files in.

  Example: $ rkvcsv -key 2 test.kv < test.csv

  This will import csv file into the database.
  Key will be used as third field in the import.

  Tool assumes that first record of the input CSV file contains field names.

`

var Usage = func() {
	fmt.Fprintf(os.Stderr, "\nUsage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, usage)
	flag.PrintDefaults()
}

var key int // index of the output field

func init() {
	flag.IntVar(&key, "key", 0, "key field index for import or export")
}

func main() {
	flag.Usage = Usage
	flag.Parse()

	dbfile := flag.Arg(0)
	if len(dbfile) == 0 {
		log.Fatal("Missing db file name as first parameter with path to database file")
	}

	kv, err := rkv.New(dbfile)
	if err != nil {
		log.Fatal("Can not open database file")
	}
	defer kv.Close()

    fmt.Println("Key is: ", key)
	/*
    // at this point exporting into CSV is not supported
    // TODO: add exporting into CSV
    stat, err := os.Stdout.Stat()
	if err == nil && (stat.Mode()&os.ModeCharDevice) == 0 {

	}*/

	stat, err := os.Stdin.Stat()
	if err == nil && (stat.Mode()&os.ModeCharDevice) == 0 {
        if err = kv.ImportCSV(os.Stdin, key); err != nil {
			log.Fatal(err)
		}
	}

}
