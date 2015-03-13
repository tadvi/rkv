package main

import (
	"flag"
	"fmt"
	"github.com/tadvi/rkv"
	"log"
	"os"
)

var compact bool   // compact database flag
	
var usage = `
  Use redirection < or > to move JSON files in or out.

  Example: $ rkv -c test.kv > test.json
  This will compact database and output to test.json   

`

var Usage = func() {
    fmt.Fprintf(os.Stderr, "\nUsage of %s:\n", os.Args[0])
    fmt.Fprintf(os.Stderr, usage)    
    flag.PrintDefaults()  
}

func init() {	
	flag.BoolVar(&compact, "c", false, "compact database flag")
    flag.BoolVar(&compact, "compact", false, "compact database flag long")
	//flag.StringVar(&keys, "k", "", "print database keys that contain value or all keys if *")
    //flag.StringVar(&keys, "-keys", "", "print database keys that contain value or all keys if *")	
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

	if compact {
		log.Println("Compacting...")
		err = kv.Compact()
		if err != nil {
			log.Fatalf("%v", err)
		}
		log.Println("Database fill ratio:", kv.FillRatio)
		log.Println("Capacity:", kv.CapKeys, "number of keys:", kv.LenKeys)
	}
	
    stat, err := os.Stdout.Stat()
    if err == nil && (stat.Mode() & os.ModeCharDevice) == 0 {        
        err = kv.ExportJSON(os.Stdout)
		if err != nil {
            log.Fatal(err)
        }
    }
	
	stat, err = os.Stdin.Stat()
    if err == nil && (stat.Mode() & os.ModeCharDevice) == 0 {
        //fmt.Println("data is being piped to stdin")
       	err = kv.ImportJSON(os.Stdin)
		if err != nil {
		    log.Fatal(err)
	    }
    }		

	/*
    TODO: enable this functionality
    if keys != "" {
		if keys == "*" {
			keys = ""
		}
		arr := kv.GetKeys(keys, -1)
		for _, key := range arr {
            fmt.Println(key)
            b, err := kv.GetBytes(key)
            if err != nil {
                log.Fatal(err)
            }
			fmt.Println(b)
            fmt.Println()
		}
	}*/

}
