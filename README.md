# Rkv - reliable kv

Rkv - embeddable KV database in Go (golang). 

* Based on Riak bitcast format
* Minimalistic design
* Embeddable and self-contained (no C dependencies) 
* Contains both direct and goroutine friendly interfaces
* Use rkv.NewSafe("test.kv") if you want to use with goroutines
* Basic KV admin tool is included in /rkv subfolder, build it and install in your bin folder
* Ability to save records with expiration 
* Use Rkv for databases under 50K records

Basic usage:

    package main

    import (
    	"log"
    	"strconv"
    
    	"github.com/tadvi/rkv"
    )
    
    func main() {
    	kv, err := rkv.New("test.kv")
    	if err != nil {
    		log.Fatal("Can not open database file")
    	}
    	defer kv.Close()
    
    	type Mytype struct {
    		Name string
    		Pos  int
    	}
    
    	total := 10
    	for i := 0; i < total; i++ {
    		data1 := Mytype{Name: "one", Pos: 1}
    		data2 := Mytype{Name: "two", Pos: 2}
    		data3 := Mytype{Name: "three", Pos: 3}
    
    		// this record will be saved for 2 days
    		key1 := "key1_" + strconv.Itoa(i)
    		err = kv.PutForDays(key1, &data1, 2)
    		if err != nil {
    			log.Fatal(err)
    		}
    
    		// this record will be saved for 3 days
    		key2 := "key2_" + strconv.Itoa(i)
    		err = kv.PutForDays(key2, &data2, 3)
    		if err != nil {
    			log.Fatal(err)
    		}
    
    		// this record will be saved until removed
    		key3 := "key3_" + strconv.Itoa(i)
    		err = kv.Put(key3, &data3)
    		if err != nil {
    			log.Fatal(err)
    		}
    	}
    }

## Use rkv tool

Use rkv tool to export or import data.
Exported data is in JSON format. This makes it easier to move data around 
since so many other tools talk JSON.

You can also compact database files with rkv tool.

Basic usage:

$ rkv -c test.kv > test.json

This will compact database and export to JSON.

$ rkv test.kv > test.json 

This simply exports database

$ rkv test.kv < test.json

Imports exported database

## TODO

* Change to use RWMutex instead of Mutex 
* Add better iteration techniques
* Add more test cases

