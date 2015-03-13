# Rkv - reliable kv

Rkv - embeddable KV database in go (golang). 

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

