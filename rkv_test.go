package rkv

import (
	"os"
	"strconv"
	"testing"
)

const (
	testdb   = "test.kv"
	testjson = "test.json"
)

func TestRkv(t *testing.T) {
    var kv Interface
    
    for _, fn := range []func(t *testing.T) Interface { Open, OpenSafe } {
        kv = fn(t)
        Close(t, kv)

        kv = fn(t)
        ExportImport(t, kv)

        kv = fn(t)
        Append(t, kv)

        kv = fn(t)
        Fetch(t, kv)
    }

    // test only Rkv specific functions
    rkvdb := Open(t).(*Rkv)
    Iterator(t, rkvdb)
}

func Open(t *testing.T) Interface {
    kv, err := New(testdb)
	if err != nil {
		t.Fatal("Can not open database file")
	}
	return kv
}

func OpenSafe(t *testing.T) Interface {
    kv, err := NewSafe(testdb)
	if err != nil {
		t.Fatal("Can not open database file")
	}
	return kv
}

func Close(t *testing.T, kv Interface) {
	kv.Close()
	os.Remove(testdb) // clean up after test execution
}

func Append(t *testing.T, kv Interface) {

	type Mytype struct {
		Name string
		Pos  int
	}

    var err error
	total := 10
	for i := 0; i < total; i++ {
		data1 := Mytype{Name: "one", Pos: 1}
		data2 := Mytype{Name: "two", Pos: 2}
		data3 := Mytype{Name: "three", Pos: 3}

		key1 := "key1_" + strconv.Itoa(i)
		err = kv.PutForDays(key1, &data1, 2)
		if err != nil {
			t.Errorf("Error \"%q\" while puting the key: \"%q\"", err.Error(), key1)
		}

		key2 := "key2_" + strconv.Itoa(i)
		err = kv.PutForDays(key2, &data2, 3)
		if err != nil {
			t.Errorf("Error \"%q\" while puting the key: \"%q\"", err.Error(), key2)
		}

		key3 := "key3_" + strconv.Itoa(i)
		err = kv.Put(key3, &data3)
		if err != nil {
			t.Errorf("Error \"%q\" while puting the key: \"%q\" with value: \"%q\"", err.Error(), key3)
		}
	}
	kv.Close()

	kv.Reopen()
	dat := Mytype{}
	for i := 0; i < total; i++ {
		key1 := "key1_" + strconv.Itoa(i)
		err := kv.Get(key1, &dat)
		if err != nil {
			t.Errorf("Error \"%q\" while getting the key: \"%q\"", err.Error(), key1)
		}
		key2 := "key2_" + strconv.Itoa(i)
		err = kv.Get(key2, &dat)
		if err != nil {
			t.Errorf("Error \"%q\" while getting the key: \"%q\"", err.Error(), key2)
		}
		key3 := "key3_" + strconv.Itoa(i)
		err = kv.Get(key3, &dat)
		if err != nil {
			t.Errorf("Error \"%q\" while getting the key: \"%q\"", err.Error(), key3)
		}
	}

	kv.Close()
	os.Remove(testdb)
}

func ExportImport(t *testing.T, kv Interface) {

	type Mytype struct {
		Name string
		Pos  int
	}

    var err error
	total := 10
	for i := 0; i < total; i++ {
		data1 := Mytype{Name: "one", Pos: 1}
		data2 := Mytype{Name: "two", Pos: 2}
		data3 := Mytype{Name: "three", Pos: 3}

		key1 := "key1_" + strconv.Itoa(i)
		err = kv.PutForDays(key1, &data1, 2)
		if err != nil {
			t.Errorf("Error \"%q\" while puting the key: \"%q\"", err.Error(), key1)
		}

		key2 := "key2_" + strconv.Itoa(i)
		err = kv.PutForDays(key2, &data2, 3)
		if err != nil {
			t.Errorf("Error \"%q\" while puting the key: \"%q\"", err.Error(), key2)
		}

		key3 := "key3_" + strconv.Itoa(i)
		err = kv.Put(key3, &data3)
		if err != nil {
			t.Errorf("Error \"%q\" while puting the key: \"%q\" with value: \"%q\"", err.Error(), key3)
		}
	}

	f, err := os.Create(testjson)
	if err != nil {
		t.Errorf("Error \"%q\" ", err.Error())
	}
	kv.ExportJSON(f)
	kv.Close()

	kv.Reopen()
	f, err = os.Open(testjson)
	if err != nil {
		t.Errorf("Error \"%q\" ", err.Error())
	}
	kv.ImportJSON(f)

	dat := Mytype{}
	for i := 0; i < total; i++ {
		key1 := "key1_" + strconv.Itoa(i)
		err := kv.Get(key1, &dat)
		if err != nil {
			t.Errorf("Error \"%q\" while getting the key: \"%q\"", err.Error(), key1)
		}
		key2 := "key2_" + strconv.Itoa(i)
		err = kv.Get(key2, &dat)
		if err != nil {
			t.Errorf("Error \"%q\" while getting the key: \"%q\"", err.Error(), key2)
		}
		key3 := "key3_" + strconv.Itoa(i)
		err = kv.Get(key3, &dat)
		if err != nil {
			t.Errorf("Error \"%q\" while getting the key: \"%q\"", err.Error(), key3)
		}
	}

	kv.Close()
	os.Remove(testjson)
	os.Remove(testdb)
}

func Fetch(t *testing.T, kv Interface) {
    var err error
	type Mytype struct {
		Name string
		Pos  int
	}

	total := 10
	for i := 0; i < total; i++ {
		data1 := Mytype{Name: "one", Pos: 1}
		data2 := Mytype{Name: "two", Pos: 2}
		data3 := Mytype{Name: "three", Pos: 3}

		key1 := "key1_" + strconv.Itoa(i)
		err = kv.PutForDays(key1, &data1, 2)
		if err != nil {
			t.Errorf("Error \"%q\" while puting the key: \"%q\"", err.Error(), key1)
		}

		key2 := "key2_" + strconv.Itoa(i)
		err = kv.PutForDays(key2, &data2, 3)
		if err != nil {
			t.Errorf("Error \"%q\" while puting the key: \"%q\"", err.Error(), key2)
		}

		key3 := "key3_" + strconv.Itoa(i)
		err = kv.Put(key3, &data3)
		if err != nil {
			t.Errorf("Error \"%q\" while puting the key: \"%q\" with value: \"%q\"", err.Error(), key3)
		}
	}
	kv.Close()

	kv.Reopen()
	arr := kv.GetKeys("", -1)	
	count := 0
	for _ = range arr {
		count += 1
	}
	if count != total*3 {
		t.Error("GetKeys count is wrong. Should be", total*3, "Found", count)
	}
    
	kv.Close()
	os.Remove(testdb)
}

func Iterator(t *testing.T, kv *Rkv) {
    var err error
	type Mytype struct {
		Name string
        LastName string
		Pos  int
	}

	total := 10
	for i := 0; i < total; i++ {
		data1 := Mytype{Name: "one", LastName: "bob", Pos: 1}
		data2 := Mytype{Name: "two", LastName: "chuck" , Pos: 2}
		data3 := Mytype{Name: "three", LastName: "norris", Pos: 3}

		key1 := "key1_" + strconv.Itoa(i)
		err = kv.PutForDays(key1, &data1, 2)
		if err != nil {
			t.Errorf("Error \"%q\" while puting the key: \"%q\"", err.Error(), key1)
		}

		key2 := "key2_" + strconv.Itoa(i)
		err = kv.PutForDays(key2, &data2, 3)
		if err != nil {
			t.Errorf("Error \"%q\" while puting the key: \"%q\"", err.Error(), key2)
		}

		key3 := "key3_" + strconv.Itoa(i)
		err = kv.Put(key3, &data3)
		if err != nil {
			t.Errorf("Error \"%q\" while puting the key: \"%q\" with value: \"%q\"", err.Error(), key3)
		}
	}
	kv.Close()

	kv.Reopen()
	count := 0
	for key := range kv.Iterator("") {
        count += 1
        v := Mytype{} 
        err := kv.Get(key, &v)
        if err != nil {
            t.Errorf("Error while iterating %q", err.Error())
        }
        if !(v.Pos > 0 && v.Pos < 4) {
            t.Errorf("Pos member has to be between 1 and 3 inclusive")
        } 
    }
    if count != total * 3 {
	   t.Error("Iterator count is wrong. Should be", total*3, "Found", count)
    }    

	kv.Close()
	os.Remove(testdb)   
}
