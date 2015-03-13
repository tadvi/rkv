package rkv

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"time"
)

const (
	/* Header offset for each record in the store.
	This offset contains the following information (in the given order)
	| -------------------------------------------------------------------------|
	| crc (int32) | tstamp (int32) | key length (int32) | value length (int32) |
	| -------------------------------------------------------------------------|
	*/
	RecordHeaderSize int32 = 16
	MinCapKeys             = 1000
)

var (
	ErrBlankKey    = errors.New("rkv: key can not be blank")
	ErrKeyNotFound = errors.New("rkv: key not found")
)

// Main structure for any Rkv file.
// Holds the current directory and the active file
type Rkv struct {
	filename   string
	activeFile *GFile
	keydir     *Keydir

	// values below are calculated only when store is open, they are not updated on Delete or Put
	FillRatio float64 // active records divided by dead-removed records, used for AutoCompact
	CapKeys   int     // total number of keys = alive + dead
	LenKeys   int     // number of keys = alive
}

var _ Interface = (*Rkv)(nil)

// GFile wrap a os.file and provide some convenient methods.
type GFile struct {
	file *os.File
	cpos int32
}

// KeydirEntry entries in the keydir, which holds the location of any key in the key-store.
type KeydirEntry struct {
	gfile  *GFile
	vsz    int32
	vpos   int32
	tstamp int64
}

// Keydir in memory structure that holds the location of all the keys in the key-value store.
type Keydir struct {
	keys map[string]*KeydirEntry
}

// NewRkv open the key-value store at the given file.
// If the file doesn't exist one will be created.
// Always try to open for read-write, but if someone is already using this file, open in read-only mode.
// Populate the KeyDir structure with the information obtained from the data file.
func New(filename string) (*Rkv, error) {
	kv := new(Rkv)
	kv.filename = filename
	kv.FillRatio = 1
	return kv.open()
}

// Reopen KV store.
func (kv *Rkv) Reopen() error {
    _, err := kv.open()
    return err
}

// Close the key-value store.
func (kv *Rkv) Close() {
	kv.isReady()
	if kv.activeFile != nil {
		kv.activeFile.file.Close()
	}
}

// AutoCompact auto compacts database once active records divided
// by dead-removed records (fill ratio) drops below fillRatio
// and there are enough alive and dead keys expressed as MinCapKeys.
func (kv *Rkv) AutoCompact(fillRatio float64) error {
	if kv.FillRatio < fillRatio && kv.CapKeys > MinCapKeys {
		return kv.Compact()
	}
	return nil
}

// Compact database.
func (kv *Rkv) Compact() error {
	temp := kv.filename + "~"
	compact, err := New(temp)
	if err != nil {
		return err
	}

	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	kv.isReady()
	for key, kde := range kv.keydir.keys {
		val, err := kde.readValue()
		if err != nil {
			return err
		}
		if err = compact.putRaw(key, val); err != nil {
			return err
		}
	}
	kv.Close()
	compact.Close()

	// move temp file and replace kv.filename
	if err = os.Remove(kv.filename); err != nil {
		return err
	}
	if err = os.Rename(temp, kv.filename); err != nil {
		return err
	}
	_, err = kv.open() // reopen database
	return err
}

// Put save the key-value pair in the current file.
func (kv *Rkv) Put(key string, value interface{}) error {
	kv.isReady()
	if key == "" {
		return ErrBlankKey
	}

	bytes, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return kv.keydir.writeTo(kv.activeFile, key, bytes, 0)
}

// PutExpire save the key-value pair in the current file with expiration in future date.
// Checking for expiration happens on database load, so only when database is
// reopen records become expired.
func (kv *Rkv) PutForDays(key string, value interface{}, days int32) error {
	kv.isReady()
	if key == "" {
		return ErrBlankKey
	}

	bytes, err := json.Marshal(value)
	if err != nil {
		return err
	}

    seconds := time.Now().Unix()
	futureDay := int32(seconds/86400) + days
	return kv.keydir.writeTo(kv.activeFile, key, bytes, futureDay)
}

// Exist returns true if such key exist in the store already.
func (kv *Rkv) Exist(key string) bool {
	kv.isReady()
	//kv.mu.Lock()
	//defer kv.mu.Unlock()

	if kde := kv.keydir.keys[key]; kde == nil {
		return false
	}
	return true
}

// Get retrieves the value for the given key from the keystore.
// May return ErrKeyNotFound error if can not find such key in datastore.
func (kv *Rkv) Get(key string, value interface{}) error {
	kv.isReady()
	kde := kv.keydir.keys[key]
	if kde == nil {
		return ErrKeyNotFound
	} else {
		bytes, err := kde.readValue()
		if err != nil {
			return err
		}
		json.Unmarshal(bytes, &value)
	}
	return nil
}

// GetBytes returns raw bytes from the database.
func (kv *Rkv) GetBytes(key string) ([]byte, error) {
	kv.isReady()
	kde := kv.keydir.keys[key]
	if kde == nil {
		return nil, ErrKeyNotFound
	} 
    return kde.readValue()		
}

// Delete specific key.
func (kv *Rkv) Delete(key string) error {
	kv.isReady()
	bytes := []byte{}
	return kv.keydir.writeTo(kv.activeFile, key, bytes, 0)
}

// DeleteAllKeys that match.
func (kv *Rkv) DeleteAllKeys(with string) error {
	kv.isReady()
	for key, _ := range kv.keydir.keys {
		if with == "" || strings.Contains(key, with) {
			bytes := []byte{}
			if err := kv.keydir.writeTo(kv.activeFile, key, bytes, 0); err != nil {
				return err
			}
		}
	}
	return nil
}

// Iterator returns iterator object (channel), do not use in more than one goroutine.
func (kv *Rkv) Iterator(with string) <-chan string {
	kv.isReady()
	iter := make(chan string, 1)
    go func() {
    	for key, _ := range kv.keydir.keys {
    		if with == "" || strings.Contains(key, with) {
    			iter <- key
    		}
    	}
        close(iter)
    }()
	return iter
}

// GetKeys returns limited number of keys matching criterio, if limit is 
// negative then returns all.
func (kv *Rkv) GetKeys(with string, limit int) []string {
	kv.isReady()
	keys := []string{}
	count := 0
	for key, _ := range kv.keydir.keys {
		if count == limit {
			break
		}
		if with == "" || strings.Contains(key, with) {
			keys = append(keys, key)
			count += 1
		}
	}
	return keys
}

// ------ helpers ------

// open KV store.
func (kv *Rkv) open() (ret *Rkv, err error) {
	var activeFile *os.File
	kv.keydir = newKeydir()
	activeFile, err = os.OpenFile(kv.filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)
	if err != nil {
		return nil, err
	}
	kv.activeFile = NewGFile(activeFile)
	err = kv.populateKeyDir()
	return kv, err
}

// isReady checks if Rkv is open and ready.
func (kv *Rkv) isReady() {
	if kv.keydir == nil {
		panic("rkv: key dir is invalid")
	}
	if kv.activeFile == nil {
		panic("rkv: active file is not defined")
	}
}

// ------ exports / imports ------

// ExportJSON export all data from KV store as mixed JSON.
func (kv *Rkv) ExportJSON(w io.Writer) error {
	kv.isReady()

	count := 0
	io.WriteString(w, "{\n")
	for key, kde := range kv.keydir.keys {
		if count > 0 {
			io.WriteString(w, ",\n")
		}
		val, err := kde.readValue()
		if err != nil {
			return err
		}
		io.WriteString(w, fmt.Sprintf(" \"%s\" : %s", key, val))
		count += 1
	}
	io.WriteString(w, "\n}\n")
	return nil
}

// ExportKeysJSON export keys data from KV store as mixed JSON.
// Will return KeyNotFound error if can not find such key in datastore.
func (kv *Rkv) ExportKeysJSON(w io.Writer, with string) error {
	arr := kv.GetKeys(with, -1)	
	return kv.exportKeys(w, arr)
}

// ExportKeyJSON export single key data from KV store as mixed JSON.
func (kv *Rkv) ExportKeyJSON(w io.Writer, key string) error {
	arr := []string{key}
	return kv.exportKeys(w, arr)
}

// exportKeys internal function.
func (kv *Rkv) exportKeys(w io.Writer, arr []string) error {
	kv.isReady()

	count := 0
	io.WriteString(w, "{\n")
	for _, key := range arr {
		if count > 0 {
			io.WriteString(w, ",\n")
		}
		kde := kv.keydir.keys[key]
		if kde == nil {
			return ErrKeyNotFound
		} else {
			bytes, err := kde.readValue()
			if err != nil {
				return err
			}

			io.WriteString(w, fmt.Sprintf(" \"%s\" : %s", key, bytes))
		}
		count += 1
	}
	io.WriteString(w, "\n}\n")
	return nil
}

// ImportJSON imports files produced with ExportJSON function, may use os.Stdin.
func (kv *Rkv) ImportJSON(r io.Reader) error {
	imp := make(map[string]interface{})
	dat, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(dat, &imp); err != nil {
		return err
	}
	for key, val := range imp {
		kv.Put(key, val)
	}
	return nil
}

// ------ unexported useful funcs ------

// putRaw save the key-value pair in the current file.
func (kv *Rkv) putRaw(key string, value []byte) error {
	return kv.keydir.writeTo(kv.activeFile, key, value, 0)
}

// getRaw retrieves the value for the given if from the keystore.
func (kv *Rkv) getRaw(key string) (value []byte, err error) {
	kde := kv.keydir.keys[key]
	if kde == nil {
		err = ErrKeyNotFound
		value = nil
	} else {
		return kde.readValue()
	}
	return
}

// populateKeyDir read the contents at the given directory and load it into the memory.
func (kv *Rkv) populateKeyDir() error {
	return kv.fill()
}

// fileExists return true if the given path exists and points to a valid file.
// If the file is symlink follows the symlink.
func fileExists(filename string) bool {
	file, err := os.Stat(filename)
	if err != nil {
		return false
	}
	return !file.IsDir()
}

// ConvertToString convert the given byte buffer using the default Go encoding (utf-8).
func ConvertToString(buff []byte) string {
	str, _ := (bufio.NewReader(bytes.NewBuffer(buff))).ReadString(byte(0))
	return str
}

// NewGFile wrap the file f in an convenient structure.
func NewGFile(f *os.File) *GFile {
	return &GFile{f, 0}
}

// newKeydir instantiate an empty key dir.
func newKeydir() *Keydir {
	ret := new(Keydir)
	ret.keys = make(map[string]*KeydirEntry)
	return ret
}

// storeData store the information on the file, update the current pos and return the position
// and size of the value entry.
func (f *GFile) storeData(key string, value []byte, expire int32) (vpos int32, vsz int32, err error) {
	buff := new(bytes.Buffer)
	keydata := []byte(key)
	binary.Write(buff, binary.BigEndian, expire)
	binary.Write(buff, binary.BigEndian, int32(len(keydata)))
	binary.Write(buff, binary.BigEndian, int32(len(value)))
	buff.Write(keydata)
	buff.Write(value)

	crc := crc32.ChecksumIEEE(buff.Bytes())

	vpos = int32(f.cpos + RecordHeaderSize + int32(len(keydata)))
	vsz = int32(len(value))
	buff2 := new(bytes.Buffer)
	binary.Write(buff2, binary.BigEndian, crc)
	buff2.Write(buff.Bytes())
	var sz int
	sz, err = f.file.Write(buff2.Bytes())
	vsz = int32(len(value))
	f.cpos += int32(sz)
	return vpos, vsz, err
}

// readHeader read the header structure from the file and return the header information.
// If data could not be obtained return an errro (including an os.EOF error).
func (f *GFile) readHeader() (crc, tstamp, klen, vlen, vpos int32, key []byte, err error) {
	var hdrbuff []byte = make([]byte, RecordHeaderSize /* crc + tstamp + len key data + len value */)
	var sz int
	sz, err = f.file.Read(hdrbuff)

	if err != nil {
		return
	}

	if int32(sz) != RecordHeaderSize {
		err = errors.New(fmt.Sprintf("Invalid header size. Expected %d got %d bytes", RecordHeaderSize, sz))
	}

	buff := bufio.NewReader(bytes.NewBuffer(hdrbuff))
	binary.Read(buff, binary.BigEndian, &crc)
	binary.Read(buff, binary.BigEndian, &tstamp)
	binary.Read(buff, binary.BigEndian, &klen)
	binary.Read(buff, binary.BigEndian, &vlen)

	key = make([]byte, klen)
	sz, err = f.file.Read(key)

	if err != nil {
		return
	}

	if int32(sz) != klen {
		err = errors.New(fmt.Sprintf("Invalid key size. Expected %d got %d bytes", klen, sz))
		return
	}

	f.file.Seek(int64(vlen), 1) /* move foward in the file to the next header (means skip the value) */
	vpos = f.cpos + RecordHeaderSize + klen
	f.cpos += int32(RecordHeaderSize + klen + vlen)
	return
}

// writeTo save the key/value pair in the given file f and update the keydir structure.
func (kd *Keydir) writeTo(f *GFile, key string, value []byte, expire int32) error {
	kde := new(KeydirEntry)
	var err error

	if f == nil || f.file == nil {
		panic("file is nil")
	}

	kde.vpos, kde.vsz, err = f.storeData(key, value, expire)

	kde.gfile = f
	kde.tstamp = 0
	if len(value) == 0 {
		delete(kd.keys, key)
	} else {
		kd.keys[key] = kde
	}
	return err
}

// fill populate the keydir structure with the information from the given file.
// Scan the entire file looking for information.
func (kv *Rkv) fill() (ret error) {
	kd := kv.keydir
	f := kv.activeFile
	f.file.Seek(0, 0) /* place the cursor in the begin of the file */
	count := 0
	kv.LenKeys = 0
	seconds := time.Now().Unix()
	today := int32(seconds / 86400)

	for {
		_, tstamp, _, vsz, vpos, keydata, err := f.readHeader()

		if err != nil && err != io.EOF {
			ret = err
			break
		} else if err == io.EOF {
			break
		}

		key := string(keydata)
		kde := new(KeydirEntry)
		kde.vpos = vpos
		kde.vsz = vsz
		kde.tstamp = 0
		kde.gfile = f

		if vsz == 0 { // this is deleted value
			delete(kd.keys, key)
		} else if tstamp != 0 && tstamp < today { // this value has expired
			delete(kd.keys, key)
		} else {
			kd.keys[key] = kde
		}
		count += 1

		if err == io.EOF {
			break
		}
	}

	kv.FillRatio = 1
	if count > 0 {
		kv.FillRatio = float64(len(kd.keys)) / float64(count)
	}
	kv.CapKeys = count // total number of keys = alive + dead
	kv.LenKeys = len(kd.keys)
	return ret
}

// readValue reads single value.
func (kde *KeydirEntry) readValue() (value []byte, err error) {
	value = make([]byte, kde.vsz)
	var read int
	read, err = kde.gfile.file.ReadAt(value, int64(kde.vpos))
	if int32(read) != kde.vsz {
		err = errors.New(fmt.Sprintf("Expected %d bytes got %d", kde.vsz, read))
	}
	return
}
