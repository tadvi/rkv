package rkv

import (
	"io"
	"sync"
)

// SafeRkv wraps Rkv to provide goroutine safe access to KV store.
type SafeRkv struct {
	Rkv
	mu sync.Mutex // mutex lock, only one goroutine can access KV datastore at one time
}

// NewSafe opens or creates new Rkv.
func NewSafe(filename string) (*SafeRkv, error) {
	kv, err := New(filename)
	return &SafeRkv{Rkv: *kv}, err
}

// Compact same as Rkv function but goroutine friendly.
func (kv *SafeRkv) Compact() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.Rkv.Compact()
}

// Put same as Rkv function but goroutine friendly.
func (kv *SafeRkv) Put(key string, value interface{}) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.Rkv.Put(key, value)
}

// PutForDays same as Rkv function but goroutine friendly.
func (kv *SafeRkv) PutForDays(key string, value interface{}, days int32) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.Rkv.PutForDays(key, value, days)
}

// Exist same as Rkv function but goroutine friendly.
func (kv *SafeRkv) Exist(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.Rkv.Exist(key)
}

// Get same as Rkv function but goroutine friendly.
func (kv *SafeRkv) Get(key string, value interface{}) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.Rkv.Get(key, value)
}

// GetBytes same as Rkv function but goroutine friendly.
func (kv *SafeRkv) GetBytes(key string) ([]byte, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.Rkv.GetBytes(key)
}

// Delete same as Rkv function but goroutine friendly.
func (kv *SafeRkv) Delete(key string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.Rkv.Delete(key)
}

// DeleteAllKeys same as Rkv function but goroutine friendly.
func (kv *SafeRkv) DeleteAllKeys(with string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.Rkv.DeleteAllKeys(with)
}

// GetKeys same as Rkv function but goroutine friendly.
func (kv *SafeRkv) GetKeys(with string, limit int) []string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.Rkv.GetKeys(with, limit)
}

// ExportJSON same as Rkv function but goroutine friendly.
func (kv *SafeRkv) ExportJSON(w io.Writer) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.Rkv.ExportJSON(w)
}

// exportKeys same as Rkv function but goroutine friendly.
func (kv *SafeRkv) exportKeys(w io.Writer, arr []string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.Rkv.exportKeys(w, arr)
}

// Iterator is unsupported in goroutines.
func (kv *SafeRkv) Iterator(with string) <-chan string {
	panic("rkv: unsupported function on SafeRkv")
}
