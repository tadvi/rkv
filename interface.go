package rkv

import (
	"io"
)

// Interface only contains functions applicable to both Rkv and SafeRkv.
// Rkv has few more functions that are not goroutine friendly and not part of
// this common interface.
type Interface interface {
	Reopen() error
	Close()

	Compact() error

	GetKeys(with string, limit int) []string
	Get(key string, value interface{}) error
	GetBytes(key string) ([]byte, error)

	Put(key string, value interface{}) error
	PutForDays(key string, value interface{}, days int32) error

	Exist(key string) bool

	Delete(key string) error
	DeleteAllKeys(with string) error

	ExportJSON(w io.Writer) error
	ImportJSON(r io.Reader) error

	// Iterator(with string) chan<- string
}
