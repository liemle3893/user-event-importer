package database

import (
	"github.com/liemle3893/user-event-importer/bitmap"
	"io"
)

type DeserializeFunc func([]byte) (bitmap.BitMap, error)
type SerializeFunc func() error

type Database interface {
	Persist(writer io.Writer) error                            // Write data
	Get(key string, fn DeserializeFunc) (bitmap.BitMap, error) // Return a fresh bit map. Must be PUTed if want to persist into db
	Put(bitMap bitmap.BitMap) error                            // Save bitmap into database
	BulkSave(bitmaps ...bitmap.BitMap) error
	Keys() []string // Return all keys
	Close()
}
