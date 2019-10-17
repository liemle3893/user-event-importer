package bolt

import (
	"errors"
	"github.com/boltdb/bolt"
	"github.com/golang/snappy"
	"github.com/liemle3893/user-event-importer/bitmap"
	database2 "github.com/liemle3893/user-event-importer/bitmap/storage"
	"io"
	"log"
	"time"
)

var (
	bucketName          = []byte("bitmap")
	BitMapNotFoundError = errors.New("bitmap not found")
)

type dbItem struct {
	b     bitmap.BitMap
	aTime int64 // last access time
	dirty bool  // true if has unsaved modifications
}

type database struct {
	db  *bolt.DB
	log log.Logger
	//
	keys  map[string]struct{} // all known keys
	items map[string]dbItem   // hot items
}

func (d *database) BulkSave(bitmaps ...bitmap.BitMap) error {
	all := make(map[string][]byte)
	for _, bm := range bitmaps {
		key := bm.Key()
		bytes, err := bm.ToBytes()
		if err != nil {
			return err
		}
		all[key] = bytes
	}

	return d.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		for key, bytes := range all {
			encodedBytes := snappy.Encode(nil, bytes)
			err := bucket.Put([]byte(key), encodedBytes)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *database) Close() {
	d.db.Close()
}

func NewBitmapDatabase(dbFile string) (database2.BitmapStorage, error) {
	db, err := bolt.Open(dbFile, 0644, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	d := database{
		db:    db,
		log:   log.Logger{},
		keys:  make(map[string]struct{}),
		items: make(map[string]dbItem),
	}
	e := d.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return nil
		}
		fn := func(k, v []byte) error { d.keys[string(k)] = struct{}{}; return nil }
		return bucket.ForEach(fn)
	})
	if e != nil {
		d.db.Close()
		return nil, e
	}
	return &d, nil
}

func (d *database) Persist(writer io.Writer) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(writer)
		if err != nil {
			return err
		}
		return nil
	})
}

func (d *database) Get(key string, fn database2.DeserializeFunc) (bitmap.BitMap, error) {
	// Check if key exists. If not. Return error
	if _, ok := d.keys[key]; !ok {
		return nil, BitMapNotFoundError
	}
	defer func() {
		// Update access time
		item := d.items[key]
		item.aTime = time.Now().Unix()
	}()
	// Get data from hot items first
	if item, ok := d.items[key]; ok {
		// Data is exists within hot items
		return item.b, nil
	}
	// Fetch data from db
	ch := make(chan bitmap.BitMap, 1)
	err := d.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return nil
		}
		bytes := bucket.Get([]byte(key))
		decodedBytes, err := snappy.Decode(nil, bytes)
		if err != nil {
			return err
		}
		bm, err := fn(decodedBytes)
		if err != nil {
			return err
		} else {
			ch <- bm
			return nil
		}
	})
	if err == nil {
		b := <-ch
		close(ch)
		// Put into hot items
		d.items[key] = dbItem{b: b, dirty: false}
		return b, err
	}
	close(ch)
	return nil, err
}

func (d *database) Put(bm bitmap.BitMap) error {
	key := bm.Key()
	bytes, err := bm.ToBytes()
	if err != nil {
		return err
	}
	return d.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		encodedBytes := snappy.Encode(nil, bytes)
		err := bucket.Put([]byte(key), encodedBytes)
		return err
	})
}

func (d *database) Keys() []string {
	var keys []string
	for key := range d.keys {
		keys = append(keys, key)
	}
	return keys
}
