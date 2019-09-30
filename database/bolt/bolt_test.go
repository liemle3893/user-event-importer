package bolt

import (
	"github.com/liemle3893/user-event-importer/bitmap"
	"github.com/liemle3893/user-event-importer/bitmap/roaring"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestLoadingDataFromFile(t *testing.T) {
	// Given
	dbFile := "/tmp/loading.db"
	db, _ := NewBitmapDatabase(dbFile)
	oriKey := "events:click:2019_09_01"
	bm := roaring.NewBitMap(oriKey, "My test")
	bm.Mark(uint64(1))
	bm.Mark(uint64(2))
	bm.Mark(uint64(423))
	// Save bitmap into database
	db.Put(bm)
	db.Close()
	// When
	db1, _ := NewBitmapDatabase(dbFile)
	defer db1.Close()

	fn := func(bytes []byte) (bitmap.BitMap, error) {
		return roaring.FromBytes(bytes, oriKey, "")
	}

	bm1, _ := db1.Get(oriKey, fn)
	// Then
	exist, _ := bm1.Exists(uint64(1))
	assert.Equal(t, true, exist)
	// Clean it up
	os.Remove(dbFile)
}
