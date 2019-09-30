package roaring

import (
	"github.com/liemle3893/user-event-importer/bitmap"
	"github.com/stretchr/testify/assert"
	"testing"
)

func prepareTest() (bitmap.BitMap) {
	oriKey := "events:click:2019_09_01"
	bm := NewBitMap(oriKey, "Test key")
	bm.Mark("1")
	bm.Mark("2")
	bm.Mark("423")
	return bm
}

func TestRoaringBitMap(t *testing.T) {
	originalBitmap := prepareTest()
	t.Run("Doing NOT on empty bitmap should do nothing", func(t *testing.T) {
		k := "test"
		b := NewBitMap(k, "Some description")
		notb, _ := b.Not()
		cardinal, _ := notb.Count()
		assert.Equal(t, uint64(0), cardinal)
	})

	t.Run("Key should exists", func(t *testing.T) {
		b, _ := originalBitmap.Exists("1")
		assert.True(t, b)
	})

	t.Run("Key should not exists after NOT op", func(t *testing.T) {
		b, _ := originalBitmap.Not()
		exists, _ := b.Exists("1")
		assert.False(t, exists)
	})

	t.Run("New Bitmap should have difference name with the old one", func(t *testing.T) {
		not, _ := originalBitmap.Not()
		assert.NotEqual(t, originalBitmap.Key(), not.Key())
	})

	t.Run("AND op should be correct", func(t *testing.T) {
		bm := NewBitMap("Empty", "EmptyBitMap")
		empty, _ := originalBitmap.And(bm)
		count, _ := empty.Count()
		assert.Equal(t, uint64(0), count)
		bm.Mark("1")
		bmCount, _ := bm.Count()
		assert.Equal(t, uint64(1), bmCount)
		onlyAt1, _ := originalBitmap.And(bm)
		count1, _ := onlyAt1.Count()
		assert.Equal(t, uint64(1), count1)
		existsAt1, _ := onlyAt1.Exists("1")
		assert.True(t, existsAt1)
	})

	t.Run("XOR op should be correct", func(t *testing.T) {
		bm := NewBitMap("Empty", "EmptyBitMap")
		bm.MarkInt(1)
		xor, _ := bm.Xor(originalBitmap)
		xorCount, _ := xor.Count()
		exists, _ := xor.Exists("1")
		assert.False(t, exists)
		assert.Equal(t, uint64(2), xorCount)
	})

	t.Run("OR op should be correct", func(t *testing.T) {
		bm := NewBitMap("Test", "EmptyBitMap")
		bm.MarkInt(3)
		or, _ := bm.Or(originalBitmap)
		orCount, _ := or.Count()
		exists, _ := or.Exists("3")
		assert.True(t, exists)
		assert.Equal(t, uint64(4), orCount)
	})

	t.Run("List elements", func(t *testing.T) {
		var elements []string
		originalBitmap.Elements(func(it string) {
			elements = append(elements, it)
		})
		assert.Equal(t, []string{"1", "2", "423"}, elements)
	})
}
