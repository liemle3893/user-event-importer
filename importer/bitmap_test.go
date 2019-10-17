package importer

import (
	"github.com/liemle3893/user-event-importer/bitmap"
	"github.com/liemle3893/user-event-importer/bitmap/roaring"
	"github.com/liemle3893/user-event-importer/event"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func newRoaringBitmap(key string) bitmap.BitMap {
	return roaring.NewBitMap(key, key)
}

func prepareTest() (*BitmapImporter, []event.Event) {
	i := NewBitMapImporter(newRoaringBitmap, false)
	events := []event.Event{
		{
			Timestamp: time.Date(2019, time.September, 10, 10, 10, 0, 0, time.Local),
			Subject:   "423",
			Kind:      "login",
		},
		{
			Timestamp: time.Date(2019, time.September, 10, 11, 10, 0, 0, time.Local),
			Subject:   "423",
			Kind:      "view_stream",
		},
		{
			Timestamp: time.Date(2019, time.September, 10, 13, 10, 0, 0, time.Local),
			Subject:   "423",
			Kind:      "send_gift",
		},
	}
	return i, events
}

func TestNewBitMapImporter(t *testing.T) {
	impt, events := prepareTest()

	// Run test
	count, _ := impt.Import(events...)
	assert.Equal(t, 15, int(count))
	es := impt.Events()
	assert.ElementsMatch(t, []string{"login", "view_stream", "send_gift"}, es)
	bitmaps := impt.Bitmaps()
	assert.Equal(t, 15, len(bitmaps))
}
