package importer

import (
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/liemle3893/user-event-importer/bitmap"
	"github.com/liemle3893/user-event-importer/event"
)

type CreateBitMapFunc func(string) bitmap.BitMap

type BitmapImporter struct {
	fn             CreateBitMapFunc
	TrackingHourly bool
	// Map of event_key -> bitmap
	eventBitmapDS map[string]bitmap.BitMap
	events        map[string]struct{}
}

func NewBitMapImporter(fn CreateBitMapFunc, hourly bool) *BitmapImporter {
	return &BitmapImporter{
		fn:             fn,
		TrackingHourly: hourly,
		eventBitmapDS:  make(map[string]bitmap.BitMap),
		events:         make(map[string]struct{}),
	}
}

func (importer *BitmapImporter) Import(events ...event.Event) (int32, error) {
	// One event would have at least 5 subkey (one more if you enable hourly tracking)
	// Eg: user 423 login at 2019-09-01 15:12:11 would yield
	// 1. login
	// 2. login:YEARLY:2019
	// 3. login:MONTHLY:2019-09
	// 4. login:WEEKLY:2019-40
	// 5. login:DAILY:2019-09-01
	eventIdGenerator := func(e event.Event) []string {
		var ids []string
		ids = append(ids, e.Kind)
		ids = append(ids, fmt.Sprintf("%s:YEARLY:%d", e.Kind, e.Timestamp.Year()))
		ids = append(ids, fmt.Sprintf("%s:MONTHLY:%d-%02d", e.Kind, e.Timestamp.Year(), e.Timestamp.Month()))
		ids = append(ids, fmt.Sprintf("%s:DAILY:%d-%02d-%02d", e.Kind, e.Timestamp.Year(), e.Timestamp.Month(), e.Timestamp.Day()))
		year, week := e.Timestamp.ISOWeek()
		ids = append(ids, fmt.Sprintf("%s:WEEKLY:%d-%02d", e.Kind, year, week))
		if importer.TrackingHourly {
			ids = append(ids, fmt.Sprintf("%s:HOURLY:%d-%02d-%02d_%02d", e.Kind, e.Timestamp.Year(), e.Timestamp.Month(), e.Timestamp.Day(), e.Timestamp.Hour()))
		}
		return ids
	}
	var counter int32 = 0
	var errors *multierror.Error
	for _, _event := range events {
		importer.events[_event.Kind] = struct{}{}
		eventKeys := eventIdGenerator(_event)
		for _, key := range eventKeys {
			bm := importer.getBitmap(key)
			err := bm.Mark(_event.Subject)
			if err != nil {
				errors = multierror.Append(errors, err)
			} else {
				counter++
			}
		}
	}
	return counter, errors.ErrorOrNil()
}

// Bitmaps will return all bit map contains within importer
func (importer *BitmapImporter) Bitmaps() []bitmap.BitMap {
	var rs []bitmap.BitMap
	for _, bm := range importer.eventBitmapDS {
		rs = append(rs, bm)
	}
	return rs
}

func (importer *BitmapImporter) getBitmap(key string) bitmap.BitMap {
	if bm, ok := importer.eventBitmapDS[key]; ok {
		return bm
	} else {
		_bitmap := importer.fn(key)
		importer.eventBitmapDS[key] = _bitmap
		return _bitmap
	}
}

func (importer *BitmapImporter) Events() []string {
	var events []string
	for key, _ := range importer.events {
		events = append(events, key)
	}
	return events
}
