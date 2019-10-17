package importer

import (
	"fmt"
	"testing"
	"time"

	"github.com/liemle3893/user-event-importer/event"
)

var (
	dataSource = make(map[string]interface{})
)

func TestImport(t *testing.T) {

	memoryImporter := NewImporter(func(events ...event.IEvent) (int32, error) {
		var count int32
		for _, event := range events {
			key := fmt.Sprintf("%v_%v_%v", event.GetSubject(), event.GetKind(), event.GetTimestamp())
			dataSource[key] = event.GetSubject()
			count++
		}
		return count, nil
	})
	mainImporter := NewCompositeImporter(memoryImporter)
	t.Run("Should import into memory success", func(t *testing.T) {
		e1 := &event.Event{
			Timestamp: time.Now(),
			Subject:   "1",
			Kind:      "click",
		}
		e2 := &event.Event{
			Timestamp: time.Now(),
			Subject:   "2",
			Kind:      "click",
		}
		e3 := &event.Event{
			Timestamp: time.Now(),
			Subject:   "2",
			Kind:      "play:song",
		}
		var events []event.IEvent
		events = append(events, e1)
		events = append(events, e2)
		events = append(events, e3)
		count, err := mainImporter.Import(events...)
		if count != 3 {
			t.Error("Invalid count")
		}
		if err != nil {
			t.Errorf("Fail to import data. %+v", err)
		}
	})

}
