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

	memoryImporter := NewImporter(func(events ...event.Event) (int32, error) {
		var count int32
		for _, event := range events {
			key := fmt.Sprintf("%v_%v_%v", event.UserID, event.Kind, event.Timestamp)
			dataSource[key] = event.Data
			count++
		}
		return count, nil
	})
	mainImporter := NewCompositeImporter(memoryImporter)
	t.Run("Should import into memory success", func(t *testing.T) {
		count, err := mainImporter.Import([]event.Event{
			{
				Timestamp: time.Now(),
				UserID:    "1",
				Kind:      "click",
				Data:      nil,
			},
			{
				Timestamp: time.Now(),
				UserID:    "2",
				Kind:      "click",
				Data:      nil,
			},
			{
				Timestamp: time.Now(),
				UserID:    "1",
				Kind:      "play:song",
				Data:      nil,
			},
		}...)
		if count != 3 {
			t.Error("Invalid count")
		}
		if err != nil {
			t.Errorf("Fail to import data. %+v", err)
		}
	})

}
