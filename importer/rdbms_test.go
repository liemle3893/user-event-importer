package importer

import (
	"github.com/jinzhu/gorm"
	"github.com/liemle3893/user-event-importer/event"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)
import _ "github.com/jinzhu/gorm/dialects/sqlite"

func TestDatabaseImporter_Import(t *testing.T) {
	gormDb, err := gorm.Open("sqlite3", ":memory:")
	if err != nil {
		panic("Fail to open db connection. ")
	}
	gormDb.LogMode(true)
	dataStorage := event.NewDatabase(gormDb)
	gormDb.AutoMigrate(&WrappedEvent{})
	importer := NewDatabaseImporter(dataStorage)
	e1 := &WrappedEvent{
		Event: event.Event{
			Timestamp: time.Date(2019, time.September, 10, 10, 10, 0, 0, time.Local),
			Subject:   "423",
			Kind:      "login",
		},
		Data: 5,
	}
	e2 := &WrappedEvent{
		Event: event.Event{
			Timestamp: time.Date(2019, time.September, 10, 11, 10, 0, 0, time.Local),
			Subject:   "423",
			Kind:      "login",
		},
		Data: 2,
	}
	e3 := &WrappedEvent{
		Event: event.Event{
			Timestamp: time.Date(2019, time.September, 10, 13, 10, 0, 0, time.Local),
			Subject:   "423",
			Kind:      "login",
		},
		Data: 3,
	}
	events := []event.IEvent{e1, e2, e3}
	count, err := importer.Import(events...)
	if err != nil {
		t.Fail()
	}
	assert.Equal(t, int32(3), count)
}

type WrappedEvent struct {
	event.Event
	Data int
}

func (WrappedEvent) TableName() string {
	return "rollup_event"
}
