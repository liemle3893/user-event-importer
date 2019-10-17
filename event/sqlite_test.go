package event

import (
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
	"time"
)

type rollUpEvent struct {
	gorm.Model
	*Event
	Data int `gorm:"column:data"`
}

func TestSQLiteStorage(t *testing.T) {
	gormDb, err := gorm.Open("sqlite3", ":memory:")
	if err != nil {
		panic("failed to connect database")
	}
	defer gormDb.Close()
	gormDb.LogMode(true)
	db := NewDatabase(gormDb)
	e1 := &rollUpEvent{
		Event: &Event{Timestamp: time.Now(),
			Subject: "user:423",
			Kind:    "test",},
		Data: 1,
	}
	e2 := &rollUpEvent{
		Event: &Event{Timestamp: time.Now(),
			Subject: "user:124",
			Kind:    "test",},
		Data: 2,
	}
	log.Printf("e1: %+v\n", e1)
	log.Printf("e2: %+v\n", e2)

	var events []interface{}
	events = append(events, e1)
	events = append(events, e2)

	// Test bulk insert
	t.Run("Insert events should work correctly", func(t *testing.T) {
		gormDb.AutoMigrate(&rollUpEvent{})
		db.Persist(events)
		var event124, event423 rollUpEvent
		gormDb.First(&event124, "subject = ?", "user:124")
		gormDb.First(&event423, "subject = ?", "user:423")
		log.Printf("event124: %+v\n", event124)
		log.Printf("event423: %+v\n", event423)
		assert.True(t, event124.ID > 0)
		assert.True(t, event423.ID > 0)
	})
}
