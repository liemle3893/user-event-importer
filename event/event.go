package event

import (
	"time"
)

type Data interface{}

// Event is an action cause by a user
type Event struct {
	// Time that event was occurred
	Timestamp time.Time `gorm:"primary_key"`
	// Kind of event. Like Click, Play song, etc...
	// Should only contains accii character.
	Kind string `gorm:"primary_key;type:varchar(100)"`
	// The owner of this event. May be user/consumer
	Subject string `gorm:"primary_key;type:varchar(100)"`
}

func (e *Event) GetTimestamp() time.Time {
	return e.Timestamp
}

func (e *Event) GetSubject() string {
	return e.Subject
}

func (e *Event) GetKind() string {
	return e.Kind
}

type IEvent interface {
	GetTimestamp() time.Time
	GetSubject() string
	GetKind() string
}
