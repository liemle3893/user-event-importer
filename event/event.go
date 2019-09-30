package event

import "time"

type EventData interface{}

// Event is an action cause by a user
type Event struct {
	Timestamp time.Time
	UserID    string
	// Kind of event. Like Click, Play song, etc...
	// Should only contains accii character.
	Kind string
	Data EventData
}
