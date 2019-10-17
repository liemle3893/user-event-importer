package importer

import (
	"github.com/liemle3893/user-event-importer/event"
)

type DatabaseImporter struct {
	storage event.DataStorage
}

func (importer *DatabaseImporter) Import(events ...event.IEvent) (int32, error) {
	var es []interface{}
	for _, e := range events {
		es = append(es, interface{}(e))
	}
	err := importer.storage.Persist(es)
	if err != nil {
		return 0, err
	}
	return int32(len(events)), nil
}

func NewDatabaseImporter(s event.DataStorage) *DatabaseImporter {
	return &DatabaseImporter{storage: s}
}
