package importer

import (
	"github.com/liemle3893/user-event-importer/event"
	"github.com/liemle3893/user-event-importer/event/storage"
)

type DatabaseImporter struct {
	storage storage.DataStorage
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

func NewDatabaseImporter(s storage.DataStorage) *DatabaseImporter {
	return &DatabaseImporter{storage: s}
}
