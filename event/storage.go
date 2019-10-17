package event

import "github.com/jinzhu/gorm"

type DataStorage interface {
	Persist(events ...interface{}) error
	Close() error
}

type simpleDatabase struct {
	gormDb *gorm.DB
}

func NewDatabase(db *gorm.DB) DataStorage {
	return &simpleDatabase{gormDb: db}
}

func (db *simpleDatabase) Close() error {
	return db.gormDb.Close()
}

func (db *simpleDatabase) Persist(events ...interface{}) error {
	tx := db.gormDb.Begin()

	var err error
DbError:
	for _, data := range events {
		_events, ok := data.([]interface{})
		if ok {
			for _, event := range _events {
				err = tx.Save(event).Error
				if err != nil {
					break
				}
			}
		} else {
			err = tx.Save(data).Error
		}
		if err != nil {
			// Error occur, roll back
			tx.Rollback()
			break DbError
		}
	}
	if err == nil {
		tx.Commit()
	} else {
		tx.Rollback()
	}
	return err
}
