package store

import (
	"errors"
	"fmt"
	"nimy/interfaces/disk"
	"nimy/interfaces/objects"
)

type DBStore interface {
	CreateDB(db string) (objects.DB, error)
	DeleteDB(db string) error
}

type dbStore struct {
	dbDiskManager disk.DBDiskManager
}

func CreateDBStore(dbDiskManager disk.DBDiskManager) DBStore {
	return dbStore{
		dbDiskManager: dbDiskManager,
	}
}

func (d dbStore) CreateDB(db string) (objects.DB, error) {
	dbObj := objects.CreateDB(db)
	if err := dbObj.HasDBNameConvention(); err != nil {
		return dbObj, err
	}
	return dbObj, d.dbDiskManager.Create(db)
}

func (d dbStore) DeleteDB(db string) error {
	if found := d.dbDiskManager.Exists(db); !found {
		return errors.New(fmt.Sprintf("%s not found", db))
	}
	return d.dbDiskManager.Delete(db)
}
