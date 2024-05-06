package disk

import (
	"fmt"
	"nimy/interfaces/util"
	"os"
)

type DBDiskManager interface {
	Create(db string) error
	Delete(db string) error
	Exists(db string) bool
}

type dbDisk struct {
	dataLocation string
	logger       util.Logger
}

func CreateDBDiskManager(dataLocation string) DBDiskManager {
	return dbDisk{
		dataLocation: dataLocation,
		logger:       util.GetLogger(),
	}
}

func (dbd dbDisk) Create(db string) error {
	dbd.logger.Log("creating database", util.Info, "db", db)
	return os.Mkdir(fmt.Sprintf("%s/%s", dbd.dataLocation, db), 0600)
}

func (dbd dbDisk) Delete(db string) error {
	dbd.logger.Log("deleting database", util.Info, "db", db)
	return os.Remove(fmt.Sprintf("%s/%s", dbd.dataLocation, db))
}

func (dbd dbDisk) Exists(db string) bool {
	_, err := os.Stat(fmt.Sprintf("%s/%s", dbd.dataLocation, db))
	return err == nil
}
