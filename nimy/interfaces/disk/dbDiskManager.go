package disk

import (
	"fmt"
	"io"
	"os"
)

type DBDiskManager interface {
	Create(db string) error
	Delete(db string) error
	Exists(db string) bool
}

type dbDisk struct {
	dataLocation string
}

func CreateDBDiskManager(dataLocation string) DBDiskManager {
	return dbDisk{
		dataLocation: dataLocation,
	}
}

func (dbd dbDisk) Create(db string) error {
	return os.Mkdir(fmt.Sprintf("%s/%s", dbd.dataLocation, db), 0600)
}

func (dbd dbDisk) Delete(db string) error {
	return os.Remove(fmt.Sprintf("%s/%s", dbd.dataLocation, db))
}

func (dbd dbDisk) Exists(db string) bool {
	_, err := os.Stat(fmt.Sprintf("%s/%s", dbd.dataLocation, db))
	return err == nil
}

func (dbd dbDisk) isEmpty(dir *os.File) (bool, error) {
	_, err := dir.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}
