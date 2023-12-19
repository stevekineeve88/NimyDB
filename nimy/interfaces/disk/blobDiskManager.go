package disk

import (
	"encoding/json"
	"errors"
	"fmt"
	"nimy/interfaces/objects"
	"os"
)

type BlobDiskManager interface {
	Create(db string, blob string, format objects.Format) error
	Delete(db string, blob string) error
	Exists(db string, blob string) bool
}

type blobDisk struct {
	dataLocation string
}

func CreateBlobDiskManager(dataLocation string) BlobDiskManager {
	return blobDisk{
		dataLocation: dataLocation,
	}
}

func (bd blobDisk) Create(db string, blob string, format objects.Format) error {
	directoryName := fmt.Sprintf("%s/%s/%s", bd.dataLocation, db, blob)
	err := os.Mkdir(directoryName, 0600)
	if err != nil {
		return err
	}
	formatError := bd.createFormatFile(directoryName, format)
	pageError := bd.createPagesFile(directoryName)
	if formatError != nil || pageError != nil {
		err = bd.Delete(db, blob)
		if err != nil {
			panic(err.Error())
		}
		return errors.New("failed to create format or page file")
	}
	return nil
}

func (bd blobDisk) Delete(db string, blob string) error {
	return os.RemoveAll(fmt.Sprintf("%s/%s/%s", bd.dataLocation, db, blob))
}

func (bd blobDisk) Exists(db string, blob string) bool {
	_, err := os.Stat(fmt.Sprintf("%s/%s/%s", bd.dataLocation, db, blob))
	return err == nil
}

func (bd blobDisk) createFormatFile(directoryName string, format objects.Format) error {
	formatData, _ := json.MarshalIndent(format.GetMap(), "", " ")
	return bd.createFile(directoryName, formatData, "format.json")
}

func (bd blobDisk) createPagesFile(directoryName string) error {
	pageData, _ := json.MarshalIndent(objects.CreatePages(nil).GetAll(), "", " ")
	return bd.createFile(directoryName, pageData, "pages.json")
}

func (bd blobDisk) createFile(blobDirectory string, fileData []byte, fileName string) error {
	filePath := fmt.Sprintf("%s/%s", blobDirectory, fileName)
	_, err := os.Create(filePath)
	if err != nil {
		return err
	}
	err = os.WriteFile(filePath, fileData, 0600)
	return err
}
