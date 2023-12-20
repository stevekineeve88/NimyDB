package disk

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"nimy/interfaces/objects"
	"os"
)

type BlobDiskManager interface {
	Create(db string, blob string, format objects.Format) error
	Delete(db string, blob string) error
	Exists(db string, blob string) bool
	CreatePage(db string, blob string) (objects.PageItem, error)
	GetPages(db string, blob string) ([]objects.PageItem, error)
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
	_, pageFileError := bd.CreatePage(db, blob)
	if formatError != nil || pageError != nil || pageFileError != nil {
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

func (bd blobDisk) CreatePage(db string, blob string) (objects.PageItem, error) {
	blankPageItem := objects.PageItem{}
	pagesItems, err := bd.GetPages(db, blob)
	if err != nil {
		return blankPageItem, nil
	}
	blobDirectory := fmt.Sprintf("%s/%s/%s", bd.dataLocation, db, blob)
	newPageItem := objects.PageItem{FileName: fmt.Sprintf("page-%s.json", uuid.New().String())}
	err = bd.createPage(blobDirectory, newPageItem)
	if err != nil {
		return blankPageItem, err
	}
	pagesItems = append(pagesItems, newPageItem)
	err = bd.writePagesFile(blobDirectory, pagesItems)
	if err != nil {
		err = bd.deletePage(blobDirectory, newPageItem)
		if err != nil {
			panic(err.Error())
		}
		return blankPageItem, errors.New("failed to create format or page file")
	}
	return newPageItem, nil
}

func (bd blobDisk) GetPages(db string, blob string) ([]objects.PageItem, error) {
	var pagesItems []objects.PageItem
	file, err := os.ReadFile(fmt.Sprintf("%s/%s/%s/pages.json", bd.dataLocation, db, blob))
	if err != nil {
		return nil, err
	}
	unmarshalError := json.Unmarshal(file, &pagesItems)
	return pagesItems, unmarshalError
}

func (bd blobDisk) createPage(directoryName string, pageItem objects.PageItem) error {
	pageData, _ := json.MarshalIndent(make(map[string]interface{}), "", " ")
	return bd.createFile(directoryName, pageData, pageItem.FileName)
}

func (bd blobDisk) deletePage(directoryName string, pageItem objects.PageItem) error {
	return os.Remove(fmt.Sprintf("%s/%s", directoryName, pageItem.FileName))
}

func (bd blobDisk) writePagesFile(directoryName string, pageItems []objects.PageItem) error {
	pagesData, _ := json.MarshalIndent(pageItems, "", " ")
	return bd.writeFile(directoryName, pagesData, "pages.json")
}

func (bd blobDisk) createFormatFile(directoryName string, format objects.Format) error {
	formatData, _ := json.MarshalIndent(format.GetMap(), "", " ")
	return bd.createFile(directoryName, formatData, "format.json")
}

func (bd blobDisk) createPagesFile(directoryName string) error {
	pageData, _ := json.MarshalIndent(make([]objects.PageItem, 0), "", " ")
	return bd.createFile(directoryName, pageData, "pages.json")
}

func (bd blobDisk) createFile(directory string, fileData []byte, fileName string) error {
	filePath := fmt.Sprintf("%s/%s", directory, fileName)
	_, err := os.Create(filePath)
	if err != nil {
		return err
	}
	return bd.writeFile(directory, fileData, fileName)
}

func (bd blobDisk) writeFile(directory string, fileData []byte, fileName string) error {
	filePath := fmt.Sprintf("%s/%s", directory, fileName)
	return os.WriteFile(filePath, fileData, 0600)
}
