package disk

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"nimy/constants"
	"nimy/interfaces/objects"
	"nimy/interfaces/util"
	"os"
)

type BlobDiskManager interface {
	CreateBlob(db string, blob string, format objects.Format) error
	DeleteBlob(db string, blob string) error
	BlobExists(db string, blob string) bool
	CreatePage(db string, blob string) (objects.PageItem, error)
	CreateIndexPage(db string, blob string, prefix string) (objects.PrefixIndexItem, error)
	CreateFormatFile(db string, blob string, format objects.Format) error
	CreatePagesFileAndDir(db string, blob string) error
	CreateIndexesFileAndDir(db string, blob string) error
	GetPageItems(db string, blob string) ([]objects.PageItem, error)
	GetPrefixIndexItems(db string, blob string) (map[string]objects.PrefixIndexItem, error)
	GetPageData(db string, blob string, fileName string) (map[string]map[string]any, error)
	GetIndexData(db string, blob string, fileName string) (map[string]string, error)
	GetFormat(db string, blob string) (objects.Format, error)
	WritePageData(db string, blob string, fileName string, records map[string]map[string]any) error
	WriteIndexData(db string, blob string, fileName string, records map[string]string) error
	WriteIndexPagesFile(directoryName string, indexItems map[string]objects.PrefixIndexItem) error
	DeletePageItem(db string, blob string, dPage objects.PageItem) error
	DeleteIndexFile(db string, blob string, fileName string) error
	WritePagesFile(directoryName string, pageItems []objects.PageItem) error
	CreateFile(directory string, fileData []byte, fileName string) error
	WriteFile(directory string, fileData []byte, fileName string) error
}

type blobDiskManager struct {
	dataLocation string
	logger       util.Logger
}

func CreateBlobDiskManager(dataLocation string) BlobDiskManager {
	return blobDiskManager{
		dataLocation: dataLocation,
		logger:       util.GetLogger(),
	}
}

func (bdm blobDiskManager) CreateBlob(db string, blob string, format objects.Format) error {
	err := os.Mkdir(fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob), 0777)
	if err != nil {
		return err
	}

	fileCreations := []func(db string, blob string) error{
		func(db string, blob string) error {
			return bdm.CreateFormatFile(db, blob, format)
		},
		bdm.CreatePagesFileAndDir,
		bdm.CreateIndexesFileAndDir,
	}

	for _, fileCreation := range fileCreations {
		err = fileCreation(db, blob)
		if err != nil {
			bdm.logger.Log("failed to create blob. Rolling back...", util.Warn, "db", db, "blob", blob, "error", err.Error())
			deleteBlobError := bdm.DeleteBlob(db, blob)
			if deleteBlobError != nil {
				bdm.logger.Log("roll back operation failed. Corrupt blob", util.Error, "db", db, "blob", blob, "error", deleteBlobError.Error())
				panic(deleteBlobError.Error())
			}
			return err
		}
	}

	return nil
}

func (bdm blobDiskManager) DeleteBlob(db string, blob string) error {
	bdm.logger.Log("deleting blob", util.Info, "db", db, "blob", blob)
	return os.RemoveAll(fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob))
}

func (bdm blobDiskManager) BlobExists(db string, blob string) bool {
	_, err := os.Stat(fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob))
	return err == nil
}

func (bdm blobDiskManager) CreatePage(db string, blob string) (objects.PageItem, error) {
	pageItem := objects.PageItem{}
	pagesItems, err := bdm.GetPageItems(db, blob)
	if err != nil {
		return pageItem, err
	}
	blobDirectory := fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob)
	pageItem.FileName = fmt.Sprintf("%s/%s.json", constants.PagesDir, uuid.New().String())
	pageData, _ := json.Marshal(make(map[string]interface{}))

	err = bdm.CreateFile(blobDirectory, pageData, pageItem.FileName)
	if err != nil {
		return pageItem, err
	}
	pagesItems = append(pagesItems, pageItem)
	err = bdm.WritePagesFile(blobDirectory, pagesItems)
	if err != nil {
		bdm.logger.Log("failed to create page. Rolling back...", util.Warn, "db", db, "blob", blob, "page", pageItem.FileName, "error", err.Error())
		deletePageError := os.Remove(fmt.Sprintf("%s/%s", blobDirectory, pageItem.FileName))
		if deletePageError != nil {
			bdm.logger.Log("roll back operation failed. Corrupt page", util.Error, "db", db, "blob", blob, "page", pageItem.FileName, "error", deletePageError.Error())
			panic(deletePageError.Error())
		}
		return pageItem, err
	}
	return pageItem, nil
}

func (bdm blobDiskManager) CreateIndexPage(db string, blob string, prefix string) (objects.PrefixIndexItem, error) {
	prefixIndexItem := objects.PrefixIndexItem{FileNames: []string{}}
	prefixIndexItemMap, err := bdm.GetPrefixIndexItems(db, blob)
	if err != nil {
		return prefixIndexItem, err
	}
	blobDirectory := fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob)
	tempPrefixIndexItem, ok := prefixIndexItemMap[prefix]
	if ok {
		prefixIndexItem = tempPrefixIndexItem
	}
	indexFileName := fmt.Sprintf("%s/%s.json", constants.IndexesDir, uuid.New().String())
	indexMap, _ := json.Marshal(make(map[string]string))

	err = bdm.CreateFile(blobDirectory, indexMap, indexFileName)
	if err != nil {
		return prefixIndexItem, err
	}
	prefixIndexItem.FileNames = append(prefixIndexItem.FileNames, indexFileName)
	prefixIndexItemMap[prefix] = prefixIndexItem
	err = bdm.WriteIndexPagesFile(blobDirectory, prefixIndexItemMap)
	if err != nil {
		bdm.logger.Log("failed to create index page. Rolling back...", util.Warn, "db", db, "blob", blob, "page", indexFileName, "error", err.Error())
		deleteIndexPageError := os.Remove(fmt.Sprintf("%s/%s", blobDirectory, indexFileName))
		if deleteIndexPageError != nil {
			bdm.logger.Log("roll back operation failed. Corrupt index page", util.Error, "db", db, "blob", blob, "page", indexFileName, "error", deleteIndexPageError.Error())
			panic(deleteIndexPageError.Error())
		}
		return prefixIndexItem, err
	}
	return prefixIndexItem, nil
}

func (bdm blobDiskManager) CreateFormatFile(db string, blob string, format objects.Format) error {
	bdm.logger.Log("creating format file", util.Info, "db", db, "blob", blob)
	formatData, _ := json.Marshal(format.GetMap())
	return bdm.CreateFile(fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob), formatData, constants.FormatFile)
}

func (bdm blobDiskManager) CreatePagesFileAndDir(db string, blob string) error {
	bdm.logger.Log("creating pages file and directory", util.Info, "db", db, "blob", blob)
	pageData, _ := json.Marshal(make([]objects.PageItem, 0))
	err := bdm.CreateFile(fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob), pageData, constants.PagesFile)
	if err != nil {
		return err
	}
	return os.Mkdir(fmt.Sprintf("%s/%s/%s/%s", bdm.dataLocation, db, blob, constants.PagesDir), 0600)
}

func (bdm blobDiskManager) CreateIndexesFileAndDir(db string, blob string) error {
	bdm.logger.Log("creating index pages file and directory", util.Info, "db", db, "blob", blob)
	indexData, _ := json.Marshal(make(map[string]objects.PrefixIndexItem))
	err := bdm.CreateFile(fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob), indexData, constants.IndexesFile)
	if err != nil {
		return err
	}
	return os.Mkdir(fmt.Sprintf("%s/%s/%s/%s", bdm.dataLocation, db, blob, constants.IndexesDir), 0600)
}

func (bdm blobDiskManager) GetFormat(db string, blob string) (objects.Format, error) {
	var formatItems map[string]objects.FormatItem
	file, err := os.ReadFile(fmt.Sprintf("%s/%s/%s/%s", bdm.dataLocation, db, blob, constants.FormatFile))
	if err != nil {
		return objects.Format{}, err
	}
	err = json.Unmarshal(file, &formatItems)
	if err != nil {
		return objects.Format{}, err
	}
	return objects.CreateFormat(formatItems), nil
}

func (bdm blobDiskManager) GetPageItems(db string, blob string) ([]objects.PageItem, error) {
	var pagesItems []objects.PageItem
	file, err := os.ReadFile(fmt.Sprintf("%s/%s/%s/%s", bdm.dataLocation, db, blob, constants.PagesFile))
	if err != nil {
		return nil, err
	}

	unmarshalError := json.Unmarshal(file, &pagesItems)
	return pagesItems, unmarshalError
}

func (bdm blobDiskManager) GetPrefixIndexItems(db string, blob string) (map[string]objects.PrefixIndexItem, error) {
	var prefixIndexItemMap map[string]objects.PrefixIndexItem
	file, err := os.ReadFile(fmt.Sprintf("%s/%s/%s/%s", bdm.dataLocation, db, blob, constants.IndexesFile))
	if err != nil {
		return nil, err
	}
	unmarshalError := json.Unmarshal(file, &prefixIndexItemMap)
	return prefixIndexItemMap, unmarshalError
}

func (bdm blobDiskManager) GetPageData(db string, blob string, fileName string) (map[string]map[string]any, error) {
	var pageData map[string]map[string]any
	file, err := os.ReadFile(fmt.Sprintf("%s/%s/%s/%s", bdm.dataLocation, db, blob, fileName))
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(file, &pageData)
	return pageData, err
}

func (bdm blobDiskManager) GetIndexData(db string, blob string, fileName string) (map[string]string, error) {
	var indexData map[string]string
	file, err := os.ReadFile(fmt.Sprintf("%s/%s/%s/%s", bdm.dataLocation, db, blob, fileName))
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(file, &indexData)
	return indexData, err
}

func (bdm blobDiskManager) WritePageData(db string, blob string, fileName string, records map[string]map[string]any) error {
	var buffer bytes.Buffer
	buffer.WriteString("{\n")
	directoryName := fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob)
	size := len(records)
	i := 1
	for key, record := range records {
		buffer.WriteString(fmt.Sprintf("\"%s\": ", key))
		body, _ := json.Marshal(record)
		buffer.Write(body)
		if i < size {
			buffer.WriteString(",\n")
			i++
		}
	}
	buffer.WriteString("\n}")
	return bdm.WriteFile(directoryName, buffer.Bytes(), fileName)
}

func (bdm blobDiskManager) WriteIndexData(db string, blob string, fileName string, records map[string]string) error {
	directoryName := fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob)
	recordData, _ := json.Marshal(records)
	return bdm.WriteFile(directoryName, recordData, fileName)
}

func (bdm blobDiskManager) DeletePageItem(db string, blob string, dPageItem objects.PageItem) error {
	pageItems, err := bdm.GetPageItems(db, blob)
	if err != nil {
		return err
	}
	for index, pageItem := range pageItems {
		if pageItem.FileName == dPageItem.FileName {
			copy(pageItems[index:], pageItems[index+1:])
			pageItems[len(pageItems)-1] = objects.PageItem{}
			pageItems = pageItems[:len(pageItems)-1]

			directoryName := fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob)
			err = bdm.WritePagesFile(directoryName, pageItems)
			if err != nil {
				return err
			}
			err = os.Remove(fmt.Sprintf("%s/%s", directoryName, dPageItem.FileName))
			if err != nil {
				bdm.logger.Log("failed to delete page. Rolling back...", util.Warn, "db", db, "blob", blob, "page", dPageItem.FileName, "error", err.Error())
				pageItems = append(pageItems, dPageItem)
				err = bdm.WritePagesFile(directoryName, pageItems)
				if err != nil {
					bdm.logger.Log("roll back operation failed. Corrupt page", util.Error, "db", db, "blob", blob, "page", dPageItem.FileName, "error", err.Error())
					panic(err.Error())
				}
			}
			return nil
		}
	}
	return errors.New(fmt.Sprintf("could not find page %s", dPageItem.FileName))
}

func (bdm blobDiskManager) DeleteIndexFile(db string, blob string, dFileName string) error {
	prefixIndexItemsMap, err := bdm.GetPrefixIndexItems(db, blob)
	if err != nil {
		return err
	}
	for prefix, prefixIndexItem := range prefixIndexItemsMap {
		for index, fileName := range prefixIndexItem.FileNames {
			if fileName == dFileName {
				copy(prefixIndexItemsMap[prefix].FileNames[index:], prefixIndexItemsMap[prefix].FileNames[index+1:])
				prefixIndexItemsMap[prefix].FileNames[len(prefixIndexItemsMap[prefix].FileNames)-1] = ""
				temp, _ := prefixIndexItemsMap[prefix]
				temp.FileNames = prefixIndexItemsMap[prefix].FileNames[:len(prefixIndexItemsMap[prefix].FileNames)-1]
				prefixIndexItemsMap[prefix] = temp

				directoryName := fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob)
				err = bdm.WriteIndexPagesFile(directoryName, prefixIndexItemsMap)
				if err != nil {
					return err
				}
				err = os.Remove(fmt.Sprintf("%s/%s", directoryName, dFileName))
				if err != nil {
					bdm.logger.Log("failed to delete index page. Rolling back...", util.Warn, "db", db, "blob", blob, "page", dFileName, "error", err.Error())
					temp, _ = prefixIndexItemsMap[prefix]
					temp.FileNames = append(temp.FileNames, dFileName)
					prefixIndexItemsMap[prefix] = temp
					err = bdm.WriteIndexPagesFile(directoryName, prefixIndexItemsMap)
					if err != nil {
						bdm.logger.Log("roll back operation failed. Corrupt index page", util.Error, "db", db, "blob", blob, "page", dFileName, "error", err.Error())
						panic(err.Error())
					}
				}
				return nil
			}
		}
	}
	return errors.New(fmt.Sprintf("could not find index %s", dFileName))
}

func (bdm blobDiskManager) WritePagesFile(directoryName string, pageItems []objects.PageItem) error {
	pagesData, _ := json.Marshal(pageItems)
	return bdm.WriteFile(directoryName, pagesData, constants.PagesFile)
}

func (bdm blobDiskManager) WriteIndexPagesFile(directoryName string, indexItems map[string]objects.PrefixIndexItem) error {
	indexData, _ := json.Marshal(indexItems)
	return bdm.WriteFile(directoryName, indexData, constants.IndexesFile)
}

func (bdm blobDiskManager) CreateFile(directory string, fileData []byte, fileName string) error {
	filePath := fmt.Sprintf("%s/%s", directory, fileName)
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}

	defer file.Close()

	return bdm.WriteFile(directory, fileData, fileName)
}

func (bdm blobDiskManager) WriteFile(directory string, fileData []byte, fileName string) error {
	filePath := fmt.Sprintf("%s/%s", directory, fileName)
	return os.WriteFile(filePath, fileData, 0777)
}
