package disk

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"nimy/constants"
	"nimy/interfaces/objects"
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
	DeletePageItem(db string, blob string, dPage objects.PageItem) error
	DeleteIndexFile(db string, blob string, fileName string) error
	WritePagesFile(directoryName string, pageItems []objects.PageItem) error
	CreateFile(directory string, fileData []byte, fileName string) error
	WriteFile(directory string, fileData []byte, fileName string) error
}

type blobDiskManager struct {
	dataLocation string
}

func CreateBlobDiskManager(dataLocation string) BlobDiskManager {
	return blobDiskManager{
		dataLocation: dataLocation,
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
		func(db string, blob string) error {
			_, err = bdm.CreatePage(db, blob)
			return err
		},
		bdm.CreateIndexesFileAndDir,
	}

	for _, fileCreation := range fileCreations {
		err = fileCreation(db, blob)
		if err != nil {
			deleteBlobError := bdm.DeleteBlob(db, blob)
			if deleteBlobError != nil {
				panic(deleteBlobError.Error())
			}
			return err
		}
	}

	return nil
}

func (bdm blobDiskManager) DeleteBlob(db string, blob string) error {
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
	pageData, _ := json.MarshalIndent(make(map[string]interface{}), "", " ")
	err = bdm.CreateFile(blobDirectory, pageData, pageItem.FileName)
	if err != nil {
		return pageItem, err
	}
	pagesItems = append(pagesItems, pageItem)
	err = bdm.WritePagesFile(blobDirectory, pagesItems)
	if err != nil {
		deletePageError := os.Remove(fmt.Sprintf("%s/%s", blobDirectory, pageItem.FileName))
		if deletePageError != nil {
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
	indexMap, _ := json.MarshalIndent(make(map[string]string), "", " ")
	err = bdm.CreateFile(blobDirectory, indexMap, indexFileName)
	if err != nil {
		return prefixIndexItem, err
	}
	prefixIndexItem.FileNames = append(prefixIndexItem.FileNames, indexFileName)
	prefixIndexItemMap[prefix] = prefixIndexItem
	err = bdm.WriteIndexPagesFile(blobDirectory, prefixIndexItemMap)
	if err != nil {
		deleteIndexPageError := os.Remove(fmt.Sprintf("%s/%s", blobDirectory, indexFileName))
		if deleteIndexPageError != nil {
			panic(deleteIndexPageError.Error())
		}
		return prefixIndexItem, err
	}
	return prefixIndexItem, nil
}

func (bdm blobDiskManager) CreateFormatFile(db string, blob string, format objects.Format) error {
	formatData, _ := json.MarshalIndent(format.GetMap(), "", " ")
	return bdm.CreateFile(fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob), formatData, constants.FormatFile)
}

func (bdm blobDiskManager) CreatePagesFileAndDir(db string, blob string) error {
	pageData, _ := json.MarshalIndent(make([]objects.PageItem, 0), "", " ")
	err := bdm.CreateFile(fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob), pageData, constants.PagesFile)
	if err != nil {
		return err
	}
	return os.Mkdir(fmt.Sprintf("%s/%s/%s/%s", bdm.dataLocation, db, blob, constants.PagesDir), 0600)
}

func (bdm blobDiskManager) CreateIndexesFileAndDir(db string, blob string) error {
	indexData, _ := json.MarshalIndent(make(map[string]objects.PrefixIndexItem), "", " ")
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
		return nil, err
	}
	err = json.Unmarshal(file, &formatItems)
	if err != nil {
		return nil, err
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
	directoryName := fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob)
	recordData, _ := json.MarshalIndent(records, "", " ")
	return bdm.WriteFile(directoryName, recordData, fileName)
}

func (bdm blobDiskManager) WriteIndexData(db string, blob string, fileName string, records map[string]string) error {
	directoryName := fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob)
	recordData, _ := json.MarshalIndent(records, "", " ")
	return bdm.WriteFile(directoryName, recordData, fileName)
}

func (bdm blobDiskManager) DeletePageItem(db string, blob string, dPageItem objects.PageItem) error {
	pageItems, err := bdm.GetPageItems(db, blob)
	if err != nil {
		return err
	}
	for index, pageItem := range pageItems {
		if pageItem.FileName == dPageItem.FileName {
			if len(pageItems) > 1 {
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
					pageItems = append(pageItems, dPageItem)
					err = bdm.WritePagesFile(directoryName, pageItems)
					if err != nil {
						panic(err.Error())
					}
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
					temp, _ = prefixIndexItemsMap[prefix]
					temp.FileNames = append(temp.FileNames, dFileName)
					prefixIndexItemsMap[prefix] = temp
					err = bdm.WriteIndexPagesFile(directoryName, prefixIndexItemsMap)
					if err != nil {
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
	pagesData, _ := json.MarshalIndent(pageItems, "", " ")
	return bdm.WriteFile(directoryName, pagesData, constants.PagesFile)
}

func (bdm blobDiskManager) WriteIndexPagesFile(directoryName string, indexItems map[string]objects.PrefixIndexItem) error {
	indexData, _ := json.MarshalIndent(indexItems, "", " ")
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
