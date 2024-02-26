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
	Create(db string, blob string, format objects.Format) error
	Delete(db string, blob string) error
	Exists(db string, blob string) bool
	CreatePage(db string, blob string) (objects.PageItem, error)
	CreateIndexPage(db string, blob string, prefix string) (objects.IndexItem, error)
	GetPages(db string, blob string) ([]objects.PageItem, error)
	GetIndexPages(db string, blob string) (map[string]objects.IndexItem, error)
	GetPage(db string, blob string, page objects.PageItem) (map[string]map[string]any, error)
	GetIndexPage(db string, blob string, fileName string) (map[string]string, error)
	GetPageInfo(db string, blob string, page objects.PageItem) (os.FileInfo, error)
	GetFormat(db string, blob string) (objects.Format, error)
	WritePage(db string, blob string, page objects.PageItem, records map[string]map[string]any) error
	WriteIndexPage(db string, blob string, fileName string, records map[string]string) error
	DeletePage(db string, blob string, dPage objects.PageItem) error
	DeleteIndexPage(db string, blob string, fileName string) error
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
	err := os.Mkdir(directoryName, 0777)
	if err != nil {
		return err
	}
	formatError := bd.createFormatFile(directoryName, format)
	pageError := bd.createPagesFile(directoryName)
	_, pageFileError := bd.CreatePage(db, blob)
	indexError := bd.createIndexesFile(directoryName)
	if formatError != nil || pageError != nil || pageFileError != nil || indexError != nil {
		err = bd.Delete(db, blob)
		if err != nil {
			panic(err.Error())
		}
		return errors.New("failed to initialize blob configuration")
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
		return blankPageItem, errors.New("failed to create page file")
	}
	return newPageItem, nil
}

func (bd blobDisk) CreateIndexPage(db string, blob string, prefix string) (objects.IndexItem, error) {
	blankIndexPageItem := objects.IndexItem{}
	indexItems, err := bd.GetIndexPages(db, blob)
	if err != nil {
		return blankIndexPageItem, nil
	}
	blobDirectory := fmt.Sprintf("%s/%s/%s", bd.dataLocation, db, blob)
	_, ok := indexItems[prefix]
	if !ok {
		indexItems[prefix] = objects.IndexItem{FileNames: []string{}}
	}
	fileName := fmt.Sprintf("index-%s.json", uuid.New().String())
	err = bd.createIndexPage(blobDirectory, fileName)
	if err != nil {
		return blankIndexPageItem, nil
	}
	indexItem, _ := indexItems[prefix]
	indexItem.FileNames = append(indexItem.FileNames, fileName)
	indexItems[prefix] = indexItem
	err = bd.writeIndexPagesFile(blobDirectory, indexItems)
	if err != nil {
		err = bd.deleteIndexPage(blobDirectory, fileName)
		if err != nil {
			panic(err.Error())
		}
		return blankIndexPageItem, errors.New("failed to create index file")
	}
	return indexItem, nil
}

func (bd blobDisk) GetFormat(db string, blob string) (objects.Format, error) {
	var formatItems map[string]objects.FormatItem
	file, err := os.ReadFile(fmt.Sprintf("%s/%s/%s/%s", bd.dataLocation, db, blob, constants.FormatFile))
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(file, &formatItems)
	if err != nil {
		return nil, err
	}
	return objects.CreateFormat(formatItems), nil
}

func (bd blobDisk) GetPages(db string, blob string) ([]objects.PageItem, error) {
	var pagesItems []objects.PageItem
	file, err := os.ReadFile(fmt.Sprintf("%s/%s/%s/%s", bd.dataLocation, db, blob, constants.PagesFile))
	if err != nil {
		return nil, err
	}
	unmarshalError := json.Unmarshal(file, &pagesItems)
	return pagesItems, unmarshalError
}

func (bd blobDisk) GetIndexPages(db string, blob string) (map[string]objects.IndexItem, error) {
	var indexPageItems map[string]objects.IndexItem
	file, err := os.ReadFile(fmt.Sprintf("%s/%s/%s/%s", bd.dataLocation, db, blob, constants.IndexesFile))
	if err != nil {
		return nil, err
	}
	unmarshalError := json.Unmarshal(file, &indexPageItems)
	return indexPageItems, unmarshalError
}

func (bd blobDisk) GetPage(db string, blob string, page objects.PageItem) (map[string]map[string]any, error) {
	var pageData map[string]map[string]any
	file, err := os.ReadFile(fmt.Sprintf("%s/%s/%s/%s", bd.dataLocation, db, blob, page.FileName))
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(file, &pageData)
	return pageData, err
}

func (bd blobDisk) GetIndexPage(db string, blob string, fileName string) (map[string]string, error) {
	var indexPageData map[string]string
	file, err := os.ReadFile(fmt.Sprintf("%s/%s/%s/%s", bd.dataLocation, db, blob, fileName))
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(file, &indexPageData)
	return indexPageData, err
}

func (bd blobDisk) GetPageInfo(db string, blob string, page objects.PageItem) (os.FileInfo, error) {
	return os.Stat(fmt.Sprintf("%s/%s/%s/%s", bd.dataLocation, db, blob, page.FileName))
}

func (bd blobDisk) WritePage(db string, blob string, page objects.PageItem, records map[string]map[string]any) error {
	directoryName := fmt.Sprintf("%s/%s/%s", bd.dataLocation, db, blob)
	recordData, _ := json.MarshalIndent(records, "", " ")
	return bd.writeFile(directoryName, recordData, page.FileName)
}

func (bd blobDisk) WriteIndexPage(db string, blob string, fileName string, records map[string]string) error {
	directoryName := fmt.Sprintf("%s/%s/%s", bd.dataLocation, db, blob)
	recordData, _ := json.MarshalIndent(records, "", " ")
	return bd.writeFile(directoryName, recordData, fileName)
}

func (bd blobDisk) DeletePage(db string, blob string, dPage objects.PageItem) error {
	pages, err := bd.GetPages(db, blob)
	if err != nil {
		return err
	}
	for index, page := range pages {
		if page.FileName == dPage.FileName {
			if len(pages) > 1 {
				copy(pages[index:], pages[index+1:])
				pages[len(pages)-1] = objects.PageItem{}
				pages = pages[:len(pages)-1]

				directoryName := fmt.Sprintf("%s/%s/%s", bd.dataLocation, db, blob)
				err = bd.writePagesFile(directoryName, pages)
				if err != nil {
					return err
				}
				err = bd.deletePage(directoryName, dPage)
				if err != nil {
					pages = append(pages, dPage)
					err = bd.writePagesFile(directoryName, pages)
					if err != nil {
						panic(err)
					}
				}
			}
			return nil
		}
	}
	return errors.New(fmt.Sprintf("could not find page %s", dPage.FileName))
}

func (bd blobDisk) DeleteIndexPage(db string, blob string, fileName string) error {
	prefixes, err := bd.GetIndexPages(db, blob)
	if err != nil {
		return err
	}
	for prefix, indexItems := range prefixes {
		for index, indexFile := range indexItems.FileNames {
			if indexFile == fileName {
				copy(prefixes[prefix].FileNames[index:], prefixes[prefix].FileNames[index+1:])
				prefixes[prefix].FileNames[len(prefixes[prefix].FileNames)-1] = ""
				temp, _ := prefixes[prefix]
				temp.FileNames = prefixes[prefix].FileNames[:len(prefixes[prefix].FileNames)-1]
				prefixes[prefix] = temp
				directoryName := fmt.Sprintf("%s/%s/%s", bd.dataLocation, db, blob)
				err = bd.writeIndexPagesFile(directoryName, prefixes)
				if err != nil {
					return err
				}
				err = bd.deleteIndexPage(directoryName, fileName)
				if err != nil {
					temp, _ = prefixes[prefix]
					temp.FileNames = append(temp.FileNames, fileName)
					prefixes[prefix] = temp
					err = bd.writeIndexPagesFile(directoryName, prefixes)
					if err != nil {
						panic(err)
					}
				}
				return nil
			}
		}
	}
	return errors.New(fmt.Sprintf("could not find index page %s", fileName))
}

func (bd blobDisk) createPage(directoryName string, pageItem objects.PageItem) error {
	pageData, _ := json.MarshalIndent(make(map[string]interface{}), "", " ")
	return bd.createFile(directoryName, pageData, pageItem.FileName)
}

func (bd blobDisk) createIndexPage(directoryName string, fileName string) error {
	indexData, _ := json.MarshalIndent(make(map[string]string), "", " ")
	return bd.createFile(directoryName, indexData, fileName)
}

func (bd blobDisk) deletePage(directoryName string, pageItem objects.PageItem) error {
	return os.Remove(fmt.Sprintf("%s/%s", directoryName, pageItem.FileName))
}

func (bd blobDisk) deleteIndexPage(directoryName string, fileName string) error {
	return os.Remove(fmt.Sprintf("%s/%s", directoryName, fileName))
}

func (bd blobDisk) writePagesFile(directoryName string, pageItems []objects.PageItem) error {
	pagesData, _ := json.MarshalIndent(pageItems, "", " ")
	return bd.writeFile(directoryName, pagesData, constants.PagesFile)
}

func (bd blobDisk) writeIndexPagesFile(directoryName string, indexItems map[string]objects.IndexItem) error {
	indexData, _ := json.MarshalIndent(indexItems, "", " ")
	return bd.writeFile(directoryName, indexData, constants.IndexesFile)
}

func (bd blobDisk) createFormatFile(directoryName string, format objects.Format) error {
	formatData, _ := json.MarshalIndent(format.GetMap(), "", " ")
	return bd.createFile(directoryName, formatData, constants.FormatFile)
}

func (bd blobDisk) createPagesFile(directoryName string) error {
	pageData, _ := json.MarshalIndent(make([]objects.PageItem, 0), "", " ")
	return bd.createFile(directoryName, pageData, constants.PagesFile)
}

func (bd blobDisk) createIndexesFile(directoryName string) error {
	indexData, _ := json.MarshalIndent(make(map[string]objects.IndexItem), "", " ")
	return bd.createFile(directoryName, indexData, constants.IndexesFile)
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
	return os.WriteFile(filePath, fileData, 0777)
}
