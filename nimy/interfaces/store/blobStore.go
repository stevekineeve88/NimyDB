package store

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"nimy/constants"
	"nimy/interfaces/disk"
	"nimy/interfaces/objects"
	"nimy/interfaces/rules"
)

type BlobStore interface {
	AddRecord(db string, blob string, record map[string]any) (string, error)
	AddRecordsBulk(db string, blob string, insertRecords []map[string]any) (string, error)
	GetRecord(db string, blob string, recordId string) (map[string]any, error)
	GetRecordFullScan(db string, blob string, recordId string) (map[string]any, error)
	DeleteRecord(db string, blob string, recordId string) error
}

type blobStore struct {
	blobDiskManager disk.BlobDiskManager
}

func CreateBlobStore(blobDiskManager disk.BlobDiskManager) BlobStore {
	return blobStore{
		blobDiskManager: blobDiskManager,
	}
}

func (bs blobStore) AddRecord(db string, blob string, record map[string]any) (string, error) {
	return bs.AddRecordsBulk(db, blob, []map[string]any{record})
}

func (bs blobStore) AddRecordsBulk(db string, blob string, insertRecords []map[string]any) (string, error) {
	format, err := bs.blobDiskManager.GetFormat(db, blob)
	if err != nil {
		return "", err
	}
	pageItems, err := bs.blobDiskManager.GetPages(db, blob)
	if err != nil {
		return "", err
	}
	currentLastPage := pageItems[len(pageItems)-1]
	recordMap, err := bs.blobDiskManager.GetPage(db, blob, currentLastPage)
	if err != nil {
		return "", err
	}
	blobRules := rules.CreateBlobRules(blob, format)
	toInsert := false
	lastRecordId := ""
	indexes := make(map[string]string)
	for _, insertRecord := range insertRecords {
		if len(recordMap) > constants.MaxPageSize/len(format.GetMap()) {
			if toInsert {
				err = bs.blobDiskManager.WritePage(db, blob, currentLastPage, recordMap)
				if err != nil {
					return "", err
				}
				toInsert = false
			}
			currentLastPage, err = bs.blobDiskManager.CreatePage(db, blob)
			if err != nil {
				return "", err
			}
			recordMap = make(map[string]map[string]any)
		}
		err = blobRules.FormatRecord(insertRecord)
		if err != nil {
			return "", err
		}
		lastRecordId = uuid.New().String()
		recordMap[lastRecordId] = insertRecord
		indexes[lastRecordId] = currentLastPage.FileName
		toInsert = true
	}
	if toInsert {
		err = bs.blobDiskManager.WritePage(db, blob, currentLastPage, recordMap)
		if err != nil {
			return "", err
		}
	}
	for recordId, pageFile := range indexes {
		err = bs.addIndex(db, blob, recordId, pageFile)
		if err != nil {
			return "", err
		}
	}
	return lastRecordId, nil
}

func (bs blobStore) GetRecord(db string, blob string, recordId string) (map[string]any, error) {
	indexRootMap, err := bs.blobDiskManager.GetIndexPages(db, blob)
	if err != nil {
		return bs.GetRecordFullScan(db, blob, recordId)
	}
	indexItems, ok := indexRootMap[recordId[0:1]]
	if !ok {
		return bs.GetRecordFullScan(db, blob, recordId)
	}
	for _, fileName := range indexItems.FileNames {
		indexMap, err := bs.blobDiskManager.GetIndexPage(db, blob, fileName)
		if err != nil {
			return bs.GetRecordFullScan(db, blob, recordId)
		}
		pageFileName, ok := indexMap[recordId]
		if ok {
			recordMap, err := bs.blobDiskManager.GetPage(db, blob, objects.PageItem{FileName: pageFileName})
			if err != nil {
				return bs.GetRecordFullScan(db, blob, recordId)
			}
			record, ok := recordMap[recordId]
			if !ok {
				return bs.GetRecordFullScan(db, blob, recordId)
			}
			return record, nil
		}
	}
	return bs.GetRecordFullScan(db, blob, recordId)
}

func (bs blobStore) GetRecordFullScan(db string, blob string, recordId string) (map[string]any, error) {
	pageItems, err := bs.blobDiskManager.GetPages(db, blob)
	if err != nil {
		return nil, err
	}
	for _, pageItem := range pageItems {
		recordMap, err := bs.blobDiskManager.GetPage(db, blob, pageItem)
		if err != nil {
			return nil, err
		}
		record, ok := recordMap[recordId]
		if ok {
			return record, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("no record found with ID %s in blob %s", recordId, blob))
}

func (bs blobStore) DeleteRecord(db string, blob string, recordId string) error {
	pageItems, err := bs.blobDiskManager.GetPages(db, blob)
	if err != nil {
		return err
	}
	for _, pageItem := range pageItems {
		records, err := bs.blobDiskManager.GetPage(db, blob, pageItem)
		if err != nil {
			return err
		}
		_, ok := records[recordId]
		if ok {
			delete(records, recordId)
			return bs.blobDiskManager.WritePage(db, blob, pageItem, records)
		}
	}
	return errors.New(fmt.Sprintf("no record found with ID %s in blob %s", recordId, blob))
}

/*func (bs blobStore) addIndexes(db string, blob string, indexMap map[string]string) error {
	indexPage, err := bs.blobDiskManager.GetIndexPages(db, blob)
	if err != nil {
		return err
	}
	prefixFileMap := make(map[string]string)
	prefixIndexMap := make(map[string]map[string]string)
	for prefix, indexItem := range indexPage {
		prefixFileMap[prefix] = indexItem.FileNames[len(indexItem.FileNames)-1]
	}

	for recordId, pageFile := range indexMap {
		prefixIndexes, ok := prefixIndexMap[recordId[0:1]]
		if !ok {
			prefixFile, ok := prefixFileMap[recordId[0:1]]
			if !ok {
				indexItem, err := bs.blobDiskManager.CreateIndexPage(db, blob, recordId[0:1])
				if err != nil {
					return err
				}
				prefixFileMap[recordId[0:1]] = indexItem.FileNames[len(indexItem.FileNames)-1]
			}
			indexes, err := bs.blobDiskManager.GetIndexPage(db, blob, prefixFile)
			if err != nil {
				return err
			}
			prefixIndexMap[recordId[0:1]] = indexes
			prefixIndexes = indexes
		}

	}
}*/

func (bs blobStore) addIndex(db string, blob string, recordId string, pageFile string) error {
	indexPage, err := bs.blobDiskManager.GetIndexPages(db, blob)
	if err != nil {
		return err
	}
	prefix := recordId[0:1]
	indexFiles, ok := indexPage[prefix]
	if !ok {
		indexFiles, err = bs.blobDiskManager.CreateIndexPage(db, blob, prefix)
		if err != nil {
			return err
		}
	}
	indexFileName := indexFiles.FileNames[len(indexFiles.FileNames)-1]
	indexes, err := bs.blobDiskManager.GetIndexPage(db, blob, indexFileName)
	if err != nil {
		return err
	}
	indexes[recordId] = pageFile
	err = bs.blobDiskManager.WriteIndexPage(db, blob, indexFileName, indexes)
	if err != nil {
		return err
	}
	if len(indexes) > constants.MaxIndexSize {
		_, err = bs.blobDiskManager.CreateIndexPage(db, blob, prefix)
		return err
	}
	return nil
}
