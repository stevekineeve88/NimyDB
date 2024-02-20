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
	GetRecordOld(db string, blob string, recordId string) (map[string]any, error)
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
	indexItems, err := bs.blobDiskManager.GetIndexPages(db, blob)
	if err != nil {
		return "", err
	}
	currentLastIndexPage := indexItems[len(indexItems)-1]
	indexMap, err := bs.blobDiskManager.GetIndexPage(db, blob, currentLastIndexPage)
	if err != nil {
		return "", err
	}
	blobRules := rules.CreateBlobRules(blob, format)
	toInsert := false
	lastRecordId := ""
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
		if len(indexMap) > constants.MaxIndexSize {
			err = bs.blobDiskManager.WriteIndexPage(db, blob, currentLastIndexPage, indexMap)
			if err != nil {
				return "", err
			}
			currentLastIndexPage, err = bs.blobDiskManager.CreateIndexPage(db, blob)
			if err != nil {
				return "", err
			}
			indexMap = make(map[string]string)
		}
		err = blobRules.FormatRecord(insertRecord)
		if err != nil {
			return "", err
		}
		lastRecordId = uuid.New().String()
		recordMap[lastRecordId] = insertRecord
		indexMap[lastRecordId] = currentLastPage.FileName
		toInsert = true
	}
	if toInsert {
		err = bs.blobDiskManager.WritePage(db, blob, currentLastPage, recordMap)
		if err != nil {
			return "", err
		}
		err = bs.blobDiskManager.WriteIndexPage(db, blob, currentLastIndexPage, indexMap)
		if err != nil {
			return "", err
		}
	}
	return lastRecordId, nil
}

func (bs blobStore) GetRecord(db string, blob string, recordId string) (map[string]any, error) {
	indexItems, err := bs.blobDiskManager.GetIndexPages(db, blob)
	if err != nil {
		return nil, err
	}
	for _, indexItem := range indexItems {
		indexMap, err := bs.blobDiskManager.GetIndexPage(db, blob, indexItem)
		if err != nil {
			return nil, err
		}
		pageFileName, ok := indexMap[recordId]
		if ok {
			recordMap, err := bs.blobDiskManager.GetPage(db, blob, objects.PageItem{FileName: pageFileName})
			if err != nil {
				return nil, err
			}
			record, ok := recordMap[recordId]
			if !ok {
				return nil, errors.New(fmt.Sprintf("index misaligned with ID %s in blob %s", recordId, blob))
			}
			return record, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("no record found with ID %s in blob %s", recordId, blob))
}

func (bs blobStore) GetRecordOld(db string, blob string, recordId string) (map[string]any, error) {
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
