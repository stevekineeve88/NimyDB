package store

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"nimy/constants"
	"nimy/interfaces/disk"
	"nimy/interfaces/rules"
)

type BlobStore interface {
	AddRecord(db string, blob string, record map[string]any) (string, error)
	AddRecordsBulk(db string, blob string, insertRecords []map[string]any) error
	GetRecord(db string, blob string, recordId string) (map[string]any, error)
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
	if len(recordMap) > constants.MaxPageSize/len(format.GetMap()) {
		currentLastPage, err = bs.blobDiskManager.CreatePage(db, blob)
		if err != nil {
			return "", err
		}
		recordMap = make(map[string]map[string]any)
	}
	blobRules := rules.CreateBlobRules(blob, format)
	err = blobRules.FormatRecord(record)
	if err != nil {
		return "", err
	}
	recordId := uuid.New().String()
	recordMap[recordId] = record
	return recordId, bs.blobDiskManager.WritePage(db, blob, currentLastPage, recordMap)
}

func (bs blobStore) AddRecordsBulk(db string, blob string, insertRecords []map[string]any) error {
	format, err := bs.blobDiskManager.GetFormat(db, blob)
	if err != nil {
		return err
	}
	pageItems, err := bs.blobDiskManager.GetPages(db, blob)
	if err != nil {
		return err
	}
	currentLastPage := pageItems[len(pageItems)-1]
	recordMap, err := bs.blobDiskManager.GetPage(db, blob, currentLastPage)
	if err != nil {
		return err
	}
	blobRules := rules.CreateBlobRules(blob, format)
	toInsert := false
	for _, insertRecord := range insertRecords {
		if len(recordMap) > constants.MaxPageSize/len(format.GetMap()) {
			if toInsert {
				err = bs.blobDiskManager.WritePage(db, blob, currentLastPage, recordMap)
				if err != nil {
					return err
				}
				toInsert = false
			}
			currentLastPage, err = bs.blobDiskManager.CreatePage(db, blob)
			if err != nil {
				return err
			}
			recordMap = make(map[string]map[string]any)
		}
		err = blobRules.FormatRecord(insertRecord)
		if err != nil {
			return err
		}
		recordMap[uuid.New().String()] = insertRecord
		toInsert = true
	}
	if toInsert {
		err = bs.blobDiskManager.WritePage(db, blob, currentLastPage, recordMap)
		if err != nil {
			return err
		}
	}
	return nil
}

func (bs blobStore) GetRecord(db string, blob string, recordId string) (map[string]any, error) {
	pageItems, err := bs.blobDiskManager.GetPages(db, blob)
	if err != nil {
		return nil, err
	}
	for _, pageItem := range pageItems {
		records, err := bs.blobDiskManager.GetPage(db, blob, pageItem)
		if err != nil {
			return nil, err
		}
		record, ok := records[recordId]
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
