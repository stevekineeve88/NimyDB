package store

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"nimy/interfaces/disk"
	"nimy/interfaces/rules"
)

type BlobStore interface {
	AddRecord(db string, blob string, record map[string]any) (string, error)
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
	records, err := bs.blobDiskManager.GetPage(db, blob, pageItems[0])
	if err != nil {
		return "", err
	}
	blobRules := rules.CreateBlobRules(blob, format)
	err = blobRules.FormatRecord(record)
	if err != nil {
		return "", err
	}
	recordId := uuid.New().String()
	records[recordId] = record
	return recordId, bs.blobDiskManager.WritePage(db, blob, pageItems[0], records)
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
