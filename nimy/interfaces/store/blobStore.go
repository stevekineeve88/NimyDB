package store

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"nimy/constants"
	"nimy/interfaces/disk"
	"nimy/interfaces/objects"
)

type BlobStore interface {
	CreateBlob(db string, blob string, format objects.Format) (objects.Blob, error)
	DeleteBlob(db string, blob string) error
	AddRecord(db string, blob string, record map[string]any) (string, error)
	AddRecords(db string, blob string, insertRecords []map[string]any) (string, error)
	GetRecordByIndex(db string, blob string, recordId string) (map[string]any, error)
	GetRecordFullScan(db string, blob string, recordId string) (map[string]any, error)
	DeleteRecord(db string, blob string, recordId string) error
	AddIndexes(db string, blob string, indexMap map[string]string) error
}

type blobStore struct {
	blobDiskManager disk.BlobDiskManager
}

func CreateBlobStore(blobDiskManager disk.BlobDiskManager) BlobStore {
	return blobStore{
		blobDiskManager: blobDiskManager,
	}
}

func (bs blobStore) CreateBlob(db string, blob string, format objects.Format) (objects.Blob, error) {
	blobObj := objects.CreateBlob(blob, format)
	if err := blobObj.HasBlobNameConvention(); err != nil {
		return blobObj, err
	}
	if err := blobObj.HasFormatStructure(); err != nil {
		return blobObj, err
	}
	return blobObj, bs.blobDiskManager.CreateBlob(db, blob, format)
}

func (bs blobStore) DeleteBlob(db string, blob string) error {
	if !bs.blobDiskManager.BlobExists(db, blob) {
		return errors.New(fmt.Sprintf("%s.%s does not exist", db, blob))
	}
	return bs.blobDiskManager.DeleteBlob(db, blob)
}

func (bs blobStore) AddRecord(db string, blob string, record map[string]any) (string, error) {
	return bs.AddRecords(db, blob, []map[string]any{record})
}

func (bs blobStore) AddRecords(db string, blob string, insertRecords []map[string]any) (string, error) {
	format, err := bs.blobDiskManager.GetFormat(db, blob)
	if err != nil {
		return "", err
	}
	pageItems, err := bs.blobDiskManager.GetPageItems(db, blob)
	if err != nil {
		return "", err
	}
	currentLastPage := pageItems[len(pageItems)-1]
	recordMap, err := bs.blobDiskManager.GetPageData(db, blob, currentLastPage.FileName)
	if err != nil {
		return "", err
	}
	blobObj := objects.CreateBlob(blob, format)
	lastRecordId := ""
	indexMap := make(map[string]string)
	for _, insertRecord := range insertRecords {
		err = blobObj.FormatRecord(insertRecord)
		if err != nil {
			return lastRecordId, err
		}
		lastRecordId = uuid.New().String()
		recordMap[lastRecordId] = insertRecord
		indexMap[lastRecordId] = currentLastPage.FileName
		if len(recordMap) > constants.MaxPageSize/len(format.GetMap()) {
			err = bs.blobDiskManager.WritePageData(db, blob, currentLastPage.FileName, recordMap)
			if err != nil {
				return lastRecordId, err
			}
			currentLastPage, err = bs.blobDiskManager.CreatePage(db, blob)
			if err != nil {
				return lastRecordId, err
			}
			recordMap = make(map[string]map[string]any)
		}
	}
	err = bs.blobDiskManager.WritePageData(db, blob, currentLastPage.FileName, recordMap)
	if err != nil {
		return lastRecordId, err
	}
	err = bs.AddIndexes(db, blob, indexMap)
	return lastRecordId, err
}

func (bs blobStore) GetRecordByIndex(db string, blob string, recordId string) (map[string]any, error) {
	indexPrefixMap, err := bs.blobDiskManager.GetPrefixIndexItems(db, blob)
	if err != nil {
		return nil, err
	}
	indexPrefixItem, ok := indexPrefixMap[constants.GetRecordIdPrefix(recordId)]
	if !ok {
		return nil, err
	}
	for _, indexFileName := range indexPrefixItem.FileNames {
		indexMap, err := bs.blobDiskManager.GetIndexData(db, blob, indexFileName)
		if err != nil {
			return nil, err
		}
		pageFileName, ok := indexMap[recordId]
		if ok {
			recordMap, err := bs.blobDiskManager.GetPageData(db, blob, pageFileName)
			if err != nil {
				return nil, err
			}
			record, ok := recordMap[recordId]
			if !ok {
				return nil, err
			}
			return record, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("no record found with ID %s in blob %s", recordId, blob))
}

func (bs blobStore) GetRecordFullScan(db string, blob string, recordId string) (map[string]any, error) {
	pageItems, err := bs.blobDiskManager.GetPageItems(db, blob)
	if err != nil {
		return nil, err
	}
	for _, pageItem := range pageItems {
		recordMap, err := bs.blobDiskManager.GetPageData(db, blob, pageItem.FileName)
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
	indexPrefixMap, err := bs.blobDiskManager.GetPrefixIndexItems(db, blob)
	if err != nil {
		return err
	}
	indexPrefixItem, ok := indexPrefixMap[constants.GetRecordIdPrefix(recordId)]
	if !ok {
		return err
	}
	for _, indexFileName := range indexPrefixItem.FileNames {
		indexMap, err := bs.blobDiskManager.GetIndexData(db, blob, indexFileName)
		if err != nil {
			return err
		}
		pageFileName, ok := indexMap[recordId]
		if ok {
			pageItem := objects.PageItem{FileName: pageFileName}
			recordMap, err := bs.blobDiskManager.GetPageData(db, blob, pageItem.FileName)
			if err != nil {
				return err
			}
			delete(recordMap, recordId)
			delete(indexMap, recordId)
			err = bs.blobDiskManager.WritePageData(db, blob, pageItem.FileName, recordMap)
			if err != nil {
				return err
			}
			err = bs.blobDiskManager.WriteIndexData(db, blob, indexFileName, indexMap)
			if err != nil {
				return err
			}
			if len(recordMap) == 0 {
				_ = bs.blobDiskManager.DeletePageItem(db, blob, pageItem)
			}
			if len(indexMap) == 0 {
				_ = bs.blobDiskManager.DeleteIndexFile(db, blob, indexFileName)
			}
			return nil
		}
	}
	return errors.New(fmt.Sprintf("no record found with ID %s in blob %s", recordId, blob))
}

func (bs blobStore) AddIndexes(db string, blob string, indexMap map[string]string) error {
	indexPrefixMap, err := bs.blobDiskManager.GetPrefixIndexItems(db, blob)
	if err != nil {
		return err
	}
	indexPrefixFileMap := make(map[string]string)
	indexPrefixDataMap := make(map[string]map[string]string)
	for prefix, indexItem := range indexPrefixMap {
		if len(indexItem.FileNames) != 0 {
			indexPrefixFileMap[prefix] = indexItem.FileNames[len(indexItem.FileNames)-1]
		}
	}

	for recordId, pageFile := range indexMap {
		prefix := constants.GetRecordIdPrefix(recordId)
		_, ok := indexPrefixDataMap[prefix]
		if !ok {
			_, ok := indexPrefixFileMap[prefix]
			if !ok {
				indexItem, err := bs.blobDiskManager.CreateIndexPage(db, blob, prefix)
				if err != nil {
					return err
				}
				indexPrefixFileMap[prefix] = indexItem.FileNames[len(indexItem.FileNames)-1]
			}
			prefixIndexMap, err := bs.blobDiskManager.GetIndexData(db, blob, indexPrefixFileMap[prefix])
			if err != nil {
				return err
			}
			indexPrefixDataMap[prefix] = prefixIndexMap
		}
		indexPrefixDataMap[prefix][recordId] = pageFile
		if len(indexPrefixDataMap[prefix]) > constants.MaxIndexSize {
			err = bs.blobDiskManager.WriteIndexData(db, blob, indexPrefixFileMap[prefix], indexPrefixDataMap[prefix])
			if err != nil {
				return err
			}
			indexItem, err := bs.blobDiskManager.CreateIndexPage(db, blob, prefix)
			if err != nil {
				return err
			}
			indexPrefixFileMap[prefix] = indexItem.FileNames[len(indexItem.FileNames)-1]
			delete(indexPrefixDataMap, prefix)
		}
	}
	for prefix, indexData := range indexPrefixDataMap {
		err = bs.blobDiskManager.WriteIndexData(db, blob, indexPrefixFileMap[prefix], indexData)
		if err != nil {
			return err
		}
	}
	return nil
}
