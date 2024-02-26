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
					return lastRecordId, err
				}
				toInsert = false
			}
			currentLastPage, err = bs.blobDiskManager.CreatePage(db, blob)
			if err != nil {
				return lastRecordId, err
			}
			recordMap = make(map[string]map[string]any)
		}
		err = blobRules.FormatRecord(insertRecord)
		if err != nil {
			return lastRecordId, err
		}
		lastRecordId = uuid.New().String()
		recordMap[lastRecordId] = insertRecord
		indexes[lastRecordId] = currentLastPage.FileName
		toInsert = true
	}
	if toInsert {
		err = bs.blobDiskManager.WritePage(db, blob, currentLastPage, recordMap)
		if err != nil {
			return lastRecordId, err
		}
	}
	err = bs.addIndexes(db, blob, indexes)
	return lastRecordId, err
}

func (bs blobStore) GetRecord(db string, blob string, recordId string) (map[string]any, error) {
	diskReads := 0
	indexRootMap, err := bs.blobDiskManager.GetIndexPages(db, blob)
	diskReads++
	if err != nil {
		return bs.GetRecordFullScan(db, blob, recordId)
	}
	indexItems, ok := indexRootMap[recordId[0:constants.IndexPrefixLength]]
	if !ok {
		return bs.GetRecordFullScan(db, blob, recordId)
	}
	for _, fileName := range indexItems.FileNames {
		indexMap, err := bs.blobDiskManager.GetIndexPage(db, blob, fileName)
		diskReads++
		if err != nil {
			return bs.GetRecordFullScan(db, blob, recordId)
		}
		pageFileName, ok := indexMap[recordId]
		if ok {
			recordMap, err := bs.blobDiskManager.GetPage(db, blob, objects.PageItem{FileName: pageFileName})
			diskReads++
			if err != nil {
				return bs.GetRecordFullScan(db, blob, recordId)
			}
			record, ok := recordMap[recordId]
			if !ok {
				return bs.GetRecordFullScan(db, blob, recordId)
			}
			fmt.Printf("Disk reads: %d\n", diskReads)
			return record, nil
		}
	}
	return bs.GetRecordFullScan(db, blob, recordId)
}

func (bs blobStore) GetRecordFullScan(db string, blob string, recordId string) (map[string]any, error) {
	diskReads := 0
	fmt.Println("FULL SCAN CALLED")
	pageItems, err := bs.blobDiskManager.GetPages(db, blob)
	diskReads++
	if err != nil {
		return nil, err
	}
	for _, pageItem := range pageItems {
		recordMap, err := bs.blobDiskManager.GetPage(db, blob, pageItem)
		diskReads++
		if err != nil {
			return nil, err
		}
		record, ok := recordMap[recordId]
		if ok {
			fmt.Printf("Disk reads: %d\n", diskReads)
			return record, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("no record found with ID %s in blob %s", recordId, blob))
}

func (bs blobStore) DeleteRecord(db string, blob string, recordId string) error {
	indexRootMap, err := bs.blobDiskManager.GetIndexPages(db, blob)
	if err != nil {
		return err
	}
	indexItems, ok := indexRootMap[recordId[0:constants.IndexPrefixLength]]
	if !ok {
		return err
	}
	for _, fileName := range indexItems.FileNames {
		indexMap, err := bs.blobDiskManager.GetIndexPage(db, blob, fileName)
		if err != nil {
			return err
		}
		pageFileName, ok := indexMap[recordId]
		if ok {
			pageItem := objects.PageItem{FileName: pageFileName}
			recordMap, err := bs.blobDiskManager.GetPage(db, blob, pageItem)
			if err != nil {
				return err
			}
			delete(recordMap, recordId)
			delete(indexMap, recordId)
			err = bs.blobDiskManager.WritePage(db, blob, pageItem, recordMap)
			if err != nil {
				return err
			}
			err = bs.blobDiskManager.WriteIndexPage(db, blob, fileName, indexMap)
			if err != nil {
				return err
			}
			if len(recordMap) == 0 {
				_ = bs.blobDiskManager.DeletePage(db, blob, pageItem)
			}
			if len(indexMap) == 0 {
				_ = bs.blobDiskManager.DeleteIndexPage(db, blob, fileName)
			}
			return nil
		}
	}
	return errors.New(fmt.Sprintf("no record found with ID %s in blob %s", recordId, blob))
}

func (bs blobStore) addIndexes(db string, blob string, indexMap map[string]string) error {
	indexPage, err := bs.blobDiskManager.GetIndexPages(db, blob)
	if err != nil {
		return err
	}
	prefixFileMap := make(map[string]string)
	prefixIndexMap := make(map[string]map[string]string)
	for prefix, indexItem := range indexPage {
		if len(indexItem.FileNames) != 0 {
			prefixFileMap[prefix] = indexItem.FileNames[len(indexItem.FileNames)-1]
		}
	}

	for recordId, pageFile := range indexMap {
		prefix := recordId[0:constants.IndexPrefixLength]
		_, ok := prefixIndexMap[prefix]
		if !ok {
			_, ok := prefixFileMap[prefix]
			if !ok {
				indexItem, err := bs.blobDiskManager.CreateIndexPage(db, blob, prefix)
				if err != nil {
					return err
				}
				prefixFileMap[prefix] = indexItem.FileNames[len(indexItem.FileNames)-1]
			}
			indexes, err := bs.blobDiskManager.GetIndexPage(db, blob, prefixFileMap[prefix])
			if err != nil {
				return err
			}
			prefixIndexMap[prefix] = indexes
		}
		prefixIndexMap[prefix][recordId] = pageFile
		if len(prefixIndexMap[prefix]) > constants.MaxIndexSize {
			err = bs.blobDiskManager.WriteIndexPage(db, blob, prefixFileMap[prefix], prefixIndexMap[prefix])
			if err != nil {
				return err
			}
			indexItem, err := bs.blobDiskManager.CreateIndexPage(db, blob, prefix)
			if err != nil {
				return err
			}
			prefixFileMap[prefix] = indexItem.FileNames[len(indexItem.FileNames)-1]
			delete(prefixIndexMap, prefix)
		}
	}
	for prefix, indexData := range prefixIndexMap {
		err = bs.blobDiskManager.WriteIndexPage(db, blob, prefixFileMap[prefix], indexData)
		if err != nil {
			return err
		}
	}
	return nil
}
