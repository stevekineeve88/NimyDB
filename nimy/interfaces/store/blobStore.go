package store

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"nimy/constants"
	"nimy/interfaces/disk"
	"nimy/interfaces/objects"
	"sync"
)

type BlobStore interface {
	CreateBlob(db string, blob string, format objects.Format) (objects.Blob, error)
	DeleteBlob(db string, blob string) error
	AddRecords(db string, blob string, insertRecords []map[string]any) (map[string]map[string]map[string]any, error)
	GetRecordByIndex(db string, blob string, recordId string) (map[string]map[string]map[string]any, error)
	GetRecordFullScan(db string, blob string, filterItems []objects.FilterItem) (map[string]map[string]map[string]any, error)
	DeleteRecordByIndex(db string, blob string, recordId string) (map[string]map[string]map[string]any, error)
	DeleteRecords(db string, blob string, filterItems []objects.FilterItem) (map[string]map[string]map[string]any, error)
	UpdateRecordByIndex(db string, blob string, recordId string, updateRecord map[string]any) (map[string]map[string]map[string]any, error)
	UpdateRecords(db string, blob string, updateRecord map[string]any, filterItems []objects.FilterItem) (map[string]map[string]map[string]any, error)
	AddIndexes(db string, blob string, indexMap map[string]string) error
	DeleteIndexes(db string, blob string, recordIds []string) error
	SearchPage(db string, blob string, fileName string, filter objects.Filter, groups *[constants.SearchThreadCount]map[string]map[string]any, wg *sync.WaitGroup, index int)
	SearchPageUpdate(db string, blob string, fileName string, filter objects.Filter, wg *sync.WaitGroup, updateRecordFormatted map[string]any, groups *[constants.SearchThreadCount]map[string]map[string]any, index int)
	SearchPageDelete(db string, blob string, fileName string, filter objects.Filter, wg *sync.WaitGroup, groups *[constants.SearchThreadCount]map[string]map[string]any, index int)
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

func (bs blobStore) AddRecords(db string, blob string, insertRecords []map[string]any) (map[string]map[string]map[string]any, error) {
	format, err := bs.blobDiskManager.GetFormat(db, blob)
	if err != nil {
		return nil, err
	}
	pageItems, err := bs.blobDiskManager.GetPageItems(db, blob)
	if err != nil {
		return nil, err
	}
	if len(pageItems) == 0 {
		pageItem, err := bs.blobDiskManager.CreatePage(db, blob)
		if err != nil {
			return nil, err
		}
		pageItems = append(pageItems, pageItem)
	}
	currentLastPage := pageItems[len(pageItems)-1]
	recordMap, err := bs.blobDiskManager.GetPageData(db, blob, currentLastPage.FileName)
	if err != nil {
		return nil, err
	}
	blobObj := objects.CreateBlob(blob, format)
	total := make(map[string]map[string]map[string]any)
	total[currentLastPage.FileName] = make(map[string]map[string]any)
	indexMap := make(map[string]string)
	for _, insertRecord := range insertRecords {
		newInsertRecord, err := blobObj.FormatRecord(insertRecord)
		if err != nil {
			return total, err
		}
		lastRecordId := uuid.New().String()
		recordMap[lastRecordId] = newInsertRecord
		total[currentLastPage.FileName][lastRecordId] = newInsertRecord
		indexMap[lastRecordId] = currentLastPage.FileName
		if len(recordMap) > constants.MaxPageSize {
			err = bs.blobDiskManager.WritePageData(db, blob, currentLastPage.FileName, recordMap)
			if err != nil {
				delete(total, currentLastPage.FileName)
				return total, err
			}
			total[currentLastPage.FileName] = recordMap
			currentLastPage, err = bs.blobDiskManager.CreatePage(db, blob)
			if err != nil {
				return total, err
			}
			total[currentLastPage.FileName] = make(map[string]map[string]any)
			recordMap = make(map[string]map[string]any)
		}
	}
	err = bs.blobDiskManager.WritePageData(db, blob, currentLastPage.FileName, recordMap)
	if err != nil {
		delete(total, currentLastPage.FileName)
		return total, err
	}
	err = bs.AddIndexes(db, blob, indexMap)
	return total, err
}

func (bs blobStore) GetRecordByIndex(db string, blob string, recordId string) (map[string]map[string]map[string]any, error) {
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
				return nil, errors.New(fmt.Sprintf("index %s is corrupted", recordId))
			}
			return map[string]map[string]map[string]any{
				pageFileName: {
					recordId: record,
				},
			}, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("no record found with ID %s in blob %s", recordId, blob))
}

func (bs blobStore) GetRecordFullScan(db string, blob string, filterItems []objects.FilterItem) (map[string]map[string]map[string]any, error) {
	format, err := bs.blobDiskManager.GetFormat(db, blob)
	if err != nil {
		return nil, err
	}
	filter := objects.Filter{FilterItems: filterItems, Format: format}
	err = filter.ConvertFilterItems()
	if err != nil {
		return nil, err
	}
	pageItems, err := bs.blobDiskManager.GetPageItems(db, blob)
	if err != nil {
		return nil, err
	}
	var wg sync.WaitGroup
	total := make(map[string]map[string]map[string]any)
	for i := 0; i < len(pageItems); i += constants.SearchThreadCount {
		var groups [constants.SearchThreadCount]map[string]map[string]any
		threadItem := i
		threadIndex := 0
		for threadItem < len(pageItems) && threadIndex < constants.SearchThreadCount {
			wg.Add(1)
			go bs.SearchPage(db, blob, pageItems[threadItem].FileName, filter, &groups, &wg, threadIndex)
			threadItem++
			threadIndex++
		}
		wg.Wait()
		currentFileIndex := i

		for _, groupItem := range groups {
			if len(groupItem) == 0 {
				currentFileIndex++
				continue
			}
			total[pageItems[currentFileIndex].FileName] = groupItem
			currentFileIndex++
		}
	}
	return total, nil
}

func (bs blobStore) DeleteRecordByIndex(db string, blob string, recordId string) (map[string]map[string]map[string]any, error) {
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
			pageItem := objects.PageItem{FileName: pageFileName}
			recordMap, err := bs.blobDiskManager.GetPageData(db, blob, pageItem.FileName)
			if err != nil {
				return nil, err
			}
			record := recordMap[recordId]

			delete(recordMap, recordId)
			delete(indexMap, recordId)

			if len(recordMap) == 0 {
				err = bs.blobDiskManager.DeletePageItem(db, blob, pageItem)
				if err != nil {
					return nil, err
				}
			} else {
				err = bs.blobDiskManager.WritePageData(db, blob, pageItem.FileName, recordMap)
				if err != nil {
					return nil, err
				}
			}

			if len(indexMap) == 0 {
				err = bs.blobDiskManager.DeleteIndexFile(db, blob, indexFileName)
				if err != nil {
					panic(err.Error())
				}
			} else {
				err = bs.blobDiskManager.WriteIndexData(db, blob, indexFileName, indexMap)
				if err != nil {
					panic(err.Error())
				}
			}
			return map[string]map[string]map[string]any{
				pageFileName: {
					recordId: record,
				},
			}, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("no record found with ID %s in blob %s", recordId, blob))
}

func (bs blobStore) DeleteRecords(db string, blob string, filterItems []objects.FilterItem) (map[string]map[string]map[string]any, error) {
	format, err := bs.blobDiskManager.GetFormat(db, blob)
	if err != nil {
		return nil, err
	}
	filter := objects.Filter{FilterItems: filterItems, Format: format}
	err = filter.ConvertFilterItems()
	if err != nil {
		return nil, err
	}
	pageItems, err := bs.blobDiskManager.GetPageItems(db, blob)
	if err != nil {
		return nil, err
	}
	var wg sync.WaitGroup
	total := make(map[string]map[string]map[string]any)
	for i := 0; i < len(pageItems); i += constants.SearchThreadCount {
		var groups [constants.SearchThreadCount]map[string]map[string]any
		threadItem := i
		threadIndex := 0
		for threadItem < len(pageItems) && threadIndex < constants.SearchThreadCount {
			wg.Add(1)
			go bs.SearchPageDelete(db, blob, pageItems[threadItem].FileName, filter, &wg, &groups, threadIndex)
			threadItem++
			threadIndex++
		}
		wg.Wait()
		currentFileIndex := i

		for _, groupItem := range groups {
			if len(groupItem) == 0 {
				currentFileIndex++
				continue
			}
			recordIds := []string{}
			for recordId, _ := range groupItem {
				recordIds = append(recordIds, recordId)
			}
			err = bs.DeleteIndexes(db, blob, recordIds)
			if err != nil {
				panic(err.Error())
			}
			total[pageItems[currentFileIndex].FileName] = groupItem
			currentFileIndex++
		}
	}
	return total, nil
}

func (bs blobStore) UpdateRecordByIndex(db string, blob string, recordId string, updateRecord map[string]any) (map[string]map[string]map[string]any, error) {
	format, err := bs.blobDiskManager.GetFormat(db, blob)
	if err != nil {
		return nil, err
	}
	indexPrefixMap, err := bs.blobDiskManager.GetPrefixIndexItems(db, blob)
	if err != nil {
		return nil, err
	}
	indexPrefixItem, ok := indexPrefixMap[constants.GetRecordIdPrefix(recordId)]
	if !ok {
		return nil, err
	}
	blobObj := objects.CreateBlob(blob, format)
	updateRecordFormatted, err := blobObj.FormatUpdateRecord(updateRecord)
	if err != nil {
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
			_, ok = recordMap[recordId]
			if !ok {
				return nil, errors.New(fmt.Sprintf("index %s is corrupted", recordId))
			}
			for key, value := range updateRecordFormatted {
				recordMap[recordId][key] = value
			}
			err = bs.blobDiskManager.WritePageData(db, blob, pageFileName, recordMap)
			if err != nil {
				return nil, err
			}
			return map[string]map[string]map[string]any{
				pageFileName: {
					recordId: recordMap[recordId],
				},
			}, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("no record found with ID %s in blob %s", recordId, blob))
}

func (bs blobStore) UpdateRecords(db string, blob string, updateRecord map[string]any, filterItems []objects.FilterItem) (map[string]map[string]map[string]any, error) {
	format, err := bs.blobDiskManager.GetFormat(db, blob)
	if err != nil {
		return nil, err
	}
	filter := objects.Filter{FilterItems: filterItems, Format: format}
	err = filter.ConvertFilterItems()
	if err != nil {
		return nil, err
	}
	pageItems, err := bs.blobDiskManager.GetPageItems(db, blob)
	if err != nil {
		return nil, err
	}
	blobObj := objects.CreateBlob(blob, format)
	updateRecordFormatted, err := blobObj.FormatUpdateRecord(updateRecord)
	if err != nil {
		return nil, err
	}
	total := make(map[string]map[string]map[string]any)
	var wg sync.WaitGroup
	for i := 0; i < len(pageItems); i += constants.SearchThreadCount {
		var groups [constants.SearchThreadCount]map[string]map[string]any
		threadItem := i
		threadIndex := 0
		for threadItem < len(pageItems) && threadIndex < constants.SearchThreadCount {
			wg.Add(1)
			go bs.SearchPageUpdate(db, blob, pageItems[threadItem].FileName, filter, &wg, updateRecordFormatted, &groups, threadIndex)
			threadItem++
			threadIndex++
		}
		wg.Wait()
		currentFileIndex := i

		for _, groupItem := range groups {
			if len(groupItem) == 0 {
				currentFileIndex++
				continue
			}
			total[pageItems[currentFileIndex].FileName] = groupItem
			currentFileIndex++
		}
	}
	return total, nil
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

func (bs blobStore) SearchPage(db string, blob string, fileName string, filter objects.Filter, groups *[constants.SearchThreadCount]map[string]map[string]any, wg *sync.WaitGroup, index int) {
	defer wg.Done()
	groupItem := make(map[string]map[string]any)
	pageData, err := bs.blobDiskManager.GetPageData(db, blob, fileName)
	if err != nil {
		return
	}
	for key, record := range pageData {
		if passes, _ := filter.Passes(record); passes {
			groupItem[key] = record
		}
	}
	groups[index] = groupItem
}

func (bs blobStore) SearchPageUpdate(db string, blob string, fileName string, filter objects.Filter, wg *sync.WaitGroup, updateRecordFormatted map[string]any, groups *[constants.SearchThreadCount]map[string]map[string]any, index int) {
	defer wg.Done()
	pageData, err := bs.blobDiskManager.GetPageData(db, blob, fileName)
	if err != nil {
		return
	}
	groupItem := make(map[string]map[string]any)
	affected := 0
	for recordId, record := range pageData {
		if passes, _ := filter.Passes(record); passes {
			for key, value := range updateRecordFormatted {
				pageData[recordId][key] = value
			}
			groupItem[recordId] = pageData[recordId]
			affected++
		}
	}
	if affected > 0 {
		err = bs.blobDiskManager.WritePageData(db, blob, fileName, pageData)
		if err != nil {
			return
		}
	}
	groups[index] = groupItem
}

func (bs blobStore) SearchPageDelete(db string, blob string, fileName string, filter objects.Filter, wg *sync.WaitGroup, groups *[constants.SearchThreadCount]map[string]map[string]any, index int) {
	defer wg.Done()
	pageData, err := bs.blobDiskManager.GetPageData(db, blob, fileName)
	if err != nil {
		return
	}
	groupItem := make(map[string]map[string]any)
	deletedItems := false
	for recordId, record := range pageData {
		if passes, _ := filter.Passes(record); passes {
			groupItem[recordId] = pageData[recordId]
			delete(pageData, recordId)
			deletedItems = true
		}
	}
	if len(pageData) == 0 {
		err = bs.blobDiskManager.DeletePageItem(db, blob, objects.PageItem{FileName: fileName})
		if err != nil {
			return
		}
	} else if deletedItems {
		err = bs.blobDiskManager.WritePageData(db, blob, fileName, pageData)
	}
	groups[index] = groupItem
}

func (bs blobStore) DeleteIndexes(db string, blob string, recordIds []string) error {
	prefixIndexItems, err := bs.blobDiskManager.GetPrefixIndexItems(db, blob)
	if err != nil {
		return err
	}
	type indexDataField struct {
		indexData    map[string]string
		hasDeletions bool
	}
	indexDataMap := make(map[string]indexDataField)
	for _, recordId := range recordIds {
		indexPrefixItem, ok := prefixIndexItems[constants.GetRecordIdPrefix(recordId)]
		if !ok {
			continue
		}
		for _, fileName := range indexPrefixItem.FileNames {
			indexDataFieldItem, ok := indexDataMap[fileName]
			if !ok {
				indexData, err := bs.blobDiskManager.GetIndexData(db, blob, fileName)
				if err != nil {
					return err
				}
				indexDataMap[fileName] = indexDataField{
					indexData:    indexData,
					hasDeletions: false,
				}
			}
			indexDataFieldItem = indexDataMap[fileName]
			if _, ok := indexDataFieldItem.indexData[recordId]; ok {
				delete(indexDataFieldItem.indexData, recordId)
				indexDataFieldItem.hasDeletions = true
				indexDataMap[fileName] = indexDataFieldItem
				break
			}
		}
	}
	for fileName, indexDataFieldItem := range indexDataMap {
		if len(indexDataFieldItem.indexData) == 0 {
			err = bs.blobDiskManager.DeleteIndexFile(db, blob, fileName)
			if err != nil {
				return err
			}
		} else if indexDataFieldItem.hasDeletions {
			err = bs.blobDiskManager.WriteIndexData(db, blob, fileName, indexDataFieldItem.indexData)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
