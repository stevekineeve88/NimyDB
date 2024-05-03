package store

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"nimy/constants"
	"nimy/interfaces/disk"
	"nimy/interfaces/objects"
	"strings"
	"sync"
)

type PartitionStore interface {
	CreatePartition(db string, blob string, format objects.Format, partition objects.Partition) (objects.Blob, error)
	AddRecords(db string, blob string, insertRecords []map[string]any) (map[string]map[string]map[string]any, error)
	GetRecordsByPartition(db string, blob string, searchPartition map[string]any, filterItems []objects.FilterItem) (map[string]map[string]map[string]any, error)
	UpdateRecordByIndex(db string, blob string, recordId string, updateRecord map[string]any) (map[string]map[string]map[string]any, error)
	UpdateRecordsByPartition(db string, blob string, updateRecord map[string]any, searchPartition map[string]any, filterItems []objects.FilterItem) (map[string]map[string]map[string]any, error)
	DeleteRecordByIndex(db string, blob string, recordId string) (map[string]map[string]map[string]any, error)
	DeleteRecordsByPartition(db string, blob string, searchPartition map[string]any, filterItems []objects.FilterItem) (map[string]map[string]map[string]any, error)
	IsPartition(db string, blob string) bool
	SearchPageDeleteWithHashKey(db string, blob string, fileName string, hashKey string, filter objects.Filter, wg *sync.WaitGroup, groups *[constants.SearchThreadCount]map[string]map[string]any, index int)
}

type partitionStore struct {
	partitionDiskManager disk.PartitionDiskManager
	blobDiskManager      disk.BlobDiskManager
	blobStore            BlobStore
}

func CreatePartitionStore(partitionDiskManager disk.PartitionDiskManager, blobDiskManager disk.BlobDiskManager, blobStore BlobStore) PartitionStore {
	return partitionStore{
		partitionDiskManager: partitionDiskManager,
		blobDiskManager:      blobDiskManager,
		blobStore:            blobStore,
	}
}

func (ps partitionStore) CreatePartition(db string, blob string, format objects.Format, partition objects.Partition) (objects.Blob, error) {
	blobObj := objects.CreateBlobWithPartition(blob, format, partition)
	if err := blobObj.HasBlobNameConvention(); err != nil {
		return blobObj, err
	}
	if err := blobObj.HasFormatStructure(); err != nil {
		return blobObj, err
	}
	if err := blobObj.HasPartitionStructure(); err != nil {
		return blobObj, err
	}
	return blobObj, ps.partitionDiskManager.CreatePartition(db, blob, format, partition)
}

func (ps partitionStore) AddRecords(db string, blob string, insertRecords []map[string]any) (map[string]map[string]map[string]any, error) {
	format, err := ps.blobDiskManager.GetFormat(db, blob)
	if err != nil {
		return nil, err
	}
	partition, err := ps.partitionDiskManager.GetPartition(db, blob)
	if err != nil {
		return nil, err
	}

	blobObj := objects.CreateBlobWithPartition(blob, format, partition)
	partitionHashMap := make(map[string][]map[string]any)
	for _, insertRecord := range insertRecords {
		newInsertRecord, err := blobObj.FormatRecord(insertRecord)
		if err != nil {
			return nil, err
		}
		partitionHashKey, err := blobObj.GetPartition().GetPartitionHashKey(newInsertRecord)
		if err != nil {
			return nil, err
		}
		_, ok := partitionHashMap[partitionHashKey]
		if !ok {
			partitionHashMap[partitionHashKey] = []map[string]any{}
		}
		partitionHashMap[partitionHashKey] = append(partitionHashMap[partitionHashKey], newInsertRecord)
	}

	total := make(map[string]map[string]map[string]any)
	for key, records := range partitionHashMap {
		partitionTotal, err := ps.addPartitionedRecords(db, blob, key, records)
		for pageFile, data := range partitionTotal {
			if len(data) > 0 {
				total[pageFile] = data
			}
		}
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (ps partitionStore) GetRecordsByPartition(db string, blob string, searchPartition map[string]any, filterItems []objects.FilterItem) (map[string]map[string]map[string]any, error) {
	format, err := ps.blobDiskManager.GetFormat(db, blob)
	if err != nil {
		return nil, err
	}
	filter := objects.Filter{FilterItems: filterItems, Format: format}
	err = filter.ConvertFilterItems()
	if err != nil {
		return nil, err
	}
	partitionHashKeyFileNames, err := ps.partitionDiskManager.GetPartitionHashKeyItemFileNames(db, blob)
	if err != nil {
		return nil, err
	}
	partition, err := ps.partitionDiskManager.GetPartition(db, blob)
	if err != nil {
		return nil, err
	}
	partitionHashKeyFileNames, err = ps.filterPartitionFiles(partitionHashKeyFileNames, partition, searchPartition)
	if err != nil {
		return nil, err
	}
	total := make(map[string]map[string]map[string]any)
	for _, partitionHashKeyFileName := range partitionHashKeyFileNames {
		hashKey := strings.Split(partitionHashKeyFileName, ".json")[0]
		partitionItem, err := ps.partitionDiskManager.GetPartitionHashKeyItem(db, blob, hashKey)
		if err != nil {
			return total, err
		}
		var wg sync.WaitGroup
		for i := 0; i < len(partitionItem.FileNames); i += constants.SearchThreadCount {
			var groups [constants.SearchThreadCount]map[string]map[string]any
			threadItem := i
			threadIndex := 0
			for threadItem < len(partitionItem.FileNames) && threadIndex < constants.SearchThreadCount {
				wg.Add(1)
				go ps.blobStore.SearchPage(db, blob, partitionItem.FileNames[threadItem], filter, &groups, &wg, threadIndex)
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
				total[partitionItem.FileNames[currentFileIndex]] = groupItem
				currentFileIndex++
			}
		}
	}
	return total, nil
}

func (ps partitionStore) UpdateRecordByIndex(db string, blob string, recordId string, updateRecord map[string]any) (map[string]map[string]map[string]any, error) {
	format, err := ps.blobDiskManager.GetFormat(db, blob)
	if err != nil {
		return nil, err
	}
	partition, err := ps.partitionDiskManager.GetPartition(db, blob)
	if err != nil {
		return nil, err
	}
	indexPrefixMap, err := ps.blobDiskManager.GetPrefixIndexItems(db, blob)
	if err != nil {
		return nil, err
	}
	indexPrefixItem, ok := indexPrefixMap[constants.GetRecordIdPrefix(recordId)]
	if !ok {
		return nil, err
	}
	blobObj := objects.CreateBlobWithPartition(blob, format, partition)
	updateRecordFormatted, err := blobObj.FormatUpdateRecord(updateRecord)
	if err != nil {
		return nil, err
	}
	for _, indexFileName := range indexPrefixItem.FileNames {
		indexMap, err := ps.blobDiskManager.GetIndexData(db, blob, indexFileName)
		if err != nil {
			return nil, err
		}
		pageFileName, ok := indexMap[recordId]
		if ok {
			recordMap, err := ps.blobDiskManager.GetPageData(db, blob, pageFileName)
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
			err = ps.blobDiskManager.WritePageData(db, blob, pageFileName, recordMap)
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

func (ps partitionStore) UpdateRecordsByPartition(db string, blob string, updateRecord map[string]any, searchPartition map[string]any, filterItems []objects.FilterItem) (map[string]map[string]map[string]any, error) {
	format, err := ps.blobDiskManager.GetFormat(db, blob)
	if err != nil {
		return nil, err
	}
	filter := objects.Filter{FilterItems: filterItems, Format: format}
	err = filter.ConvertFilterItems()
	if err != nil {
		return nil, err
	}
	partitionHashKeyFileNames, err := ps.partitionDiskManager.GetPartitionHashKeyItemFileNames(db, blob)
	if err != nil {
		return nil, err
	}
	partition, err := ps.partitionDiskManager.GetPartition(db, blob)
	if err != nil {
		return nil, err
	}
	partitionHashKeyFileNames, err = ps.filterPartitionFiles(partitionHashKeyFileNames, partition, searchPartition)
	if err != nil {
		return nil, err
	}
	blobObj := objects.CreateBlobWithPartition(blob, format, partition)
	updateRecordFormatted, err := blobObj.FormatUpdateRecord(updateRecord)
	if err != nil {
		return nil, err
	}
	total := make(map[string]map[string]map[string]any)
	for _, partitionHashKeyFileName := range partitionHashKeyFileNames {
		hashKey := strings.Split(partitionHashKeyFileName, ".json")[0]
		partitionItem, err := ps.partitionDiskManager.GetPartitionHashKeyItem(db, blob, hashKey)
		if err != nil {
			return total, err
		}
		var wg sync.WaitGroup
		for i := 0; i < len(partitionItem.FileNames); i += constants.SearchThreadCount {
			var groups [constants.SearchThreadCount]map[string]map[string]any
			threadItem := i
			threadIndex := 0
			for threadItem < len(partitionItem.FileNames) && threadIndex < constants.SearchThreadCount {
				wg.Add(1)
				go ps.blobStore.SearchPageUpdate(db, blob, partitionItem.FileNames[threadItem], filter, &wg, updateRecordFormatted, &groups, threadIndex)
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
				total[partitionItem.FileNames[currentFileIndex]] = groupItem
				currentFileIndex++
			}
		}
	}
	return total, nil
}

func (ps partitionStore) DeleteRecordByIndex(db string, blob string, recordId string) (map[string]map[string]map[string]any, error) {
	indexPrefixMap, err := ps.blobDiskManager.GetPrefixIndexItems(db, blob)
	if err != nil {
		return nil, err
	}
	indexPrefixItem, ok := indexPrefixMap[constants.GetRecordIdPrefix(recordId)]
	if !ok {
		return nil, err
	}
	partition, err := ps.partitionDiskManager.GetPartition(db, blob)
	if err != nil {
		return nil, err
	}
	for _, indexFileName := range indexPrefixItem.FileNames {
		indexMap, err := ps.blobDiskManager.GetIndexData(db, blob, indexFileName)
		if err != nil {
			return nil, err
		}
		pageFileName, ok := indexMap[recordId]
		if ok {
			pageItem := objects.PageItem{FileName: pageFileName}
			recordMap, err := ps.blobDiskManager.GetPageData(db, blob, pageItem.FileName)
			if err != nil {
				return nil, err
			}
			record := recordMap[recordId]

			delete(recordMap, recordId)
			delete(indexMap, recordId)

			if len(recordMap) == 0 {
				partitionKey, err := partition.GetPartitionHashKey(record)
				if err != nil {
					return nil, err
				}
				err = ps.partitionDiskManager.DeletePartitionPageItem(db, blob, partitionKey, pageItem.FileName)
				if err != nil {
					return nil, err
				}
			} else {
				err = ps.blobDiskManager.WritePageData(db, blob, pageItem.FileName, recordMap)
				if err != nil {
					return nil, err
				}
			}

			if len(indexMap) == 0 {
				err = ps.blobDiskManager.DeleteIndexFile(db, blob, indexFileName)
				if err != nil {
					panic(err)
				}
			} else {
				err = ps.blobDiskManager.WriteIndexData(db, blob, indexFileName, indexMap)
				if err != nil {
					panic(err)
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

func (ps partitionStore) DeleteRecordsByPartition(db string, blob string, searchPartition map[string]any, filterItems []objects.FilterItem) (map[string]map[string]map[string]any, error) {
	format, err := ps.blobDiskManager.GetFormat(db, blob)
	if err != nil {
		return nil, err
	}
	filter := objects.Filter{FilterItems: filterItems, Format: format}
	err = filter.ConvertFilterItems()
	if err != nil {
		return nil, err
	}
	partitionHashKeyFileNames, err := ps.partitionDiskManager.GetPartitionHashKeyItemFileNames(db, blob)
	if err != nil {
		return nil, err
	}
	partition, err := ps.partitionDiskManager.GetPartition(db, blob)
	if err != nil {
		return nil, err
	}
	partitionHashKeyFileNames, err = ps.filterPartitionFiles(partitionHashKeyFileNames, partition, searchPartition)
	if err != nil {
		return nil, err
	}
	total := make(map[string]map[string]map[string]any)
	for _, partitionHashKeyFileName := range partitionHashKeyFileNames {
		hashKey := strings.Split(partitionHashKeyFileName, ".json")[0]
		partitionItem, err := ps.partitionDiskManager.GetPartitionHashKeyItem(db, blob, hashKey)
		if err != nil {
			return total, err
		}
		var wg sync.WaitGroup
		for i := 0; i < len(partitionItem.FileNames); i += constants.SearchThreadCount {
			var groups [constants.SearchThreadCount]map[string]map[string]any
			threadItem := i
			threadIndex := 0
			for threadItem < len(partitionItem.FileNames) && threadIndex < constants.SearchThreadCount {
				wg.Add(1)
				go ps.SearchPageDeleteWithHashKey(db, blob, partitionItem.FileNames[threadItem], hashKey, filter, &wg, &groups, threadIndex)
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
				err = ps.blobStore.DeleteIndexes(db, blob, recordIds)
				if err != nil {
					panic(err.Error())
				}
				total[partitionItem.FileNames[currentFileIndex]] = groupItem
				currentFileIndex++
			}
		}
	}
	return total, nil
}

func (ps partitionStore) IsPartition(db string, blob string) bool {
	_, err := ps.partitionDiskManager.GetPartition(db, blob)
	return err == nil
}

func (ps partitionStore) SearchPageDeleteWithHashKey(db string, blob string, fileName string, hashKey string, filter objects.Filter, wg *sync.WaitGroup, groups *[constants.SearchThreadCount]map[string]map[string]any, index int) {
	defer wg.Done()
	pageData, err := ps.blobDiskManager.GetPageData(db, blob, fileName)
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
		err = ps.partitionDiskManager.DeletePartitionPageItem(db, blob, hashKey, fileName)
		if err != nil {
			return
		}
	} else if deletedItems {
		err = ps.blobDiskManager.WritePageData(db, blob, fileName, pageData)
	}
	groups[index] = groupItem
}

func (ps partitionStore) addPartitionedRecords(db string, blob string, hashKey string, insertRecords []map[string]any) (map[string]map[string]map[string]any, error) {
	partitionItem, err := ps.partitionDiskManager.GetPartitionHashKeyItem(db, blob, hashKey)
	if err != nil {
		err = ps.partitionDiskManager.CreatePartitionHashKeyItem(db, blob, hashKey)
		if err != nil {
			return nil, err
		}
	}
	if len(partitionItem.FileNames) == 0 {
		partitionItem, err = ps.partitionDiskManager.CreatePartitionHashKeyFile(db, blob, hashKey)
		if err != nil {
			return nil, err
		}
	}
	currentPageFile := partitionItem.FileNames[len(partitionItem.FileNames)-1]
	recordMap, err := ps.blobDiskManager.GetPageData(db, blob, currentPageFile)
	if err != nil {
		return nil, err
	}
	lastRecordId := ""
	total := make(map[string]map[string]map[string]any)
	total[currentPageFile] = make(map[string]map[string]any)
	indexMap := make(map[string]string)
	for _, insertRecord := range insertRecords {
		lastRecordId = uuid.New().String()
		recordMap[lastRecordId] = insertRecord
		total[currentPageFile][lastRecordId] = insertRecord
		indexMap[lastRecordId] = currentPageFile
		if len(recordMap) > constants.MaxPageSize {
			err = ps.blobDiskManager.WritePageData(db, blob, currentPageFile, recordMap)
			if err != nil {
				delete(total, currentPageFile)
				return total, err
			}
			recordMap = make(map[string]map[string]any)
			partitionItem, err = ps.partitionDiskManager.CreatePartitionHashKeyFile(db, blob, hashKey)
			if err != nil {
				return total, err
			}
			currentPageFile = partitionItem.FileNames[len(partitionItem.FileNames)-1]
			total[currentPageFile] = make(map[string]map[string]any)
		}
	}
	err = ps.blobDiskManager.WritePageData(db, blob, currentPageFile, recordMap)
	if err != nil {
		delete(total, currentPageFile)
		return total, err
	}
	return total, ps.blobStore.AddIndexes(db, blob, indexMap)
}

func (ps partitionStore) filterPartitionFiles(partitionHashKeyFileNames []string, partition objects.Partition, partitionSearch map[string]any) ([]string, error) {
	var foundFiles []string
	for _, partitionHashKeyFileName := range partitionHashKeyFileNames {
		currentChar := 0
		found := true
		for _, partitionKey := range partition.Keys {
			_, ok := partitionSearch[partitionKey]
			if !ok {
				currentChar += 28
				continue
			}
			valueHash, err := partition.GetPartitionHashKeyItem(partitionKey, partitionSearch)
			if err != nil {
				return nil, err
			}
			if partitionHashKeyFileName[currentChar:currentChar+len(valueHash)] != valueHash {
				found = false
				break
			}
			currentChar += 28
		}
		if found {
			foundFiles = append(foundFiles, partitionHashKeyFileName)
		}
	}
	return foundFiles, nil
}
