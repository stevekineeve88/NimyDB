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
	AddRecords(db string, blob string, insertRecords []map[string]any) (string, error)
	GetRecordsByPartition(db string, blob string, searchPartition map[string]any, filterItems []objects.FilterItem) (map[string]map[string]map[string]any, error)
	UpdateRecordByIndex(db string, blob string, recordId string, updateRecord map[string]any) (map[string]map[string]map[string]any, error)
	UpdateRecordsByPartition(db string, blob string, updateRecord map[string]any, searchPartition map[string]any, filterItems []objects.FilterItem) (map[string]map[string]map[string]any, error)
	UpdateRecords(db string, blob string, updateRecord map[string]any, filterItems []objects.FilterItem) (map[string]map[string]map[string]any, error)
	IsPartition(db string, blob string) bool
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

func (ps partitionStore) AddRecords(db string, blob string, insertRecords []map[string]any) (string, error) {
	format, err := ps.blobDiskManager.GetFormat(db, blob)
	if err != nil {
		return "", err
	}
	partition, err := ps.partitionDiskManager.GetPartition(db, blob)
	if err != nil {
		return "", err
	}

	blobObj := objects.CreateBlobWithPartition(blob, format, partition)
	partitionHashMap := make(map[string][]map[string]any)
	for _, insertRecord := range insertRecords {
		newInsertRecord, err := blobObj.FormatRecord(insertRecord)
		if err != nil {
			return "", err
		}
		partitionHashKey, err := blobObj.GetPartition().GetPartitionHashKey(newInsertRecord)
		if err != nil {
			return "", err
		}
		_, ok := partitionHashMap[partitionHashKey]
		if !ok {
			partitionHashMap[partitionHashKey] = []map[string]any{}
		}
		partitionHashMap[partitionHashKey] = append(partitionHashMap[partitionHashKey], newInsertRecord)
	}

	lastRecordId := ""
	for key, records := range partitionHashMap {
		lastRecordId, err = ps.addPartitionedRecords(db, blob, key, records)
		if err != nil {
			return lastRecordId, err
		}
	}
	return lastRecordId, nil
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

func (ps partitionStore) UpdateRecords(db string, blob string, updateRecord map[string]any, filterItems []objects.FilterItem) (map[string]map[string]map[string]any, error) {
	format, err := ps.blobDiskManager.GetFormat(db, blob)
	if err != nil {
		return nil, err
	}
	partition, err := ps.partitionDiskManager.GetPartition(db, blob)
	if err != nil {
		return nil, err
	}
	filter := objects.Filter{FilterItems: filterItems, Format: format}
	err = filter.ConvertFilterItems()
	if err != nil {
		return nil, err
	}
	pageItems, err := ps.blobDiskManager.GetPageItems(db, blob)
	if err != nil {
		return nil, err
	}
	blobObj := objects.CreateBlobWithPartition(blob, format, partition)
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
			go ps.blobStore.SearchPageUpdate(db, blob, pageItems[threadItem].FileName, filter, &wg, updateRecordFormatted, &groups, threadIndex)
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

func (ps partitionStore) IsPartition(db string, blob string) bool {
	_, err := ps.partitionDiskManager.GetPartition(db, blob)
	return err == nil
}

func (ps partitionStore) addPartitionedRecords(db string, blob string, hashKey string, insertRecords []map[string]any) (string, error) {
	partitionItem, err := ps.partitionDiskManager.GetPartitionHashKeyItem(db, blob, hashKey)
	if err != nil {
		err = ps.partitionDiskManager.CreatePartitionHashKeyItem(db, blob, hashKey)
		if err != nil {
			return "", err
		}
	}
	if len(partitionItem.FileNames) == 0 {
		partitionItem, err = ps.partitionDiskManager.CreatePartitionHashKeyFile(db, blob, hashKey)
		if err != nil {
			return "", err
		}
	}
	currentPageFile := partitionItem.FileNames[len(partitionItem.FileNames)-1]
	recordMap, err := ps.blobDiskManager.GetPageData(db, blob, currentPageFile)
	if err != nil {
		return "", err
	}
	lastRecordId := ""
	indexMap := make(map[string]string)
	for _, insertRecord := range insertRecords {
		lastRecordId = uuid.New().String()
		recordMap[lastRecordId] = insertRecord
		indexMap[lastRecordId] = currentPageFile
		if len(recordMap) > constants.MaxPageSize {
			err = ps.blobDiskManager.WritePageData(db, blob, currentPageFile, recordMap)
			if err != nil {
				return lastRecordId, err
			}
			recordMap = make(map[string]map[string]any)
			partitionItem, err = ps.partitionDiskManager.CreatePartitionHashKeyFile(db, blob, hashKey)
			if err != nil {
				return lastRecordId, err
			}
			currentPageFile = partitionItem.FileNames[len(partitionItem.FileNames)-1]
		}
	}
	err = ps.blobDiskManager.WritePageData(db, blob, currentPageFile, recordMap)
	if err != nil {
		return lastRecordId, err
	}
	return lastRecordId, ps.blobStore.AddIndexes(db, blob, indexMap)
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
