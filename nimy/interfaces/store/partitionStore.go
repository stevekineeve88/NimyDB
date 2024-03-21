package store

import (
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
	GetRecordsByPartition(db string, blob string, searchPartition map[string]any, filter objects.Filter) (map[string]map[string]any, error)
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

func (ps partitionStore) GetRecordsByPartition(db string, blob string, searchPartition map[string]any, filter objects.Filter) (map[string]map[string]any, error) {
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
	format, err := ps.blobDiskManager.GetFormat(db, blob)
	if err != nil {
		return nil, err
	}
	recordMap := make(map[string]map[string]any)
	for _, partitionHashKeyFileName := range partitionHashKeyFileNames {
		hashKey := strings.Split(partitionHashKeyFileName, ".json")[0]
		partitionItem, err := ps.partitionDiskManager.GetPartitionHashKeyItem(db, blob, hashKey)
		if err != nil {
			return recordMap, err
		}
		var wg sync.WaitGroup
		for i := 0; i < len(partitionItem.FileNames); i += constants.SearchThreadCount {
			var groups [constants.SearchThreadCount]map[string]map[string]any
			threadItem := i
			threadCount := 0
			for threadItem < len(partitionItem.FileNames) && threadCount < constants.SearchThreadCount {
				wg.Add(1)
				go ps.blobStore.SearchPage(db, blob, partitionItem.FileNames[threadItem], filter, format, &groups, &wg, threadCount)
				threadItem++
				threadCount++
			}
			wg.Wait()
			for _, groupItem := range groups {
				for key, record := range groupItem {
					recordMap[key] = record
				}
			}
		}
	}
	return recordMap, nil
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
