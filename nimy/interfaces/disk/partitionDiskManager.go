package disk

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"nimy/constants"
	"nimy/interfaces/objects"
	"os"
	"strings"
)

type PartitionDiskManager interface {
	CreatePartition(db string, blob string, format objects.Format, partition objects.Partition) error
	CreatePartitionsFile(db string, blob string, partition objects.Partition) error
	CreatePartitionHashKeyItem(db string, blob string, hashKey string) error
	CreatePartitionHashKeyFile(db string, blob string, hashKey string) (objects.PartitionItem, error)
	GetPartition(db string, blob string) (objects.Partition, error)
	GetPartitionHashKeyItem(db string, blob string, hashKey string) (objects.PartitionItem, error)
	GetPartitionHashKeyItemFileNames(db string, blob string) ([]string, error)
}

type partitionDiskManager struct {
	dataLocation    string
	blobDiskManager BlobDiskManager
}

func CreatePartitionDiskManager(dataLocation string, blobDiskManager BlobDiskManager) PartitionDiskManager {
	return partitionDiskManager{
		dataLocation:    dataLocation,
		blobDiskManager: blobDiskManager,
	}
}

func (pdm partitionDiskManager) CreatePartition(db string, blob string, format objects.Format, partition objects.Partition) error {
	err := os.Mkdir(fmt.Sprintf("%s/%s/%s", pdm.dataLocation, db, blob), 0777)
	if err != nil {
		return err
	}

	fileCreations := []func(db string, blob string) error{
		func(db string, blob string) error {
			return pdm.blobDiskManager.CreateFormatFile(db, blob, format)
		},
		func(db string, blob string) error {
			return pdm.CreatePartitionsFile(db, blob, partition)
		},
		pdm.blobDiskManager.CreatePagesFile,
		pdm.blobDiskManager.CreateIndexesFile,
	}

	for _, fileCreation := range fileCreations {
		err = fileCreation(db, blob)
		if err != nil {
			deleteBlobError := pdm.blobDiskManager.DeleteBlob(db, blob)
			if deleteBlobError != nil {
				panic(deleteBlobError.Error())
			}
			return err
		}
	}

	return nil
}

func (pdm partitionDiskManager) CreatePartitionsFile(db string, blob string, partition objects.Partition) error {
	partitionData, _ := json.MarshalIndent(partition, "", " ")
	return pdm.blobDiskManager.CreateFile(fmt.Sprintf("%s/%s/%s", pdm.dataLocation, db, blob), partitionData, constants.PartitionsFile)
}

func (pdm partitionDiskManager) CreatePartitionHashKeyItem(db string, blob string, hashKey string) error {
	partitionHashKeyItemsData, _ := json.MarshalIndent(objects.PartitionItem{FileNames: []string{}}, "", " ")
	return pdm.blobDiskManager.CreateFile(fmt.Sprintf("%s/%s/%s", pdm.dataLocation, db, blob), partitionHashKeyItemsData, hashKey+".json")
}

func (pdm partitionDiskManager) CreatePartitionHashKeyFile(db string, blob string, hashKey string) (objects.PartitionItem, error) {
	partitionItem := objects.PartitionItem{FileNames: []string{}}
	partitionItem, err := pdm.GetPartitionHashKeyItem(db, blob, hashKey)
	if err != nil {
		return partitionItem, err
	}
	pagesItems, err := pdm.blobDiskManager.GetPageItems(db, blob)
	if err != nil {
		return partitionItem, err
	}
	blobDirectory := fmt.Sprintf("%s/%s/%s", pdm.dataLocation, db, blob)

	partitionFileName := fmt.Sprintf("page-%s.json", uuid.New().String())
	partitionItem.FileNames = append(partitionItem.FileNames, partitionFileName)
	pagesItems = append(pagesItems, objects.PageItem{FileName: partitionFileName})

	pageData, _ := json.MarshalIndent(make(map[string]interface{}), "", " ")
	err = pdm.blobDiskManager.CreateFile(blobDirectory, pageData, partitionFileName)
	if err != nil {
		return partitionItem, err
	}
	err = pdm.WritePartitionHashKeyItem(blobDirectory, hashKey, partitionItem)
	if err != nil {
		deletePageError := os.Remove(fmt.Sprintf("%s/%s", blobDirectory, partitionFileName))
		if deletePageError != nil {
			panic(deletePageError.Error())
		}
		return partitionItem, err
	}
	return partitionItem, pdm.blobDiskManager.WritePagesFile(blobDirectory, pagesItems)
}

func (pdm partitionDiskManager) GetPartition(db string, blob string) (objects.Partition, error) {
	var partition objects.Partition
	file, err := os.ReadFile(fmt.Sprintf("%s/%s/%s/%s", pdm.dataLocation, db, blob, constants.PartitionsFile))
	if err != nil {
		return partition, err
	}

	unmarshalError := json.Unmarshal(file, &partition)
	return partition, unmarshalError
}

func (pdm partitionDiskManager) GetPartitionHashKeyItem(db string, blob string, hashKey string) (objects.PartitionItem, error) {
	var partitionItems objects.PartitionItem
	file, err := os.ReadFile(fmt.Sprintf("%s/%s/%s/%s", pdm.dataLocation, db, blob, hashKey+".json"))
	if err != nil {
		return partitionItems, err
	}

	unmarshalError := json.Unmarshal(file, &partitionItems)
	return partitionItems, unmarshalError
}

func (pdm partitionDiskManager) GetPartitionHashKeyItemFileNames(db string, blob string) ([]string, error) {
	files, err := os.ReadDir(fmt.Sprintf("%s/%s/%s", pdm.dataLocation, db, blob))
	if err != nil {
		return nil, err
	}
	staticFiles := []string{
		constants.FormatFile,
		constants.IndexesFile,
		constants.PartitionsFile,
		constants.PagesFile,
	}
	staticPrefixes := []string{
		"index-",
		"page-",
	}
	var partitionHashKeyItemFileNames []string
	for _, file := range files {
		isPartitionFile := true
		for _, staticFile := range staticFiles {
			if staticFile == file.Name() {
				isPartitionFile = false
				break
			}
		}
		if !isPartitionFile {
			continue
		}
		for _, staticPrefix := range staticPrefixes {
			if strings.HasPrefix(file.Name(), staticPrefix) {
				isPartitionFile = false
				break
			}
		}
		if isPartitionFile {
			partitionHashKeyItemFileNames = append(partitionHashKeyItemFileNames, file.Name())
		}
	}
	return partitionHashKeyItemFileNames, nil
}

func (pdm partitionDiskManager) WritePartitionHashKeyItem(directoryName string, hashKey string, partitionItem objects.PartitionItem) error {
	partitionItemData, _ := json.MarshalIndent(partitionItem, "", " ")
	return pdm.blobDiskManager.WriteFile(directoryName, partitionItemData, hashKey+".json")
}
