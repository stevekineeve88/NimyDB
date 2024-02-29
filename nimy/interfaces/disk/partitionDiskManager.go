package disk

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"nimy/constants"
	"nimy/interfaces/objects"
	"os"
)

type PartitionDiskManager interface {
	CreatePartition(db string, blob string, format objects.Format, partition objects.Partition) error
	CreatePartitionPage(db string, blob string, hashKey string) (objects.PartitionItem, error)
	CreatePartitionPagesFile(db string, blob string) error
	CreatePartitionsFile(db string, blob string, partition objects.Partition) error
	GetPartitionPageItems(db string, blob string) (map[string]objects.PartitionItem, error)
	GetPartition(db string, blob string) (objects.Partition, error)
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
		pdm.CreatePartitionPagesFile,
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

func (pdm partitionDiskManager) CreatePartitionPage(db string, blob string, hashKey string) (objects.PartitionItem, error) {
	partitionPageItem := objects.PartitionItem{FileNames: []string{}}
	partitionPageItemMap, err := pdm.GetPartitionPageItems(db, blob)
	if err != nil {
		return partitionPageItem, err
	}
	blobDirectory := fmt.Sprintf("%s/%s/%s", pdm.dataLocation, db, blob)
	tempPartitionPageItem, ok := partitionPageItemMap[hashKey]
	if ok {
		partitionPageItem = tempPartitionPageItem
	}
	partitionPageFileName := fmt.Sprintf("partition-%s.json", uuid.New().String())
	pageData, _ := json.MarshalIndent(make(map[string]interface{}), "", " ")
	err = pdm.blobDiskManager.CreateFile(blobDirectory, pageData, partitionPageFileName)
	if err != nil {
		return partitionPageItem, err
	}
	partitionPageItem.FileNames = append(partitionPageItem.FileNames, partitionPageFileName)
	partitionPageItemMap[hashKey] = partitionPageItem
	err = pdm.writePartitionPagesFile(blobDirectory, partitionPageItemMap)
	if err != nil {
		deletePartitionPageError := os.Remove(fmt.Sprintf("%s/%s", blobDirectory, partitionPageFileName))
		if deletePartitionPageError != nil {
			panic(deletePartitionPageError.Error())
		}
		return partitionPageItem, err
	}
	return partitionPageItem, nil
}

func (pdm partitionDiskManager) CreatePartitionsFile(db string, blob string, partition objects.Partition) error {
	partitionData, _ := json.MarshalIndent(partition, "", " ")
	return pdm.blobDiskManager.CreateFile(fmt.Sprintf("%s/%s/%s", pdm.dataLocation, db, blob), partitionData, constants.PartitionsFile)
}

func (pdm partitionDiskManager) CreatePartitionPagesFile(db string, blob string) error {
	pageData, _ := json.MarshalIndent(make(map[string]objects.PartitionItem), "", " ")
	return pdm.blobDiskManager.CreateFile(fmt.Sprintf("%s/%s/%s", pdm.dataLocation, db, blob), pageData, constants.PagesFile)
}

func (pdm partitionDiskManager) GetPartitionPageItems(db string, blob string) (map[string]objects.PartitionItem, error) {
	var partitionPageItemsMap map[string]objects.PartitionItem
	file, err := os.ReadFile(fmt.Sprintf("%s/%s/%s/%s", pdm.dataLocation, db, blob, constants.PagesFile))
	if err != nil {
		return nil, err
	}

	unmarshalError := json.Unmarshal(file, &partitionPageItemsMap)
	return partitionPageItemsMap, unmarshalError
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

func (pdm partitionDiskManager) writePartitionPagesFile(directoryName string, partitionPageItemMap map[string]objects.PartitionItem) error {
	pagesData, _ := json.MarshalIndent(partitionPageItemMap, "", " ")
	return pdm.blobDiskManager.WriteFile(directoryName, pagesData, constants.PagesFile)
}
