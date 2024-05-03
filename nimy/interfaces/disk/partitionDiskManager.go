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
	CreatePartitionsFileAndDir(db string, blob string, partition objects.Partition) error
	CreatePartitionHashKeyItem(db string, blob string, hashKey string) error
	CreatePartitionHashKeyFile(db string, blob string, hashKey string) (objects.PartitionItem, error)
	GetPartition(db string, blob string) (objects.Partition, error)
	GetPartitionHashKeyItem(db string, blob string, hashKey string) (objects.PartitionItem, error)
	GetPartitionHashKeyItemFileNames(db string, blob string) ([]string, error)
	DeletePartitionPageItem(db string, blob string, hashKey string, fileName string) error
	WritePartitionHashKeyItem(directoryName string, hashKey string, partitionItem objects.PartitionItem) error
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
			return pdm.CreatePartitionsFileAndDir(db, blob, partition)
		},
		pdm.blobDiskManager.CreatePagesFileAndDir,
		pdm.blobDiskManager.CreateIndexesFileAndDir,
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

func (pdm partitionDiskManager) CreatePartitionsFileAndDir(db string, blob string, partition objects.Partition) error {
	partitionData, _ := json.Marshal(partition)
	err := pdm.blobDiskManager.CreateFile(fmt.Sprintf("%s/%s/%s", pdm.dataLocation, db, blob), partitionData, constants.PartitionsFile)
	if err != nil {
		return err
	}
	return os.Mkdir(fmt.Sprintf("%s/%s/%s/%s", pdm.dataLocation, db, blob, constants.PartitionsDir), 0600)
}

func (pdm partitionDiskManager) CreatePartitionHashKeyItem(db string, blob string, hashKey string) error {
	partitionHashKeyItemsData, _ := json.Marshal(objects.PartitionItem{FileNames: []string{}})
	return pdm.blobDiskManager.CreateFile(fmt.Sprintf("%s/%s/%s/%s", pdm.dataLocation, db, blob, constants.PartitionsDir), partitionHashKeyItemsData, hashKey+".json")
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

	partitionFileName := fmt.Sprintf("%s/%s.json", constants.PagesDir, uuid.New().String())
	partitionItem.FileNames = append(partitionItem.FileNames, partitionFileName)
	pagesItems = append(pagesItems, objects.PageItem{FileName: partitionFileName})

	pageData, _ := json.Marshal(make(map[string]interface{}))
	err = pdm.blobDiskManager.CreateFile(blobDirectory, pageData, partitionFileName)
	if err != nil {
		return partitionItem, err
	}
	err = pdm.WritePartitionHashKeyItem(blobDirectory+"/"+constants.PartitionsDir, hashKey, partitionItem)
	if err != nil {
		deletePageError := os.Remove(fmt.Sprintf("%s/%s/%s", blobDirectory, constants.PartitionsDir, partitionFileName))
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
	file, err := os.ReadFile(fmt.Sprintf("%s/%s/%s/%s/%s", pdm.dataLocation, db, blob, constants.PartitionsDir, hashKey+".json"))
	if err != nil {
		return partitionItems, err
	}

	unmarshalError := json.Unmarshal(file, &partitionItems)
	return partitionItems, unmarshalError
}

func (pdm partitionDiskManager) GetPartitionHashKeyItemFileNames(db string, blob string) ([]string, error) {
	files, err := os.ReadDir(fmt.Sprintf("%s/%s/%s/%s", pdm.dataLocation, db, blob, constants.PartitionsDir))
	if err != nil {
		return nil, err
	}
	var partitionHashKeyItemFileNames []string
	for _, file := range files {
		partitionHashKeyItemFileNames = append(partitionHashKeyItemFileNames, file.Name())
	}
	return partitionHashKeyItemFileNames, nil
}

func (pdm partitionDiskManager) DeletePartitionPageItem(db string, blob string, hashKey string, fileName string) error {
	partitionItems, err := pdm.GetPartitionHashKeyItem(db, blob, hashKey)
	if err != nil {
		return err
	}
	for index, pageFileName := range partitionItems.FileNames {
		if pageFileName == fileName {
			copy(partitionItems.FileNames[index:], partitionItems.FileNames[index+1:])
			partitionItems.FileNames[len(partitionItems.FileNames)-1] = ""
			partitionItems.FileNames = partitionItems.FileNames[:len(partitionItems.FileNames)-1]
			err = pdm.WritePartitionHashKeyItem(fmt.Sprintf("%s/%s/%s/%s", pdm.dataLocation, db, blob, constants.PartitionsDir), hashKey, partitionItems)
			if err != nil {
				return err
			}
			break
		}
	}
	return pdm.blobDiskManager.DeletePageItem(db, blob, objects.PageItem{FileName: fileName})
}

func (pdm partitionDiskManager) WritePartitionHashKeyItem(directoryName string, hashKey string, partitionItem objects.PartitionItem) error {
	partitionItemData, _ := json.Marshal(partitionItem)
	return pdm.blobDiskManager.WriteFile(directoryName, partitionItemData, hashKey+".json")
}
