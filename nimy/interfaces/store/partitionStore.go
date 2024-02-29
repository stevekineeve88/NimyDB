package store

import (
	"nimy/interfaces/disk"
	"nimy/interfaces/objects"
)

type PartitionStore interface {
	CreatePartition(db string, blob string, format objects.Format, partition objects.Partition) (objects.Blob, error)
}

type partitionStore struct {
	partitionDiskManager disk.PartitionDiskManager
}

func CreatePartitionStore(partitionDiskManager disk.PartitionDiskManager) PartitionStore {
	return partitionStore{
		partitionDiskManager: partitionDiskManager,
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
