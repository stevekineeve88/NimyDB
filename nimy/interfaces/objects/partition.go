package objects

import (
	"errors"
	"fmt"
)

type Partition struct {
	Keys []string `json:"keys"`
}

type PartitionItem struct {
	FileNames []string `json:"fileNames"`
}

func (p Partition) GetPartitionKey(record map[string]any) (string, error) {
	partitionKey := ""
	for _, key := range p.Keys {
		partitionKeyItem, err := p.getPartitionKeyItem(key, record)
		if err != nil {
			return partitionKey, err
		}
		partitionKey += partitionKeyItem
	}
	return partitionKey, nil
}

func (p Partition) getPartitionKeyItem(partitionKey string, record map[string]any) (string, error) {
	recordItem, ok := record[partitionKey]
	if !ok {
		return "", errors.New(fmt.Sprintf("partition key %s not found in record", partitionKey))
	}
	return fmt.Sprintf("\\k%s\\v%s", partitionKey, recordItem), nil
}
