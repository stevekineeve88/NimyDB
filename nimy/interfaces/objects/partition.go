package objects

import (
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
)

type Partition struct {
	Keys []string `json:"keys"`
}

type PartitionItem struct {
	FileNames []string `json:"fileNames"`
}

func (p Partition) GetPartitionHashKey(record map[string]any) (string, error) {
	partitionKey := ""
	for _, key := range p.Keys {
		partitionKeyItem, err := p.GetPartitionHashKeyItem(key, record)
		if err != nil {
			return partitionKey, err
		}
		partitionKey += partitionKeyItem
	}
	return partitionKey, nil
}

func (p Partition) GetPartitionHashKeyItem(partitionKey string, record map[string]any) (string, error) {
	recordItem, ok := record[partitionKey]
	if !ok {
		return "", errors.New(fmt.Sprintf("partition key %s not found in record", partitionKey))
	}
	hasher := sha1.New()
	hasher.Write([]byte(fmt.Sprintf("%+v", recordItem)))
	return base64.URLEncoding.EncodeToString(hasher.Sum(nil)), nil
}
