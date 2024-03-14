package objects

import (
	"errors"
	"fmt"
	"nimy/constants"
	"regexp"
	"slices"
	"strconv"
	"time"
)

type Blob interface {
	HasBlobNameConvention() error
	HasFormatStructure() error
	HasPartitionStructure() error
	FormatRecord(record map[string]any) (map[string]any, error)
	GetPartition() Partition
}

type blobObj struct {
	name      string
	format    Format
	partition Partition
}

func CreateBlob(blob string, format Format) Blob {
	return blobObj{
		name:      blob,
		format:    format,
		partition: Partition{Keys: []string{}},
	}
}

func CreateBlobWithPartition(blob string, format Format, partition Partition) Blob {
	return blobObj{
		name:      blob,
		format:    format,
		partition: partition,
	}
}

func (b blobObj) HasBlobNameConvention() error {
	if len(b.name) > constants.KeyMaxLength {
		return errors.New(fmt.Sprintf("name length on %s exceeds %d", b.name, constants.BlobMaxLength))
	}
	match, _ := regexp.MatchString(constants.BlobRegex, b.name)
	if !match {
		return errors.New(fmt.Sprintf("name %s does not match %s", b.name, constants.BlobRegexDesc))
	}
	return nil
}

func (b blobObj) HasFormatStructure() error {
	for key, formatItem := range b.format.GetMap() {
		if len(key) > constants.KeyMaxLength {
			return errors.New(fmt.Sprintf("key length on %s exceeds %d", key, constants.KeyMaxLength))
		}
		match, _ := regexp.MatchString(constants.KeyRegex, key)
		if !match {
			return errors.New(fmt.Sprintf("key %s does not match %s", key, constants.KeyRegexDesc))
		}
		if err := b.checkFormatItem(key, formatItem); err != nil {
			return err
		}
	}
	return nil
}

func (b blobObj) HasPartitionStructure() error {
	formatMap := b.format.GetMap()
	for _, partitionKey := range b.partition.Keys {
		_, ok := formatMap[partitionKey]
		if !ok {
			return errors.New(fmt.Sprintf("partition key %s not found in format", partitionKey))
		}
	}
	return nil
}

func (b blobObj) FormatRecord(record map[string]any) (map[string]any, error) {
	if len(b.format.GetMap()) != len(record) {
		return nil, errors.New("record does not match format length")
	}
	newRecord := make(map[string]any)
	for key, value := range record {
		formatItem, ok := b.format.GetMap()[key]
		if !ok {
			return nil, errors.New(fmt.Sprintf("key %s does not exist in %s", key, b.name))
		}
		newValue, err := b.convertRecordValue(value, formatItem)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("error on key %s: %s", key, err.Error()))
		}
		newRecord[key] = newValue
	}
	return newRecord, nil
}

func (b blobObj) GetPartition() Partition {
	return b.partition
}

func (b blobObj) checkFormatItem(key string, formatItem FormatItem) error {
	if !slices.Contains(constants.GetFormatTypes(), formatItem.KeyType) {
		return errors.New(fmt.Sprintf("key type %s does not exist on key %s", formatItem.KeyType, key))
	}
	return nil
}

func (b blobObj) convertRecordValue(value any, formatItem FormatItem) (any, error) {
	switch formatItem.KeyType {
	case constants.String:
		converted, ok := value.(string)
		if !ok {
			return nil, errors.New(fmt.Sprintf("%+v could not be converted to string", value))
		}
		return converted, nil
	case constants.Int:
		converted, ok := value.(int)
		if !ok {
			return nil, errors.New(fmt.Sprintf("%+v could not be converted to int", value))
		}
		return converted, nil
	case constants.Float:
		converted, ok := value.(float64)
		if !ok {
			return nil, errors.New(fmt.Sprintf("%+v could not be converted to float", value))
		}
		return converted, nil
	case constants.Bool:
		converted, ok := value.(bool)
		if !ok {
			return nil, errors.New(fmt.Sprintf("%+v could not convert to bool", value))
		}
		return converted, nil
	case constants.Date:
		fallthrough
	case constants.DateTime:
		var timeValue time.Time
		switch value.(type) {
		case float64:
			timeValue = time.Unix(int64(value.(float64)), 0)
		case string:
			intConv, err := strconv.ParseInt(value.(string), 10, 64)
			if err != nil {
				return nil, errors.New(fmt.Sprintf("%+v could not be converted to int (UNIX FORMAT)", value))
			}
			timeValue = time.Unix(intConv, 0)
		case int64:
			timeValue = time.Unix(value.(int64), 0)
		case int:
			timeValue = time.Unix(int64(value.(int)), 0)
		default:
			return nil, errors.New(fmt.Sprintf("%+v cannot be converted to int", value))
		}
		if formatItem.KeyType == constants.Date {
			return timeValue.Format(time.DateOnly), nil
		}
		return timeValue, nil
	}
	return nil, errors.New("type not handled")
}
