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
	FormatRecord(record map[string]any) error
}

type blobObj struct {
	name   string
	format Format
}

func CreateBlob(blob string, format Format) Blob {
	return blobObj{
		name:   blob,
		format: format,
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

func (b blobObj) FormatRecord(record map[string]any) error {
	if len(b.format.GetMap()) != len(record) {
		return errors.New("record does not match format length")
	}
	for key, value := range record {
		formatItem, ok := b.format.GetMap()[key]
		if !ok {
			return errors.New(fmt.Sprintf("key %s does not exist in %s", key, b.name))
		}
		newValue, err := b.convertRecordValue(value.(string), formatItem)
		if err != nil {
			return errors.New(fmt.Sprintf("error on key %s: %s", key, err.Error()))
		}
		record[key] = newValue
	}
	return nil
}

func (b blobObj) checkFormatItem(key string, formatItem FormatItem) error {
	if !slices.Contains(constants.GetFormatTypes(), formatItem.KeyType) {
		return errors.New(fmt.Sprintf("key type %s does not exist on key %s", formatItem.KeyType, key))
	}
	return nil
}

func (b blobObj) convertRecordValue(value string, formatItem FormatItem) (any, error) {
	switch formatItem.KeyType {
	case constants.String:
		return value, nil
	case constants.Int:
		intConv, err := strconv.Atoi(value)
		if err != nil {
			return nil, err
		}
		return intConv, nil
	case constants.Float:
		floatConv, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, err
		}
		return floatConv, nil
	case constants.Bool:
		if !slices.Contains(constants.GetAcceptedBoolValues(), value) {
			return nil, errors.New(fmt.Sprintf("%s is not an accepted boolean value", value))
		}
		return value == constants.BoolValTrue, nil
	case constants.DateTime:
		intConv, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, err
		}
		return time.Unix(intConv, 0), nil
	case constants.Date:
		intConv, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, err
		}
		timeConv := time.Unix(intConv, 0)
		return timeConv.Format(time.DateOnly), nil
	}
	return nil, errors.New("type not handled")
}
