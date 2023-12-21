package rules

import (
	"errors"
	"fmt"
	"nimy/constants"
	"nimy/interfaces/objects"
	"regexp"
	"slices"
	"strconv"
	"time"
)

type BlobRules interface {
	CheckBlob() error
	CheckFormatStructure() error
	FormatRecord(record map[string]any) error
}

type blobRules struct {
	blob   string
	format objects.Format
}

func CreateBlobRules(blob string, format objects.Format) BlobRules {
	return blobRules{
		blob:   blob,
		format: format,
	}
}

func (br blobRules) CheckBlob() error {
	if len(br.blob) > constants.KeyMaxLength {
		return errors.New(fmt.Sprintf("blob name length on %s exceeds %d", br.blob, constants.BlobMaxLength))
	}
	match, _ := regexp.MatchString(constants.BlobRegex, br.blob)
	if !match {
		return errors.New(fmt.Sprintf("blob name %s does not match %s", br.blob, constants.BlobRegexDesc))
	}
	return nil
}

func (br blobRules) CheckFormatStructure() error {
	for key, formatItem := range br.format.GetMap() {
		if len(key) > constants.KeyMaxLength {
			return errors.New(fmt.Sprintf("key length on %s exceeds %d", key, constants.KeyMaxLength))
		}
		match, _ := regexp.MatchString(constants.KeyRegex, key)
		if !match {
			return errors.New(fmt.Sprintf("key %s does not match %s", key, constants.KeyRegexDesc))
		}
		if err := br.checkFormatItem(key, formatItem); err != nil {
			return err
		}
	}
	return nil
}

func (br blobRules) FormatRecord(record map[string]any) error {
	if len(br.format.GetMap()) != len(record) {
		return errors.New("record does not match format length")
	}
	for key, value := range record {
		formatItem, ok := br.format.GetMap()[key]
		if !ok {
			return errors.New(fmt.Sprintf("key %s does not exist in %s", key, br.blob))
		}
		newValue, err := br.convertRecordValue(value.(string), formatItem)
		if err != nil {
			return errors.New(fmt.Sprintf("error on key %s: %s", key, err.Error()))
		}
		record[key] = newValue
	}
	return nil
}

func (br blobRules) checkFormatItem(key string, formatItem objects.FormatItem) error {
	if !slices.Contains(constants.GetFormatTypes(), formatItem.KeyType) {
		return errors.New(fmt.Sprintf("key type %s does not exist on key %s", formatItem.KeyType, key))
	}
	return nil
}

func (br blobRules) convertRecordValue(value string, formatItem objects.FormatItem) (any, error) {
	switch formatItem.KeyType {
	case constants.String:
		return value, nil
	case constants.Int:
		intConv, err := strconv.Atoi(value)
		if err != nil {
			return nil, err
		}
		return intConv, nil
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
	}
	return nil, errors.New("type not handled")
}
