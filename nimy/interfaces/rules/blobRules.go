package rules

import (
	"errors"
	"fmt"
	"nimy/constants"
	"nimy/interfaces/objects"
	"regexp"
	"slices"
)

type BlobRules interface {
	CheckBlob() error
	CheckFormat() error
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

func (br blobRules) CheckFormat() error {
	for key, formatItem := range br.format.GetMap() {
		if err := br.checkKey(key); err != nil {
			return err
		}
		if err := br.checkFormatItem(key, formatItem); err != nil {
			return err
		}
	}
	return nil
}

func (br blobRules) checkFormatItem(key string, formatItem objects.FormatItem) error {
	if !slices.Contains(constants.GetFormatTypes(), formatItem.KeyType) {
		return errors.New(fmt.Sprintf("key type %s does not exist on key %s", formatItem.KeyType, key))
	}
	return nil
}

func (br blobRules) checkKey(key string) error {
	if len(key) > constants.KeyMaxLength {
		return errors.New(fmt.Sprintf("key length on %s exceeds %d", key, constants.KeyMaxLength))
	}
	match, _ := regexp.MatchString(constants.KeyRegex, key)
	if !match {
		return errors.New(fmt.Sprintf("key %s does not match %s", key, constants.KeyRegexDesc))
	}
	return nil
}
