package parser

import (
	"errors"
	"fmt"
	"nimy/interfaces/objects"
	"nimy/interfaces/store"
	"nimy/parser/constants"
	"strings"
)

type QueryParams struct {
	Action string         `json:"action"`
	On     string         `json:"on"`
	Name   string         `json:"name"`
	With   map[string]any `json:"with"`
}

type WithDistinguished struct {
	MapStringString   map[string]map[string]string
	ArrayString       map[string][]string
	ArrayMapStringAny map[string][]map[string]any
	MapStringAny      map[string]map[string]any
}

type QueryAnalyser struct {
	dbStore        store.DBStore
	blobStore      store.BlobStore
	partitionStore store.PartitionStore
}

func CreateQueryAnalyser(dbStore store.DBStore, blobStore store.BlobStore, partitionStore store.PartitionStore) QueryAnalyser {
	return QueryAnalyser{
		dbStore:        dbStore,
		blobStore:      blobStore,
		partitionStore: partitionStore,
	}
}

func (qa *QueryAnalyser) Query(queryParams QueryParams) error {
	switch queryParams.Action {
	case constants.TokenCreate:
		return qa.createActions(queryParams)
	case constants.TokenDelete:
		fallthrough
	default:
		return nil
	}
}

func (qa *QueryAnalyser) createActions(queryParams QueryParams) error {
	switch queryParams.On {
	case constants.TokenDB:
		_, err := qa.dbStore.CreateDB(queryParams.Name)
		return err
	case constants.TokenBlob:
		blobParts := strings.Split(queryParams.Name, ".")
		if len(blobParts) != 2 {
			return errors.New("'name' property must match db.blob format")
		}
		withDistinguished, err := qa.convert(queryParams.With)
		if err != nil {
			return err
		}
		formatMap, ok := withDistinguished.MapStringString[constants.TokenFormatObj]
		if !ok {
			return errors.New(fmt.Sprintf("'with' is missing %s data", constants.TokenFormatObj))
		}
		partitionArray, ok := withDistinguished.ArrayString[constants.TokenPartitionObj]
		if !ok {
			_, err = qa.blobStore.CreateBlob(blobParts[0], blobParts[1], qa.buildFormat(formatMap))
			return err
		}
		_, err = qa.partitionStore.CreatePartition(blobParts[0], blobParts[1], qa.buildFormat(formatMap), qa.buildPartition(partitionArray))
		return err
	case constants.TokenRecords:
		blobParts := strings.Split(queryParams.Name, ".")
		if len(blobParts) != 2 {
			return errors.New("'name' property must match db.blob format")
		}
		withDistinguished, err := qa.convert(queryParams.With)
		if err != nil {
			return err
		}
		records, ok := withDistinguished.ArrayMapStringAny[constants.TokenRecordsObj]
		if !ok {
			record, ok := withDistinguished.MapStringAny[constants.TokenRecordObj]
			if !ok {
				return errors.New(fmt.Sprintf("%s or %s not present in with argument", constants.TokenRecordsObj, constants.TokenRecordObj))
			}
			records = []map[string]any{record}
		}
		if qa.partitionStore.IsPartition(blobParts[0], blobParts[1]) {
			_, err = qa.partitionStore.AddRecords(blobParts[0], blobParts[1], records)
			return err
		}
		_, err = qa.blobStore.AddRecords(blobParts[0], blobParts[1], records)
		return err
	default:
		return nil
	}
}

func (qa *QueryAnalyser) deleteActions(queryParams QueryParams) {
	switch queryParams.On {
	case constants.TokenDB:
	case constants.TokenBlob:
	case constants.TokenRecords:
	default:
	}
}

func (qa *QueryAnalyser) buildFormat(formatMap map[string]string) objects.Format {
	format := objects.CreateFormat(make(map[string]objects.FormatItem))
	for key, value := range formatMap {
		format.AddItem(key, objects.FormatItem{KeyType: value})
	}
	return format
}

func (qa *QueryAnalyser) buildPartition(partitionArray []string) objects.Partition {
	return objects.Partition{Keys: partitionArray}
}

func (qa *QueryAnalyser) convert(with map[string]any) (WithDistinguished, error) {
	withDistinguished := WithDistinguished{
		MapStringString:   make(map[string]map[string]string),
		ArrayString:       make(map[string][]string),
		ArrayMapStringAny: make(map[string][]map[string]any),
		MapStringAny:      make(map[string]map[string]any),
	}
	for key, data := range with {
		switch key {
		case constants.TokenRecordObj:
			converted, ok := data.(map[string]any)
			if !ok {
				return WithDistinguished{}, errors.New(fmt.Sprintf("could not convert %s to map[string]any", key))
			}
			withDistinguished.MapStringAny[key] = converted
		case constants.TokenRecordsObj:
			converted, ok := data.([]map[string]any)
			if !ok {
				return WithDistinguished{}, errors.New(fmt.Sprintf("could not convert %s to []map[string]any", key))
			}
			withDistinguished.ArrayMapStringAny[key] = converted
		case constants.TokenFormatObj:
			converted, ok := data.(map[string]string)
			if !ok {
				return WithDistinguished{}, errors.New(fmt.Sprintf("could not convert %s to map[string]string", key))
			}
			withDistinguished.MapStringString[key] = converted
		case constants.TokenPartitionObj:
			converted, ok := data.([]string)
			if !ok {
				return WithDistinguished{}, errors.New(fmt.Sprintf("could not convert %s to []string", key))
			}
			withDistinguished.ArrayString[key] = converted
		}
	}
	return withDistinguished, nil
}
