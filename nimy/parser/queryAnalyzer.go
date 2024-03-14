package parser

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"nimy/interfaces/disk"
	"nimy/interfaces/objects"
	"nimy/interfaces/store"
	"nimy/parser/constants"
	"strings"
)

type QueryParams struct {
	Action string `json:"action,required"`
	On     string `json:"on,required"`
	Name   string `json:"name,required"`
	With   With   `json:"with"`
}

type With struct {
	Format    map[string]string `json:"FORMAT,omitempty"`
	Partition []string          `json:"PARTITION,omitempty"`
	Record    map[string]any    `json:"RECORD,omitempty"`
	Records   []map[string]any  `json:"RECORDS,omitempty"`
	RecordId  string            `json:"RECORD_ID,omitempty"`
}

type QueryAnalyser struct {
	dbStore        store.DBStore
	blobStore      store.BlobStore
	partitionStore store.PartitionStore
}

func CreateQueryAnalyser(dataLocation string) QueryAnalyser {
	dbDisk := disk.CreateDBDiskManager(dataLocation)
	blobDisk := disk.CreateBlobDiskManager(dataLocation)
	partitionDisk := disk.CreatePartitionDiskManager(dataLocation, blobDisk)

	blobStore := store.CreateBlobStore(blobDisk)
	partitionStore := store.CreatePartitionStore(partitionDisk, blobDisk, blobStore)
	dbStore := store.CreateDBStore(dbDisk)

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
		return qa.deleteActions(queryParams)
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
		if queryParams.With.Format == nil {
			return errors.New(fmt.Sprintf("'With' is missing %s data", constants.TokenFormatObj))
		}
		if queryParams.With.Partition == nil {
			_, err := qa.blobStore.CreateBlob(blobParts[0], blobParts[1], qa.buildFormat(queryParams.With.Format))
			return err
		}
		_, err := qa.partitionStore.CreatePartition(blobParts[0], blobParts[1], qa.buildFormat(queryParams.With.Format), qa.buildPartition(queryParams.With.Partition))
		return err
	case constants.TokenRecords:
		blobParts := strings.Split(queryParams.Name, ".")
		if len(blobParts) != 2 {
			return errors.New("'name' property must match db.blob format")
		}
		var records []map[string]any
		if queryParams.With.Records == nil {
			if queryParams.With.Record == nil {
				return errors.New(fmt.Sprintf("%s or %s not present in 'with' argument", constants.TokenRecordsObj, constants.TokenRecordObj))
			}
			records = []map[string]any{queryParams.With.Record}
		} else {
			records = queryParams.With.Records
		}
		if qa.partitionStore.IsPartition(blobParts[0], blobParts[1]) {
			_, err := qa.partitionStore.AddRecords(blobParts[0], blobParts[1], records)
			return err
		}
		_, err := qa.blobStore.AddRecords(blobParts[0], blobParts[1], records)
		return err
	default:
		return errors.New(fmt.Sprintf("'on' parameter %s not applicable With action", queryParams.On))
	}
}

func (qa *QueryAnalyser) deleteActions(queryParams QueryParams) error {
	switch queryParams.On {
	case constants.TokenDB:
		return qa.dbStore.DeleteDB(queryParams.Name)
	case constants.TokenBlob:
		blobParts := strings.Split(queryParams.Name, ".")
		if len(blobParts) != 2 {
			return errors.New("'name' property must match db.blob format")
		}
		return qa.blobStore.DeleteBlob(blobParts[0], blobParts[1])
	case constants.TokenRecords:
		blobParts := strings.Split(queryParams.Name, ".")
		if len(blobParts) != 2 {
			return errors.New("'name' property must match db.blob format")
		}
		if err := qa.checkRecordId(queryParams.With.RecordId); err != nil {
			return errors.New(fmt.Sprintf("error on %s: %s", constants.TokenRecordIDObj, err.Error()))
		}
		return qa.blobStore.DeleteRecord(blobParts[0], blobParts[1], queryParams.With.RecordId)
	default:
		return errors.New(fmt.Sprintf("'on' parameter %s not applicable With action", queryParams.On))
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

func (qa *QueryAnalyser) checkRecordId(recordId string) error {
	_, err := uuid.Parse(recordId)
	return err
}
