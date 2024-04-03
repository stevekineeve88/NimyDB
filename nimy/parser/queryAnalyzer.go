package parser

import (
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
	Format          map[string]string    `json:"FORMAT,omitempty"`
	Partition       []string             `json:"PARTITION,omitempty"`
	Record          map[string]any       `json:"RECORD,omitempty"`
	Records         []map[string]any     `json:"RECORDS,omitempty"`
	RecordId        string               `json:"RECORD_ID,omitempty"`
	PartitionSearch map[string]any       `json:"PARTITION_SEARCH,omitempty"`
	Filter          []objects.FilterItem `json:"FILTER,omitempty"`
}

type QueryAnalyser struct {
	dbStore        store.DBStore
	blobStore      store.BlobStore
	partitionStore store.PartitionStore
}

type QueryResult struct {
	Records      map[string]map[string]map[string]any `json:"records,omitempty"`
	Blob         objects.Blob                         `json:"blob,omitempty"`
	DB           objects.DB                           `json:"db,omitempty"`
	LastInsertId string                               `json:"last_insert_id,omitempty"`
	Error        bool                                 `json:"error,required"`
	ErrorMessage string                               `json:"error_message,omitempty"`
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

func (qa *QueryAnalyser) Query(queryParams QueryParams) QueryResult {
	switch queryParams.Action {
	case constants.ActionCreate:
		return qa.createActions(queryParams)
	case constants.ActionDelete:
		return qa.deleteActions(queryParams)
	case constants.ActionGet:
		return qa.getActions(queryParams)
	default:
		return QueryResult{Error: true, ErrorMessage: fmt.Sprintf("'action' parameter %s not applicable", queryParams.On)}
	}
}

func (qa *QueryAnalyser) createActions(queryParams QueryParams) QueryResult {
	switch queryParams.On {
	case constants.OnDB:
		db, err := qa.dbStore.CreateDB(queryParams.Name)
		if err != nil {
			return QueryResult{Error: true, ErrorMessage: err.Error()}
		}
		return QueryResult{DB: db, Error: false}
	case constants.OnBlob:
		blobParts := strings.Split(queryParams.Name, ".")
		if len(blobParts) != 2 {
			return QueryResult{Error: true, ErrorMessage: "'name' property must match db.blob format"}
		}
		if queryParams.With.Format == nil {
			return QueryResult{Error: true, ErrorMessage: "'with' is missing FORMAT data"}
		}
		if queryParams.With.Partition == nil {
			blob, err := qa.blobStore.CreateBlob(blobParts[0], blobParts[1], qa.buildFormat(queryParams.With.Format))
			if err != nil {
				return QueryResult{Error: true, ErrorMessage: err.Error()}
			}
			return QueryResult{Blob: blob, Error: false}
		}
		blob, err := qa.partitionStore.CreatePartition(blobParts[0], blobParts[1], qa.buildFormat(queryParams.With.Format), qa.buildPartition(queryParams.With.Partition))
		if err != nil {
			return QueryResult{Error: true, ErrorMessage: err.Error()}
		}
		return QueryResult{Blob: blob, Error: false}
	case constants.OnRecords:
		blobParts := strings.Split(queryParams.Name, ".")
		if len(blobParts) != 2 {
			return QueryResult{Error: true, ErrorMessage: "'name' property must match db.blob format"}
		}
		var records []map[string]any
		if queryParams.With.Records == nil {
			if queryParams.With.Record == nil {
				return QueryResult{Error: true, ErrorMessage: "RECORDS or RECORD not present in 'with' argument"}
			}
			records = []map[string]any{queryParams.With.Record}
		} else {
			records = queryParams.With.Records
		}
		if qa.partitionStore.IsPartition(blobParts[0], blobParts[1]) {
			lastInsertId, err := qa.partitionStore.AddRecords(blobParts[0], blobParts[1], records)
			if err != nil {
				return QueryResult{Error: true, ErrorMessage: err.Error()}
			}
			return QueryResult{LastInsertId: lastInsertId, Error: false}
		}
		lastInsertId, err := qa.blobStore.AddRecords(blobParts[0], blobParts[1], records)
		if err != nil {
			return QueryResult{Error: true, ErrorMessage: err.Error()}
		}
		return QueryResult{LastInsertId: lastInsertId, Error: false}
	default:
		return QueryResult{Error: true, ErrorMessage: fmt.Sprintf("'on' parameter %s not applicable with action", queryParams.On)}
	}
}

func (qa *QueryAnalyser) deleteActions(queryParams QueryParams) QueryResult {
	switch queryParams.On {
	case constants.OnDB:
		err := qa.dbStore.DeleteDB(queryParams.Name)
		if err != nil {
			return QueryResult{Error: true, ErrorMessage: err.Error()}
		}
		return QueryResult{Error: false}
	case constants.OnBlob:
		blobParts := strings.Split(queryParams.Name, ".")
		if len(blobParts) != 2 {
			return QueryResult{Error: true, ErrorMessage: "'name' property must match db.blob format"}
		}
		err := qa.blobStore.DeleteBlob(blobParts[0], blobParts[1])
		if err != nil {
			return QueryResult{Error: true, ErrorMessage: err.Error()}
		}
		return QueryResult{Error: false}
	case constants.OnRecords:
		blobParts := strings.Split(queryParams.Name, ".")
		if len(blobParts) != 2 {
			return QueryResult{Error: true, ErrorMessage: "'name' property must match db.blob format"}
		}
		if err := qa.checkRecordId(queryParams.With.RecordId); err != nil {
			return QueryResult{Error: true, ErrorMessage: fmt.Sprintf("error on RECORD_ID: %s", err.Error())}
		}
		err := qa.blobStore.DeleteRecord(blobParts[0], blobParts[1], queryParams.With.RecordId)
		if err != nil {
			return QueryResult{Error: true, ErrorMessage: err.Error()}
		}
		return QueryResult{Error: false}
	default:
		return QueryResult{Error: true, ErrorMessage: fmt.Sprintf("'on' parameter %s not applicable with action", queryParams.On)}
	}
}

func (qa *QueryAnalyser) getActions(queryParams QueryParams) QueryResult {
	switch queryParams.On {
	case constants.OnRecords:
		blobParts := strings.Split(queryParams.Name, ".")
		if len(blobParts) != 2 {
			return QueryResult{Error: true, ErrorMessage: "'name' property must match db.blob format"}
		}
		if queryParams.With.PartitionSearch != nil {
			records, err := qa.partitionStore.GetRecordsByPartition(blobParts[0], blobParts[1], queryParams.With.PartitionSearch, queryParams.With.Filter)
			if err != nil {
				return QueryResult{Error: true, ErrorMessage: err.Error()}
			}
			return QueryResult{Records: records, Error: false}
		}
		if queryParams.With.RecordId != "" {
			if err := qa.checkRecordId(queryParams.With.RecordId); err == nil {
				record, err := qa.blobStore.GetRecordByIndex(blobParts[0], blobParts[1], queryParams.With.RecordId)
				if err != nil {
					return QueryResult{Error: true, ErrorMessage: err.Error()}
				}
				return QueryResult{Records: record, Error: false}
			} else {
				return QueryResult{Error: true, ErrorMessage: err.Error()}
			}
		}
		records, err := qa.blobStore.GetRecordFullScan(blobParts[0], blobParts[1], queryParams.With.Filter)
		if err != nil {
			return QueryResult{Error: true, ErrorMessage: err.Error()}
		}
		return QueryResult{Records: records, Error: false}
	default:
		return QueryResult{Error: true, ErrorMessage: fmt.Sprintf("'on' parameter %s not applicable with action", queryParams.On)}
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
