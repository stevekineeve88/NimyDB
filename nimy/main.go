package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"nimy/interfaces/disk"
	"nimy/interfaces/objects"
	"nimy/interfaces/store"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	dataLocation := "C:\\nimy-data"
	dbDisk := disk.CreateDBDiskManager(dataLocation)
	blobDisk := disk.CreateBlobDiskManager(dataLocation)
	partitionDisk := disk.CreatePartitionDiskManager(dataLocation, blobDisk)
	blobStore := store.CreateBlobStore(blobDisk)
	partitionStore := store.CreatePartitionStore(partitionDisk, blobDisk, blobStore)
	dbStore := store.CreateDBStore(dbDisk)
	fmt.Println("---WELCOME TO NimyDB-----")
	var currentDb string

	for true {
		input := getInput("Enter Command: ")
		if input == "DONE" {
			break
		}
		/*rootParser := parser.CreateRootParser(input)
		rootParser.AddDBStore(dbStore)
		rootParser.AddBlobStore(blobStore)
		if err := rootParser.Parse(); err != nil {
			fmt.Println(err.Error())
		}*/
		switch input {
		case "DELETE DB":
			db := getInput("Enter DB: ")
			if err := dbStore.DeleteDB(db); err != nil {
				fmt.Println(err.Error())
			}
		case "CREATE DB":
			db := getInput("Enter DB: ")
			if _, err := dbStore.CreateDB(db); err != nil {
				fmt.Println(err.Error())
			}
		case "USE":
			useInput := getInput("Enter Db name: ")
			if !dbDisk.Exists(useInput) {
				fmt.Println("Database does not exist...")
				continue
			}
			currentDb = useInput
			fmt.Printf("Using DB %s \n", currentDb)
		case "CREATE BLOB":
			if currentDb == "" {
				fmt.Println("Not using a database")
				continue
			}
			blob := getInput("Enter Blob name: ")
			format := objects.CreateFormat(nil)
			for true {
				column := getInput("Enter Column name (DONE if finished): ")
				if column == "DONE" {
					break
				}
				colType := getInput("Enter a Column Type: ")
				format.AddItem(column, objects.FormatItem{
					KeyType: colType,
				})
			}
			if _, err := blobStore.CreateBlob(currentDb, blob, format); err != nil {
				fmt.Println(err.Error())
			}
		case "CREATE PARTITION":
			if currentDb == "" {
				fmt.Println("Not using a database")
				continue
			}
			blob := getInput("Enter Blob name: ")
			format := objects.CreateFormat(make(map[string]objects.FormatItem))
			for true {
				column := getInput("Enter Column name (DONE if finished): ")
				if column == "DONE" {
					break
				}
				colType := getInput("Enter a Column Type: ")
				format.AddItem(column, objects.FormatItem{
					KeyType: colType,
				})
			}
			partition := objects.Partition{Keys: []string{}}
			for true {
				column := getInput("Enter partition Column name (DONE if finished): ")
				if column == "DONE" {
					break
				}
				partition.Keys = append(partition.Keys, column)
			}
			if _, err := partitionStore.CreatePartition(currentDb, blob, format, partition); err != nil {
				fmt.Println(err.Error())
			}
		case "DELETE BLOB":
			if currentDb == "" {
				fmt.Println("Not using a database")
				continue
			}
			blob := getInput("Enter Blob name: ")
			if err := blobDisk.DeleteBlob(currentDb, blob); err != nil {
				fmt.Println(err.Error())
			}
		case "ADD RECORD":
			if currentDb == "" {
				fmt.Println("Not using a database")
				continue
			}
			blob := getInput("Enter Blob name: ")
			format, err := blobDisk.GetFormat(currentDb, blob)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
			record := make(map[string]any)
			for key, _ := range format.GetMap() {
				record[key] = getInput(fmt.Sprintf("Enter value for %s: ", key))
			}
			recordId, err := blobStore.AddRecord(currentDb, blob, record)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
			fmt.Printf("record added with ID %s\n", recordId)
		case "GET RECORD":
			if currentDb == "" {
				fmt.Println("Not using a database")
				continue
			}
			blob := getInput("Enter Blob name: ")
			recordId := getInput("Enter Record ID: ")
			start := time.Now()
			record, err := blobStore.GetRecordByIndex(currentDb, blob, recordId)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
			fmt.Println(time.Now().Sub(start).Seconds())
			fmt.Println(record)
		case "GET RECORDS PARTITION":
			if currentDb == "" {
				fmt.Println("Not using a database")
				continue
			}
			blob := getInput("Enter Blob name: ")
			searchPartition := make(map[string]any)
			for true {
				partition := getInput("Enter partition to search (DONE if finished): ")
				if partition == "DONE" {
					break
				}
				value := getInput("Enter value to search: ")
				searchPartition[partition] = value
			}
			recordMap, _ := partitionStore.GetRecordsByPartition(currentDb, blob, searchPartition)
			display(recordMap, 100)

		case "GET RECORD OLD":
			if currentDb == "" {
				fmt.Println("Not using a database")
				continue
			}
			blob := getInput("Enter Blob name: ")
			recordId := getInput("Enter Record ID: ")
			start := time.Now()
			record, err := blobStore.GetRecordFullScan(currentDb, blob, recordId)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
			fmt.Println(time.Now().Sub(start).Seconds())
			fmt.Println(record)
		case "DELETE RECORD":
			if currentDb == "" {
				fmt.Println("Not using a database")
				continue
			}
			blob := getInput("Enter Blob name: ")
			recordId := getInput("Enter Record ID: ")
			err := blobStore.DeleteRecord(currentDb, blob, recordId)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
		case "SIMULATE MASS ADD":
			size, _ := strconv.Atoi(getInput("Size of set: "))
			simulateAddUsers(size, blobStore)
		case "SIMULATE MASS PARTITION":
			simulateAddLogs(partitionStore)
		case "DONE":
			break
		default:
			fmt.Printf("COMMAND NOT FOUND: %s \n", input)
		}
	}
}

func simulateAddUsers(size int, bs store.BlobStore) {
	firstNames := []string{
		"John",
		"Jacob",
		"Jingle",
	}
	lastNames := []string{
		"Johnson",
		"Jameson",
		"Jingle",
	}
	initialRecord := make(map[string]any)
	initialRecord["full_name"] = fmt.Sprintf("%s %s", firstNames[rand.Intn(3)], lastNames[rand.Intn(3)])
	initialRecord["is_deleted"] = strconv.Itoa(rand.Intn(2))
	initialRecord["created"] = strconv.FormatInt(time.Now().Unix(), 10)
	insertRecords := []map[string]any{
		initialRecord,
	}
	for i := 1; i < size; i++ {
		record := make(map[string]any)
		record["full_name"] = fmt.Sprintf("%s %s", firstNames[rand.Intn(3)], lastNames[rand.Intn(3)])
		record["is_deleted"] = strconv.Itoa(rand.Intn(2))
		record["created"] = strconv.FormatInt(time.Now().Unix(), 10)
		insertRecords = append(insertRecords, record)
	}
	fmt.Println(len(insertRecords))
	_, err := bs.AddRecords("app", "users", insertRecords)
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("FINISHED INSERTING!!")
	}
}

func simulateAddLogs(ps store.PartitionStore) {
	categories := []string{
		"A",
		"B",
		"C",
		"D",
		"E",
		"F",
	}
	comments := []string{
		"Had breakfast yum yum",
		"Why hello. Another log here please",
		"Just passing through",
		"N/A",
	}
	var insertRecords []map[string]any
	currentDate := time.Date(2024, 01, 01, 0, 0, 0, 0, time.Local)
	endDate := time.Date(2024, 12, 31, 0, 0, 0, 0, time.Local)
	for currentDate.Unix() < endDate.Unix() {
		for _, category := range categories {
			record := make(map[string]any)
			record["category"] = category
			record["comments"] = comments[rand.Intn(3)]
			record["log_date"] = strconv.FormatInt(currentDate.Unix(), 10)
			insertRecords = append(insertRecords, record)
		}
		currentDate = currentDate.Add(time.Hour * 24)
	}
	fmt.Println(len(insertRecords))
	_, err := ps.AddRecords("app", "user_logs", insertRecords)
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("FINISHED INSERTING!!")
	}
}

func getInput(prompt string) string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(prompt)
	text, _ := reader.ReadString('\n')
	text = strings.TrimSpace(text)
	return text
}

func display(recordMap map[string]map[string]any, size int) {
	count := 1
	for key, value := range recordMap {
		if count > size {
			break
		}
		fmt.Printf("ID: %s, Record: %+v\n", key, value)
		count++
	}
}
