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
	blobStore := store.CreateBlobStore(blobDisk)
	dbStore := store.CreateDBStore(dbDisk)
	fmt.Println("---WELCOME TO NimyDB-----")
	var currentDb string

	for true {
		input := getInput("Enter Command: ")
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
			useInput := getInput("Enter Db Name: ")
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
			blob := getInput("Enter Blob Name: ")
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
		case "DELETE BLOB":
			if currentDb == "" {
				fmt.Println("Not using a database")
				continue
			}
			blob := getInput("Enter Blob Name: ")
			if err := blobDisk.DeleteBlob(currentDb, blob); err != nil {
				fmt.Println(err.Error())
			}
		case "ADD RECORD":
			if currentDb == "" {
				fmt.Println("Not using a database")
				continue
			}
			blob := getInput("Enter Blob Name: ")
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
			blob := getInput("Enter Blob Name: ")
			recordId := getInput("Enter Record ID: ")
			start := time.Now()
			record, err := blobStore.GetRecord(currentDb, blob, recordId)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
			fmt.Println(time.Now().Sub(start).Seconds())
			fmt.Println(record)
		case "GET RECORD OLD":
			if currentDb == "" {
				fmt.Println("Not using a database")
				continue
			}
			blob := getInput("Enter Blob Name: ")
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
			blob := getInput("Enter Blob Name: ")
			recordId := getInput("Enter Record ID: ")
			err := blobStore.DeleteRecord(currentDb, blob, recordId)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
		case "SIMULATE MASS ADD":
			size, _ := strconv.Atoi(getInput("Size of set: "))
			simulateAddUsers(size, blobStore)
		case "DONE":
			break
		default:
			fmt.Printf("COMMAND NOT FOUND: %s \n", input)
		}
		if input == "DONE" {
			break
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

func getInput(prompt string) string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(prompt)
	text, _ := reader.ReadString('\n')
	text = strings.TrimSpace(text)
	return text
}
