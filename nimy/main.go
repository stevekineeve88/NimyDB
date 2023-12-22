package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"nimy/interfaces/disk"
	"nimy/interfaces/objects"
	"nimy/interfaces/rules"
	"nimy/interfaces/store"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	dataLocation := "../data"
	dbDisk := disk.CreateDBDiskManager(dataLocation)
	blobDisk := disk.CreateBlobDiskManager(dataLocation)
	blobStore := store.CreateBlobStore(blobDisk)
	fmt.Println("---WELCOME TO NimyDB-----")
	var currentDb string

	for true {
		input := getInput("Enter Command: ")
		switch input {
		case "DELETE DB":
			db := getInput("Enter DB: ")
			if err := dbDisk.Delete(db); err != nil {
				fmt.Println(err.Error())
			}
		case "CREATE DB":
			db := getInput("Enter DB: ")
			if err := dbDisk.Create(db); err != nil {
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
			blobRules := rules.CreateBlobRules(blob, format)
			if err := blobRules.CheckBlob(); err != nil {
				fmt.Println(err.Error())
				continue
			}
			if err := blobRules.CheckFormatStructure(); err != nil {
				fmt.Println(err.Error())
				continue
			}
			if err := blobDisk.Create(currentDb, blob, format); err != nil {
				fmt.Println(err.Error())
			}
		case "DELETE BLOB":
			if currentDb == "" {
				fmt.Println("Not using a database")
				continue
			}
			blob := getInput("Enter Blob Name: ")
			if err := blobDisk.Delete(currentDb, blob); err != nil {
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
			record, err := blobStore.GetRecord(currentDb, blob, recordId)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
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
			simulateAddUsers(blobStore)
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

func simulateAddUsers(bs store.BlobStore) {
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
	count := 1
	size := 30000
	initialRecord := make(map[string]any)
	initialRecord["full_name"] = fmt.Sprintf("%s %s", firstNames[rand.Intn(3)], lastNames[rand.Intn(3)])
	initialRecord["is_deleted"] = strconv.Itoa(rand.Intn(2))
	initialRecord["created"] = strconv.FormatInt(time.Now().Unix(), 10)
	insertRecords := []map[string]any{
		initialRecord,
	}
	for true {
		record := make(map[string]any)
		record["full_name"] = fmt.Sprintf("%s %s", firstNames[rand.Intn(3)], lastNames[rand.Intn(3)])
		record["is_deleted"] = strconv.Itoa(rand.Intn(2))
		record["created"] = strconv.FormatInt(time.Now().Unix(), 10)
		insertRecords = append(insertRecords, record)
		count++
		if count == size {
			break
		}
	}
	fmt.Println(len(insertRecords))
	err := bs.AddRecordsBulk("app", "users", insertRecords)
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
