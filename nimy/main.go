package main

import (
	"bufio"
	"fmt"
	"nimy/interfaces/disk"
	"nimy/interfaces/objects"
	"nimy/interfaces/rules"
	"os"
	"strings"
)

func main() {
	dataLocation := "../data"
	dbDisk := disk.CreateDBDiskManager(dataLocation)
	blobDisk := disk.CreateBlobDiskManager(dataLocation)
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
			if err := blobRules.CheckFormat(); err != nil {
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

func getInput(prompt string) string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(prompt)
	text, _ := reader.ReadString('\n')
	text = strings.TrimSpace(text)
	return text
}
