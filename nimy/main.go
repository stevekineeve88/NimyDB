package main

import (
	"bufio"
	"fmt"
	"nimy/interfaces/disk"
	"nimy/interfaces/parser"
	"nimy/interfaces/store"
	"os"
	"strings"
)

func main() {
	dataLocation := "C:\\nimy-data"
	//Initialize disk managers and stores
	dbDisk := disk.CreateDBDiskManager(dataLocation)
	blobDisk := disk.CreateBlobDiskManager(dataLocation)
	partitionDisk := disk.CreatePartitionDiskManager(dataLocation, blobDisk)
	blobStore := store.CreateBlobStore(blobDisk)
	partitionStore := store.CreatePartitionStore(partitionDisk, blobDisk, blobStore)
	dbStore := store.CreateDBStore(dbDisk)

	//Initialize parser
	rootParser := parser.RootTokenParser{}
	rootParser.AddDBStore(dbStore)
	rootParser.AddBlobStore(blobStore)
	rootParser.AddPartitionStore(partitionStore)

	fmt.Println("---WELCOME TO NimyDB-----")

	for true {
		input := getInput("Enter Command: ")
		if input == "DONE" {
			break
		}
		statementParser, err := parser.ParseStatement(input)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		rootParser.AddStatementParser(statementParser)
		if err = rootParser.Parse(); err != nil {
			fmt.Println(err.Error())
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
