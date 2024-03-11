package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"nimy/interfaces/disk"
	"nimy/interfaces/store"
	"nimy/parser"
	"os"
	"strconv"
	"strings"
	"time"
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
	rootParser := parser.CreateQueryAnalyser(dbStore, blobStore, partitionStore)

	fmt.Println("---WELCOME TO NimyDB-----")

	for true {
		input := getInput("Enter Command: ")
		if input == "DONE" {
			break
		}
		rootParser.Query(parser.QueryParams{
			Action: "CREATE",
			On:     "BLOB",
			Name:   "app.test",
			With: map[string]any{
				"FORMAT": map[string]string{
					"full_name": "string",
				},
				"PARTITION": []string{
					"full_name",
				},
			},
		})
	}
}

func getInput(prompt string) string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(prompt)
	text, _ := reader.ReadString('\n')
	text = strings.TrimSpace(text)
	return text
}

func buildMassPartitionQuery() parser.QueryParams {
	currentYear, _ := strconv.Atoi(getInput("Enter start year: "))
	endYear, _ := strconv.Atoi(getInput("Enter end year: "))

	category := []string{
		"A",
		"B",
		"C",
		"D",
		"E",
		"F",
	}
	comments := []string{
		"N/A",
		"A good day today",
		"A bad day today",
	}

	var records []map[string]any
	for currentYear <= endYear {
		currentDate := time.Date(currentYear, 01, 01, 0, 0, 0, 0, time.Local)
		endDate := time.Date(currentYear, 12, 31, 0, 0, 0, 0, time.Local)
		for endDate.After(currentDate) {
			for _, value := range category {
				records = append(records, map[string]any{
					"category": value,
					"log_date": currentDate.Unix(),
					"comments": comments[rand.Intn(len(comments))],
					"rank":     rand.Intn(10),
				})
			}
			currentDate = currentDate.Add(time.Hour * 24)
		}
		currentYear++
	}
	return parser.QueryParams{
		Action: "CREATE",
		On:     "RECORDS",
		Name:   "app.user_logs",
		With: map[string]any{
			"RECORDS": records,
		},
	}
}
