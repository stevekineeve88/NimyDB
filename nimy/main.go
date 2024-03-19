package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"nimy/parser"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	dataLocation := "C:\\nimy-data"
	queryAnalyzer := parser.CreateQueryAnalyser(dataLocation)

	fmt.Println("---WELCOME TO NimyDB-----")

	for true {
		input := getInput("Enter Command: ")
		if input == "DONE" {
			break
		}
		queryParams := parser.QueryParams{}
		if input == "SIMULATE MASS PARTITION" {
			queryParams = buildMassPartitionQuery()
		} else {
			err := json.Unmarshal([]byte(input), &queryParams)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
		}
		startTime := time.Now()
		result := queryAnalyzer.Query(queryParams)
		fmt.Printf("query time: %f\n", time.Now().Sub(startTime).Seconds())
		if result.Error {
			fmt.Println(result.ErrorMessage)
		} else {
			fmt.Printf("search size: %d\n", result.SearchSize)
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
		Name:   "app.user_logs_quant",
		With: parser.With{
			Records: records,
		},
	}
}
