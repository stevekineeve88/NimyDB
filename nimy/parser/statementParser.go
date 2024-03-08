package parser

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"nimy/interfaces/objects"
	"nimy/parser/constants"
	"strconv"
	"strings"
	"time"
)

type StatementParser struct {
	Tokens  []string
	Objects map[string]interface{}
}

func ParseStatement(statement string) (StatementParser, error) {
	var tokens []string
	tokenObjects := make(map[string]interface{})
	currentToken := ""
	index := 0
	for index < len(statement) {
		currentChar := statement[index : index+1]
		switch currentChar {
		case " ":
			if currentToken != "" {
				tokens = append(tokens, currentToken)
			}
			currentToken = ""
		case "[":
			fallthrough
		case "(":
			fallthrough
		case "{":
			_, ok := tokenObjects[currentToken]
			if ok {
				return StatementParser{}, errors.New(fmt.Sprintf("duplicate instance of object: %s", currentToken))
			}
			eoeMap := map[string]string{
				"{": "}",
				"[": "]",
				"(": ")",
			}
			index++
			newIndex, element, hitEnd := parseElement(index, statement, eoeMap[currentChar])
			if !hitEnd {
				return StatementParser{}, errors.New(fmt.Sprintf("missing ending character: %s", eoeMap[currentChar]))
			}
			index = newIndex
			object, err := parseObject(currentToken, element)
			if err != nil {
				return StatementParser{}, err
			}
			tokenObjects[currentToken] = object
			tokens = append(tokens, currentToken)
			currentToken = ""
		default:
			currentToken += currentChar
		}
		index++
	}
	if currentToken != "" {
		tokens = append(tokens, currentToken)
	}
	return StatementParser{
		Tokens:  tokens,
		Objects: tokenObjects,
	}, nil
}

func parseElement(index int, statement string, eoe string) (int, string, bool) {
	element := ""
	for index < len(statement) {
		if statement[index:index+1] == eoe {
			return index, element, true
		}
		element += statement[index : index+1]
		index++
	}

	return index, element, false
}

func parseObject(objectType string, element string) (interface{}, error) {
	switch objectType {
	case constants.TokenFormatObj:
		formatMap, err := parseMap(element)
		if err != nil {
			return nil, err
		}
		return buildFormat(formatMap), nil
	case constants.TokenPartitionObj:
		partitionArray, err := parseArray(element)
		if err != nil {
			return nil, err
		}
		return objects.Partition{Keys: partitionArray}, nil
	case constants.TokenObjectObj:
		return parseMap(element)
	case constants.TokenObjectIDObj:
		_, err := uuid.Parse(element)
		return element, err
	case constants.TokenObjectsObj:
		return parseMapList(element)
	default:
		return nil, errors.New(fmt.Sprintf("object type %s does not exist", objectType))
	}
}

func parseMap(element string) (map[string]string, error) {
	parsedMap := make(map[string]string)
	index := 0
	currentKey := ""
	currentToken := ""
	parsingValue := false
	for index < len(element) {
		currentChar := element[index : index+1]
		switch currentChar {
		case " ":
			if !parsingValue && currentToken != "" {
				return nil, errors.New("no spaces allowed in keys")
			}
			if parsingValue {
				currentToken += currentChar
			}
		case ":":
			if currentKey != "" || currentToken == "" {
				return nil, errors.New(fmt.Sprintf("missing , after key %s", currentKey))
			}
			currentKey = currentToken
			currentToken = ""
			parsingValue = true
		case ",":
			if currentKey == "" || currentToken == "" {
				return nil, errors.New("key and value not set properly")
			}
			parsedMap[currentKey] = checkKeyWord(strings.TrimSpace(currentToken))
			currentKey = ""
			currentToken = ""
			parsingValue = false
		case "{":
			return nil, errors.New("object collision detected. possibly missing }")
		default:
			currentToken += currentChar
		}
		index++
	}
	if currentKey == "" || currentToken == "" {
		return nil, errors.New("key and value not set properly")
	}
	parsedMap[currentKey] = checkKeyWord(strings.TrimSpace(currentToken))
	return parsedMap, nil
}

func parseArray(element string) ([]string, error) {
	var arrayElements []string
	index := 0
	currentToken := ""
	for index < len(element) {
		currentChar := element[index : index+1]
		switch currentChar {
		case " ":
			index++
			continue
		case ",":
			if currentToken == "" {
				return nil, errors.New("no proceeding element found")
			}
			arrayElements = append(arrayElements, currentToken)
			currentToken = ""
		default:
			currentToken += currentChar
		}
		index++
	}
	if currentToken == "" {
		return nil, errors.New("no elements set")
	}
	arrayElements = append(arrayElements, currentToken)
	return arrayElements, nil
}

func parseMapList(element string) ([]map[string]string, error) {
	var arrayMaps []map[string]string
	var currentMap map[string]string
	searchingMapStart := true
	index := 0
	for index < len(element) {
		currentChar := element[index : index+1]
		switch currentChar {
		case "{":
			if !searchingMapStart {
				return nil, errors.New("missing , in object list")
			}
			index++
			newIndex, mapElement, hitEnd := parseElement(index, element, "}")
			if !hitEnd {
				return nil, errors.New("missing ending character: }")
			}
			parsedMap, err := parseMap(mapElement)
			if err != nil {
				return nil, err
			}
			currentMap = parsedMap
			index = newIndex
			searchingMapStart = false
		case ",":
			if currentMap == nil {
				return nil, errors.New("misplaced , in object list")
			}
			arrayMaps = append(arrayMaps, currentMap)
			currentMap = nil
			searchingMapStart = true
		case " ":
			index++
			continue
		default:
			return nil, errors.New(fmt.Sprintf("msiplaced character %s in objecct list", currentChar))
		}
		index++
	}

	if currentMap != nil {
		arrayMaps = append(arrayMaps, currentMap)
	}

	return arrayMaps, nil
}

func checkKeyWord(value string) string {
	switch value {
	case "TODAY":
		return strconv.FormatInt(time.Now().Unix(), 10)
	default:
		return value
	}
}

func buildFormat(formatMap map[string]string) objects.Format {
	format := objects.CreateFormat(make(map[string]objects.FormatItem))
	for key, value := range formatMap {
		format.AddItem(key, objects.FormatItem{KeyType: value})
	}
	return format
}
