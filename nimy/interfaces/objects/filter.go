package objects

import (
	"errors"
	"fmt"
	"nimy/constants"
	"strings"
	"time"
)

type FilterItem struct {
	Key   string `json:"key,required"`
	Op    string `json:"op,required"`
	Value any    `json:"value,required"`
}

type Filter struct {
	FilterItems []FilterItem
}

func (f *Filter) Passes(record map[string]any, format Format) (bool, error) {
	if f.FilterItems == nil {
		return true, nil
	}
	formatMap := format.GetMap()
	for _, filterItem := range f.FilterItems {
		value, ok := record[filterItem.Key]
		if !ok {
			return false, errors.New(fmt.Sprintf("'%s' not found in record", filterItem.Key))
		}
		result := true
		switch formatMap[filterItem.Key].KeyType {
		case constants.String:
			result = f.checkString(filterItem.Value, value.(string), filterItem.Op)
		case constants.Int:
			//??? No idea why this is needed, but it is
			value, ok = value.(float64)
			if ok {
				value = int(value.(float64))
			} else {
				value, ok = value.(int)
				if !ok {
					return false, nil
				}
			}
			result = f.checkInt(filterItem.Value, value.(int), filterItem.Op)
		case constants.Float:
			result = f.checkFloat(filterItem.Value, value.(float64), filterItem.Op)
		case constants.Date:
			result = f.checkDate(filterItem.Value, value.(string), filterItem.Op)
		case constants.DateTime:
			result = f.checkDateTime(filterItem.Value, value.(string), filterItem.Op)
		default:
			return false, errors.New(fmt.Sprintf("format type %s not known in filter", formatMap[filterItem.Key].KeyType))
		}

		if !result {
			return false, nil
		}
	}
	return true, nil
}

func (f *Filter) checkString(compare any, value string, op string) bool {
	filterValue, ok := compare.(string)
	if !ok {
		return false
	}
	switch op {
	case "CONTAINS":
		return strings.Contains(strings.ToLower(value), strings.ToLower(filterValue))
	case "PREFIX":
		return strings.HasPrefix(strings.ToLower(value), strings.ToLower(filterValue))
	case "SUFFIX":
		return strings.HasSuffix(strings.ToLower(value), strings.ToLower(filterValue))
	case "=":
		return strings.ToLower(value) == strings.ToLower(filterValue)
	default:
		return false
	}
}

func (f *Filter) checkInt(compare any, value int, op string) bool {
	//??? No idea why this is needed, but it is
	compare, ok := compare.(float64)
	if ok {
		compare = int(compare.(float64))
	} else {
		compare, ok = compare.(int)
		if !ok {
			return false
		}
	}
	switch op {
	case "=":
		return compare.(int) == value
	case ">":
		return value > compare.(int)
	case ">=":
		return value >= compare.(int)
	case "<":
		return value < compare.(int)
	case "<=":
		return value <= compare.(int)
	default:
		return false
	}
}

func (f *Filter) checkFloat(compare any, value float64, op string) bool {
	filterValue, ok := compare.(float64)
	if !ok {
		return false
	}
	switch op {
	case "=":
		return filterValue == value
	case ">":
		return value > filterValue
	case ">=":
		return value >= filterValue
	case "<":
		return value < filterValue
	case "<=":
		return value <= filterValue
	default:
		return false
	}
}

func (f *Filter) checkDate(compare any, value string, op string) bool {
	filterValue, ok := compare.(string)
	if !ok {
		return false
	}
	filterValueDate, err := time.Parse("2006-01-02", filterValue)
	if err != nil {
		return false
	}
	valueDate, err := time.Parse("2006-01-02", value)
	if err != nil {
		return false
	}
	switch op {
	case "=":
		return filterValueDate == valueDate
	case ">":
		return valueDate.After(filterValueDate)
	case ">=":
		return valueDate.After(filterValueDate) || filterValueDate == valueDate
	case "<":
		return valueDate.Before(filterValueDate)
	case "<=":
		return valueDate.Before(filterValueDate) || filterValueDate == valueDate
	default:
		return false
	}
}

func (f *Filter) checkDateTime(compare any, value string, op string) bool {
	filterValue, ok := compare.(int64)
	if !ok {
		return false
	}
	filterValueDateTime := time.Unix(filterValue, 0)
	valueDateTime, err := time.Parse(time.DateTime, value)
	if err != nil {
		return false
	}
	switch op {
	case "=":
		return filterValueDateTime == valueDateTime
	case ">":
		return valueDateTime.After(filterValueDateTime)
	case ">=":
		return valueDateTime.After(filterValueDateTime) || filterValueDateTime == valueDateTime
	case "<":
		return valueDateTime.Before(filterValueDateTime)
	case "<=":
		return valueDateTime.Before(filterValueDateTime) || filterValueDateTime == valueDateTime
	default:
		return false
	}
}
