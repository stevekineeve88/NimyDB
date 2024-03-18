package objects

import (
	"errors"
	"fmt"
	"nimy/constants"
	"nimy/interfaces/util"
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
			compare, ok := filterItem.Value.(string)
			if !ok {
				return false, errors.New(fmt.Sprintf("%+v could not be converted to string", filterItem.Value))
			}
			_, ok = value.(string)
			if !ok {
				return false, errors.New(fmt.Sprintf("record is corrupt value %+v", value))
			}
			result = f.checkString(compare, value.(string), filterItem.Op)
		case constants.Int:
			value, err := util.ConvertToInt(value)
			if err != nil {
				return false, errors.New(fmt.Sprintf("corrupt record with value %+v: %s", value, err.Error()))
			}
			compare, err := util.ConvertToInt(value)
			if err != nil {
				return false, errors.New(fmt.Sprintf("could not convert %+v to int in filter", compare))
			}
			result = f.checkInt(compare, value, filterItem.Op)
		case constants.Float:
			value, err := util.ConvertToFloat64(value)
			if err != nil {
				return false, errors.New(fmt.Sprintf("corrupt record with value %+v: %s", value, err.Error()))
			}
			compare, err := util.ConvertToFloat64(filterItem.Value)
			if err != nil {
				return false, errors.New(fmt.Sprintf("could not convert %+v to int in filter", compare))
			}
			result = f.checkFloat(compare, value, filterItem.Op)
		case constants.Date:
			compare, ok := filterItem.Value.(string)
			if !ok {
				return false, errors.New(fmt.Sprintf("%+v could not be converted to string", filterItem.Value))
			}
			_, ok = value.(string)
			if !ok {
				return false, errors.New(fmt.Sprintf("record is corrupt value %+v", value))
			}
			result = f.checkDate(compare, value.(string), filterItem.Op)
		case constants.DateTime:
			_, ok = value.(string)
			if !ok {
				return false, errors.New(fmt.Sprintf("record is corrupt value %+v", value))
			}
			compare, err := util.ConvertToInt(filterItem.Value)
			if err != nil {
				return false, errors.New(fmt.Sprintf("could not convert %+v to int in filter", compare))
			}
			result = f.checkDateTime(int64(compare), value.(string), filterItem.Op)
		default:
			return false, errors.New(fmt.Sprintf("format type %s not known in filter", formatMap[filterItem.Key].KeyType))
		}

		if !result {
			return false, nil
		}
	}
	return true, nil
}

func (f *Filter) checkString(compare string, value string, op string) bool {
	switch op {
	case "CONTAINS":
		return strings.Contains(strings.ToLower(value), strings.ToLower(compare))
	case "PREFIX":
		return strings.HasPrefix(strings.ToLower(value), strings.ToLower(compare))
	case "SUFFIX":
		return strings.HasSuffix(strings.ToLower(value), strings.ToLower(compare))
	case "=":
		return strings.ToLower(value) == strings.ToLower(compare)
	default:
		return false
	}
}

func (f *Filter) checkInt(compare int, value int, op string) bool {
	switch op {
	case "=":
		return compare == value
	case ">":
		return value > compare
	case ">=":
		return value >= compare
	case "<":
		return value < compare
	case "<=":
		return value <= compare
	default:
		return false
	}
}

func (f *Filter) checkFloat(compare float64, value float64, op string) bool {
	switch op {
	case "=":
		return compare == value
	case ">":
		return value > compare
	case ">=":
		return value >= compare
	case "<":
		return value < compare
	case "<=":
		return value <= compare
	default:
		return false
	}
}

func (f *Filter) checkDate(compare string, value string, op string) bool {
	filterValueDate, err := time.Parse("2006-01-02", compare)
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

func (f *Filter) checkDateTime(compare int64, value string, op string) bool {
	filterValueDateTime := time.Unix(compare, 0)
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
