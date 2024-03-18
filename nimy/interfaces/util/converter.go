package util

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

func ConvertToInt(value any) (int, error) {
	switch value.(type) {
	case int:
		return value.(int), nil
	case float64:
		return int(value.(float64)), nil
	case string:
		return strconv.Atoi(value.(string))
	default:
		return -1, errors.New(fmt.Sprintf("cannot convert %+v of type %t to int", value, reflect.TypeOf(value)))
	}
}

func ConvertToFloat64(value any) (float64, error) {
	switch value.(type) {
	case int:
		return float64(value.(int)), nil
	case float64:
		return value.(float64), nil
	case string:
		return strconv.ParseFloat(value.(string), 64)
	default:
		return -1, errors.New(fmt.Sprintf("cannot convert %+v of type %t to float", value, reflect.TypeOf(value)))
	}
}
