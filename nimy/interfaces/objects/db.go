package objects

import (
	"errors"
	"fmt"
	"nimy/constants"
	"regexp"
)

type DB struct {
	Name string `json:"name,required"`
}

func CreateDB(db string) DB {
	return DB{
		Name: db,
	}
}

func (d *DB) HasDBNameConvention() error {
	if len(d.Name) > constants.KeyMaxLength {
		return errors.New(fmt.Sprintf("Name length on %s exceeds %d", d.Name, constants.DBMaxLength))
	}
	match, _ := regexp.MatchString(constants.DBRegex, d.Name)
	if !match {
		return errors.New(fmt.Sprintf("Name %s does not match %s", d.Name, constants.DBRegexDesc))
	}
	return nil
}
