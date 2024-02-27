package objects

import (
	"errors"
	"fmt"
	"nimy/constants"
	"regexp"
)

type DB interface {
	HasDBNameConvention() error
}

type dbObj struct {
	name string
}

func CreateDB(db string) DB {
	return dbObj{
		name: db,
	}
}

func (d dbObj) HasDBNameConvention() error {
	if len(d.name) > constants.KeyMaxLength {
		return errors.New(fmt.Sprintf("name name length on %s exceeds %d", d.name, constants.DBMaxLength))
	}
	match, _ := regexp.MatchString(constants.DBRegex, d.name)
	if !match {
		return errors.New(fmt.Sprintf("name name %s does not match %s", d.name, constants.DBRegexDesc))
	}
	return nil
}
