package parser

import (
	"errors"
	"fmt"
	"nimy/parser/constants"
	"strings"
)

type DeleteTokenParser struct {
	statementParser StatementParser
	rootTokenParser RootTokenParser
}

func CreateDeleteTokenParser(statementParser StatementParser, rootTokenParser RootTokenParser) DeleteTokenParser {
	return DeleteTokenParser{
		statementParser: statementParser,
		rootTokenParser: rootTokenParser,
	}
}

func (p *DeleteTokenParser) Parse() error {
	if len(p.statementParser.Tokens) == 0 {
		return errors.New("incorrect syntax: missing delete action")
	}
	args := p.statementParser.Tokens[1:len(p.statementParser.Tokens)]
	if len(args) == 0 {
		return errors.New("not enough arguments")
	}
	actionUponToken := p.statementParser.Tokens[0]
	maxArgsMap := map[string]int{
		constants.TokenDB:      1,
		constants.TokenBlob:    1,
		constants.TokenRecords: 2,
	}
	maxArgs, ok := maxArgsMap[actionUponToken]
	if ok && len(args) > maxArgs {
		return errors.New("too many arguments")
	}
	switch actionUponToken {
	case constants.TokenDB:
		return p.runDeleteDB(args)
	case constants.TokenBlob:
		return p.runDeleteBlob(args)
	case constants.TokenRecords:
		return p.runDeleteRecord(args)
	default:
		return errors.New(fmt.Sprintf("invalid token after %s: %s", constants.TokenDelete, p.statementParser.Tokens[0]))
	}
}

/*
 * Arg 0: DB Name
 */
func (p *DeleteTokenParser) runDeleteDB(args []string) error {
	db := args[0]
	err := p.rootTokenParser.dbStore.DeleteDB(db)
	return err
}

/*
 * Arg 0: Blob name with format db.blob
 */
func (p *DeleteTokenParser) runDeleteBlob(args []string) error {
	blobLocation := args[0]
	blobParts := strings.Split(blobLocation, ".")
	if len(blobParts) != 2 {
		return errors.New(fmt.Sprintf("could not parse blob %s", blobLocation))
	}
	return p.rootTokenParser.blobStore.DeleteBlob(blobParts[0], blobParts[1])
}

/*
 * Arg 0: Blob name with format db.blob
 * Arg 1: OBJECT_ID mapped to object
 */
func (p *DeleteTokenParser) runDeleteRecord(args []string) error {
	blobLocation := args[0]
	blobParts := strings.Split(blobLocation, ".")
	if len(blobParts) != 2 {
		return errors.New(fmt.Sprintf("could not parse blob %s", blobLocation))
	}
	if 1 >= len(args) || args[1] != constants.TokenObjectIDObj {
		return errors.New("missing object id after blob")
	}
	recordId := p.statementParser.Objects[constants.TokenObjectIDObj].(string)
	return p.rootTokenParser.blobStore.DeleteRecord(blobParts[0], blobParts[1], recordId)
}
