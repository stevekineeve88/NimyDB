package parser

import (
	"errors"
	"fmt"
	"nimy/constants"
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
	switch strings.ToUpper(p.statementParser.Tokens[0]) {
	case constants.TokenDB:
		if len(p.statementParser.Tokens) > 2 {
			return errors.New("too many arguments")
		}
		return p.runDeleteDB(args)
	case constants.TokenBlob:
		if len(p.statementParser.Tokens) > 2 {
			return errors.New("too many arguments")
		}
		return p.runDeleteBlob(args)
	default:
		return errors.New(fmt.Sprintf("invalid token after %s: %s", constants.TokenDelete, p.statementParser.Tokens[0]))
	}
}

/*
 * Arg 0: DB Name
 */
func (p *DeleteTokenParser) runDeleteDB(args []string) error {
	if len(args) == 0 {
		return errors.New("not enough arguments")
	}
	db := args[0]
	err := p.rootTokenParser.dbStore.DeleteDB(db)
	return err
}

func (p *DeleteTokenParser) runDeleteBlob(args []string) error {
	if len(args) == 0 {
		return errors.New("not enough arguments")
	}
	blobLocation := args[0]
	blobParts := strings.Split(blobLocation, ".")
	if len(blobParts) != 2 {
		return errors.New(fmt.Sprintf("could not parse blob %s", blobLocation))
	}
	return p.rootTokenParser.blobStore.DeleteBlob(blobParts[0], blobParts[1])
}
