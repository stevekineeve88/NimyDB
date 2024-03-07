package parser

import (
	"errors"
	"fmt"
	"nimy/constants"
	"nimy/interfaces/objects"
	"strings"
)

type CreateTokenParser struct {
	statementParser StatementParser
	rootTokenParser RootTokenParser
}

func CreateCreateTokenParser(statementParser StatementParser, rootTokenParser RootTokenParser) CreateTokenParser {
	return CreateTokenParser{
		statementParser: statementParser,
		rootTokenParser: rootTokenParser,
	}
}

func (p *CreateTokenParser) Parse() error {
	if len(p.statementParser.Tokens) == 0 {
		return errors.New("incorrect syntax: missing create action")
	}
	args := p.statementParser.Tokens[1:len(p.statementParser.Tokens)]
	if len(args) == 0 {
		return errors.New("not enough arguments")
	}
	actionUponToken := p.statementParser.Tokens[0]
	maxArgsMap := map[string]int{
		constants.TokenDB:     1,
		constants.TokenBlob:   3,
		constants.TokenRecord: 2,
	}
	maxArgs, ok := maxArgsMap[actionUponToken]
	if ok && len(args) > maxArgs {
		return errors.New("too many arguments")
	}
	switch actionUponToken {
	case constants.TokenDB:
		return p.runCreateDB(args)
	case constants.TokenBlob:
		return p.runCreateBlob(args)
	case constants.TokenRecord:
		return p.runCreateRecord(args)
	default:
		return errors.New(fmt.Sprintf("invalid token after %s: %s", constants.TokenCreate, p.statementParser.Tokens[0]))
	}
}

/*
 * Arg 0: DB Name
 */
func (p *CreateTokenParser) runCreateDB(args []string) error {
	db := args[0]
	_, err := p.rootTokenParser.dbStore.CreateDB(db)
	return err
}

/*
 * Arg 0: Blob name in format db.blob
 * Arg 1: FORMAT token mapped to object
 * Arg 2: PARTITION token mapped to object (optional)
 */
func (p *CreateTokenParser) runCreateBlob(args []string) error {
	blobLocation := args[0]
	blobParts := strings.Split(blobLocation, ".")
	if len(blobParts) != 2 {
		return errors.New(fmt.Sprintf("could not parse blob %s", blobLocation))
	}
	if 1 >= len(args) || args[1] != constants.TokenFormatObj {
		return errors.New(fmt.Sprintf("missing format directly after %s", blobLocation))
	}
	format := p.statementParser.Objects[constants.TokenFormatObj].(objects.Format)
	if 2 >= len(args) {
		_, err := p.rootTokenParser.blobStore.CreateBlob(blobParts[0], blobParts[1], format)
		return err
	}
	partition := p.statementParser.Objects[constants.TokenPartitionObj].(objects.Partition)
	_, err := p.rootTokenParser.partitionStore.CreatePartition(blobParts[0], blobParts[1], format, partition)
	return err
}

/*
 * Arg 0: Blob name in format db.blob
 * Arg 1: OBJECT token mapped to object
 */
func (p *CreateTokenParser) runCreateRecord(args []string) error {
	blobLocation := args[0]
	blobParts := strings.Split(blobLocation, ".")
	if len(blobParts) != 2 {
		return errors.New(fmt.Sprintf("could not parse blob %s", blobLocation))
	}
	if 1 >= len(args) || args[1] != constants.TokenObjectObj {
		return errors.New(fmt.Sprintf("missing object directly after %s", blobLocation))
	}
	record := p.statementParser.Objects[constants.TokenObjectObj].(map[string]string)
	if p.rootTokenParser.partitionStore.IsPartition(blobParts[0], blobParts[1]) {
		_, err := p.rootTokenParser.partitionStore.AddRecords(blobParts[0], blobParts[1], []map[string]string{record})
		return err
	}
	_, err := p.rootTokenParser.blobStore.AddRecords(blobParts[0], blobParts[1], []map[string]string{record})
	return err
}
