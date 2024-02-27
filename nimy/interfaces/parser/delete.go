package parser

import (
	"errors"
	"fmt"
	"nimy/constants"
	"nimy/interfaces/store"
	"strings"
)

type DeleteParser struct {
	tokens      []string
	cTokenIndex int
	dbStore     store.DBStore
	blobStore   store.BlobStore
}

func CreateDeleteParser(tokens []string, cTokenIndex int) DeleteParser {
	return DeleteParser{
		tokens:      tokens,
		cTokenIndex: cTokenIndex,
	}
}

func (p *DeleteParser) AddDBStore(dbStore store.DBStore) {
	p.dbStore = dbStore
}

func (p *DeleteParser) AddBlobStore(blobStore store.BlobStore) {
	p.blobStore = blobStore
}

func (p *DeleteParser) Parse() error {
	if p.cTokenIndex >= len(p.tokens) {
		return errors.New("incorrect syntax: missing delete action")
	}
	switch strings.ToUpper(p.tokens[p.cTokenIndex]) {
	case constants.ParseDB:
		if p.cTokenIndex+1 >= len(p.tokens) {
			return errors.New("missing database")
		}
		db := p.tokens[p.cTokenIndex+1]
		return p.dbStore.DeleteDB(db)
	case constants.ParseBlob:
		if p.cTokenIndex+1 >= len(p.tokens) {
			return errors.New("missing blob")
		}
		blobLocation := p.tokens[p.cTokenIndex+1]
		blobParts := strings.Split(blobLocation, ".")
		if len(blobParts) != 2 {
			return errors.New(fmt.Sprintf("could not parse blob %s", blobLocation))
		}
		return p.blobStore.DeleteBlob(blobParts[0], blobParts[1])
	default:
		return errors.New(fmt.Sprintf("invalid token after %s: %s", p.tokens[p.cTokenIndex-1], p.tokens[p.cTokenIndex]))
	}
}
