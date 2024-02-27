package parser

import (
	"errors"
	"fmt"
	"nimy/constants"
	"nimy/interfaces/store"
	"strings"
)

type RootParser struct {
	tokens    []string
	dbStore   store.DBStore
	blobStore store.BlobStore
}

func CreateRootParser(statement string) RootParser {
	statement = strings.Join(strings.Fields(statement), " ")
	statement = strings.TrimSpace(statement)
	return RootParser{
		tokens: strings.Split(statement, " "),
	}
}

func (p *RootParser) AddDBStore(dbStore store.DBStore) {
	p.dbStore = dbStore
}

func (p *RootParser) AddBlobStore(blobStore store.BlobStore) {
	p.blobStore = blobStore
}

func (p *RootParser) Parse() error {
	if len(p.tokens) == 0 {
		return errors.New("empty statement")
	}
	switch p.tokens[0] {
	case constants.ParseCreate:
		parser := CreateCreateParser(p.tokens, 1)
		parser.AddDBStore(p.dbStore)
		return parser.Parse()
	case constants.ParseDelete:
		parser := CreateDeleteParser(p.tokens, 1)
		parser.AddDBStore(p.dbStore)
		parser.AddBlobStore(p.blobStore)
		return parser.Parse()
	default:
		return errors.New(fmt.Sprintf("invalid token: %s", p.tokens[0]))
	}
}
