package parser

import (
	"errors"
	"fmt"
	"nimy/constants"
	"nimy/interfaces/store"
	"strings"
)

type CreateParser struct {
	tokens      []string
	cTokenIndex int
	dbStore     store.DBStore
}

func CreateCreateParser(tokens []string, cTokenIndex int) CreateParser {
	return CreateParser{
		tokens:      tokens,
		cTokenIndex: cTokenIndex,
	}
}

func (p *CreateParser) AddDBStore(dbStore store.DBStore) {
	p.dbStore = dbStore
}

func (p *CreateParser) Parse() error {
	if p.cTokenIndex >= len(p.tokens) {
		return errors.New("incorrect syntax: missing create action")
	}
	switch strings.ToUpper(p.tokens[p.cTokenIndex]) {
	case constants.ParseDB:
		if p.cTokenIndex+1 >= len(p.tokens) {
			return errors.New("missing database")
		}
		db := p.tokens[p.cTokenIndex+1]
		_, err := p.dbStore.CreateDB(db)
		return err
	default:
		return errors.New(fmt.Sprintf("invalid token after %s: %s", p.tokens[p.cTokenIndex-1], p.tokens[p.cTokenIndex]))
	}
}
