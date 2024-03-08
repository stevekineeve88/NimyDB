package parser

import (
	"errors"
	"fmt"
	"nimy/interfaces/store"
	"nimy/parser/constants"
)

type RootTokenParser struct {
	statementParser StatementParser
	dbStore         store.DBStore
	blobStore       store.BlobStore
	partitionStore  store.PartitionStore
}

func (p *RootTokenParser) AddStatementParser(statementParser StatementParser) {
	p.statementParser = statementParser
}

func (p *RootTokenParser) AddDBStore(dbStore store.DBStore) {
	p.dbStore = dbStore
}

func (p *RootTokenParser) AddBlobStore(blobStore store.BlobStore) {
	p.blobStore = blobStore
}

func (p *RootTokenParser) AddPartitionStore(partitionStore store.PartitionStore) {
	p.partitionStore = partitionStore
}

func (p *RootTokenParser) Parse() error {
	if len(p.statementParser.Tokens) == 0 {
		return errors.New("empty statement")
	}
	newStatementParser := p.statementParser
	newStatementParser.Tokens = newStatementParser.Tokens[1:len(newStatementParser.Tokens)]
	switch p.statementParser.Tokens[0] {
	case constants.TokenCreate:
		parser := CreateCreateTokenParser(newStatementParser, *p)
		return parser.Parse()
	case constants.TokenDelete:
		parser := CreateDeleteTokenParser(newStatementParser, *p)
		return parser.Parse()
	default:
		return errors.New(fmt.Sprintf("invalid token: %s", p.statementParser.Tokens[0]))
	}
}
