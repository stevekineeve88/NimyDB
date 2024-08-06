package models

import (
	"github.com/stevekineeve88/nimydb-engine/pkg/query/models"
)

type ProtocolMessage struct {
	Type  string            `json:"type"`
	Query queryModels.Query `json:"query"`
}
