package handlers

import (
	"github.com/stevekineeve88/nimydb-engine/pkg/query/constants"
	"github.com/stevekineeve88/nimydb-engine/pkg/query/managers"
	"github.com/stevekineeve88/nimydb-engine/pkg/query/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/system/managers"
	"nimy/interface/constants"
	"nimy/interface/models"
)

type ProtocolHandler interface {
	Handle(message models.ProtocolMessage) queryModels.QueryResult
}

type protocolHandler struct {
	queryManager       queryManagers.QueryManager
	systemQueryManager queryManagers.QueryManager
	logManager         systemManagers.LogManager
}

func CreateProtocolHandler(
	queryManager queryManagers.QueryManager,
	systemQueryManager queryManagers.QueryManager,
	logManager systemManagers.LogManager,
) ProtocolHandler {
	return &protocolHandler{
		queryManager:       queryManager,
		systemQueryManager: systemQueryManager,
		logManager:         logManager,
	}
}

func (handler *protocolHandler) Handle(message models.ProtocolMessage) queryModels.QueryResult {
	switch message.Type {
	case constants.ProtocolTypeSystem:
		return handler.systemQueryManager.Query(message.Query)
	default:
		result := handler.queryManager.Query(message.Query)
		if result.ErrorMessage == "" {
			handler._addSystemLog(result, message.Query)
		}
		return result
	}
}

func (handler *protocolHandler) _addSystemLog(result queryModels.QueryResult, query queryModels.Query) {
	switch query.Action {
	case queryConstants.ActionCreate:
		query.With.Records = result.Records
		_ = handler.logManager.AddLog(query)
	case queryConstants.ActionUpdate:
		fallthrough
	case queryConstants.ActionDelete:
		_ = handler.logManager.AddLog(query)
	}
}
