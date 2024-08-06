package handlers

import (
	"encoding/json"
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/query/constants"
	"github.com/stevekineeve88/nimydb-engine/pkg/query/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/system/constants"
	"github.com/stevekineeve88/nimydb-engine/pkg/system/models"
	"log/slog"
	"net"
	"nimy/interface/models"
)

type ConnectionHandler interface {
	Start(port string)
}

type connectionHandler struct {
	protocolHandler ProtocolHandler
	userPool        *models.UserPool
}

func CreateConnectionHandler(protocolHandler ProtocolHandler, userPool *models.UserPool) ConnectionHandler {
	return &connectionHandler{
		protocolHandler: protocolHandler,
		userPool:        userPool,
	}
}

func (handler *connectionHandler) Start(port string) {
	l, err := net.Listen("tcp4", fmt.Sprintf(":%s", port))
	if err != nil {
		panic(err)
	}
	defer func() {
		if l != nil {
			slog.Info("stopping server")
			_ = l.Close()
		}
	}()
	slog.Info("server started successfully")
	for {
		c, err := l.Accept()
		if err != nil {
			return
		}
		go handler._handleConnection(c)
	}
}

func (handler *connectionHandler) _handleConnection(conn net.Conn) {
	defer func() {
		slog.Info("closing connection", "client", conn.RemoteAddr().String())
		handler.userPool.Delete(conn.RemoteAddr().String())
		_ = conn.Close()
	}()
	slog.Info("connection established", "client", conn.RemoteAddr().String())
	for {
		decoder := json.NewDecoder(conn)
		var message models.ProtocolMessage
		err := decoder.Decode(&message)
		if err != nil {
			return
		}
		if !handler._hasPermission(conn.RemoteAddr().String(), message) {
			result := queryModels.QueryResult{
				ErrorMessage: "permission denied",
			}
			resultBytes, _ := json.Marshal(result)
			_, _ = conn.Write(resultBytes)
			continue
		}
		result := handler.protocolHandler.Handle(message)
		if result.ConnectionUser.Id != "" {
			handler.userPool.Add(conn.RemoteAddr().String(), result.ConnectionUser)
		}
		resultBytes, _ := json.Marshal(result)
		_, _ = conn.Write(resultBytes)
	}
}

func (handler *connectionHandler) _hasPermission(connectionKey string, message models.ProtocolMessage) bool {
	user, _ := handler.userPool.Get(connectionKey)
	checks := map[string]func(user systemModels.User) bool{
		queryConstants.ActionGet + queryConstants.OnLogs:    func(user systemModels.User) bool { return systemConstants.HasSuperRead(user.Permission) },
		queryConstants.ActionGet + queryConstants.OnUsers:   func(user systemModels.User) bool { return systemConstants.HasSuperRead(user.Permission) },
		queryConstants.ActionCreate + queryConstants.OnData: func(user systemModels.User) bool { return systemConstants.HasReadWrite(user.Permission) },
		queryConstants.ActionCreate + queryConstants.OnBlob: func(user systemModels.User) bool { return systemConstants.HasReadWrite(user.Permission) },
		queryConstants.ActionCreate + queryConstants.OnDB:   func(user systemModels.User) bool { return systemConstants.HasReadWrite(user.Permission) },
		queryConstants.ActionDelete + queryConstants.OnData: func(user systemModels.User) bool { return systemConstants.HasReadWrite(user.Permission) },
		queryConstants.ActionDelete + queryConstants.OnBlob: func(user systemModels.User) bool { return systemConstants.HasReadWrite(user.Permission) },
		queryConstants.ActionDelete + queryConstants.OnDB:   func(user systemModels.User) bool { return systemConstants.HasReadWrite(user.Permission) },
		queryConstants.ActionUpdate + queryConstants.OnData: func(user systemModels.User) bool { return systemConstants.HasReadWrite(user.Permission) },
	}
	if checkFunc, ok := checks[message.Query.Action+message.Query.On]; ok {
		return checkFunc(user)
	}
	return true
}
