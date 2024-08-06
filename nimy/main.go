package main

import (
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/utils"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/managers"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/query/managers"
	"github.com/stevekineeve88/nimydb-engine/pkg/system"
	"github.com/stevekineeve88/nimydb-engine/pkg/system/managers"
	"log/slog"
	"nimy/config"
	"nimy/interface/handlers"
	"nimy/interface/models"
)

func main() {
	configFileLoc := "./config/config.json"

	slog.Info("reading from config", "file", configFileLoc)
	nimyDBConfig := config.GetConfig(configFileLoc)
	_ = diskUtils.CreateDir(nimyDBConfig.DataLocation)

	dbMap := memoryModels.NewDBMap(nimyDBConfig.DataLocation, nimyDBConfig.Caching)
	userPool := models.NewUserPool()

	//set managers
	operationManager := memoryManagers.CreateOperationManager(&dbMap)

	slog.Info("initializing system database")
	system.InitDB(operationManager)
	slog.Info("system database initialized successfully")

	queryManager := queryManagers.CreateQueryManager(operationManager)
	logManager := systemManagers.CreateLogManager(operationManager)
	userManager := systemManagers.CreateUserManager(operationManager)

	slog.Info("initializing root user")
	userManager.InitRoot(nimyDBConfig.RootPass)
	slog.Info("root user initialized successfully")
	systemQueryManager := queryManagers.CreateSystemQueryManager(logManager, userManager)

	//set handlers
	protocolHandler := handlers.CreateProtocolHandler(queryManager, systemQueryManager, logManager)
	connectionHandler := handlers.CreateConnectionHandler(protocolHandler, &userPool)

	slog.Info("starting server", "port", nimyDBConfig.Port)
	connectionHandler.Start(nimyDBConfig.Port)
}
