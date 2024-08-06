package models

import (
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/system/models"
	"sync"
)

type UserPool struct {
	m             *sync.Mutex
	ConnectionMap map[string]systemModels.User
}

func NewUserPool() UserPool {
	return UserPool{
		m:             &sync.Mutex{},
		ConnectionMap: make(map[string]systemModels.User),
	}
}

func (up *UserPool) Add(connectionKey string, user systemModels.User) {
	up.m.Lock()
	defer up.m.Unlock()
	up.ConnectionMap[connectionKey] = user
}

func (up *UserPool) Get(connectionKey string) (systemModels.User, error) {
	up.m.Lock()
	defer up.m.Unlock()
	if user, ok := up.ConnectionMap[connectionKey]; ok {
		return user, nil
	}
	return systemModels.User{}, fmt.Errorf("connection %s not authenticated", connectionKey)
}

func (up *UserPool) Delete(connectionKey string) {
	up.m.Lock()
	defer up.m.Unlock()
	delete(up.ConnectionMap, connectionKey)
}
