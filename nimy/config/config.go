package config

import (
	"encoding/json"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/utils"
)

type Config struct {
	Port         string `json:"port"`
	Caching      bool   `json:"caching"`
	RootPass     string `json:"rootPass"`
	DataLocation string `json:"dataLocation"`
}

func GetConfig(fileLocation string) Config {
	data, err := diskUtils.GetFile(fileLocation)
	if err != nil {
		panic(err)
	}
	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		panic(err)
	}
	return config
}
