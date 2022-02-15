package main

import (
	"encoding/json"
	"io/ioutil"
)

const (
	cacheMaxSeg     = "cache_max_segments"
	walMaxSeg       = "wal_max_segments"
	walLowMark      = "wal_low_mark"
	tbNumTokens     = "token_bucket_max_tokens"
	tbResetInterval = "token_bucket_interval"
	//TODO Add the rest of consts
)

type DataBase struct {
	//TODO Memtable
	cache  *CacheLRU.Cache
	config map[string]int
}

func _defaultConfig() map[string]int {
	cfg := make(map[string]int)
	cfg[cacheMaxSeg] = 5
	cfg[walMaxSeg] = 5
	cfg[walLowMark] = 3
	cfg[tbNumTokens] = 60
	cfg[tbResetInterval] = 60
	//TODO Add the rest of def configs
	return cfg
}

func GenerateDataBase() DataBase {
	db := DataBase{}
	db.config = make(map[string]int)
	configFile, err := ioutil.ReadFile("config.json")
	if err != nil {
		db.config = _defaultConfig()
	} else {
		err := json.Unmarshal(configFile, &db.config)
		if err != nil {
			db.config = _defaultConfig()
		}
	}
	//TODO Memtable
	db.cache = CacheLRU.GenerateCache(uint32(db.config[cacheMaxSeg]))
	return db
}

func (db *DataBase) Put(key string, value []byte) {
	//TODO Write path
}

func (db *DataBase) Get(key string) (bool, []byte) {
	//TODO Get path
	return false, nil
}

func (db *DataBase) Delete(key string, value []byte) bool {
	//TODO Delete path
	return false
}

func (db *DataBase) Quit() {
	//TODO Flush memtable
}
