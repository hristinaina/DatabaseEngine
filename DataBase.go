package main

import (
	"NASP/BloomFilter"
	"NASP/CacheLRU"
	"NASP/CountMinSketch"
	"NASP/HyperLogLog"
	"NASP/Memtable"
	"NASP/MerkleTree"
	"NASP/SSTable"
	"NASP/TokenBucket"
	"NASP/WAL"
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

const (
	cacheMaxSeg     = "cache_max_segments"
	walMaxSeg       = "wal_max_segments"
	walLowMark      = "wal_low_mark"
	tbNumTokens     = "token_bucket_max_tokens"
	tbResetInterval = "token_bucket_interval"
	mtMaxLevel      = "memtable_max_level"
	mtThreshold     = "memtable_threshold"
	mtMaxElem       = "memtable_max_elements"
	sstMaxIndex     = "sstable_max_elem_per_level"
	sstMaxLevel     = "sstable_max_level"
)

type DataBase struct {
	//TODO Memtable
	memtable    *Memtable.Memtable
	cache       *CacheLRU.Cache
	tokenBucket *TokenBucket.TokenBucket
	wal         *WAL.WriteAheadLog
	config      map[string]int
}

func _defaultConfig() map[string]int {
	cfg := make(map[string]int)
	cfg[cacheMaxSeg] = 5
	cfg[walMaxSeg] = 5
	cfg[walLowMark] = 3
	cfg[tbNumTokens] = 60
	cfg[tbResetInterval] = 60
	cfg[mtMaxLevel] = 10
	cfg[mtThreshold] = 175
	cfg[mtMaxElem] = 20
	cfg[sstMaxIndex] = 4
	cfg[sstMaxLevel] = 3
	return cfg
}

func GenerateDataBase() DataBase {
	db := DataBase{}
	// Config
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
	// TokenBucket
	db.tokenBucket = TokenBucket.GenerateTokenBuket(db.config[tbNumTokens], db.config[tbResetInterval])
	// Memtable
	db.memtable = Memtable.NewMemtable(db.config[mtMaxLevel], db.config[mtThreshold], db.config[mtMaxElem])
	// Cache
	db.cache = CacheLRU.GenerateCache(uint32(db.config[cacheMaxSeg]))
	// Wal
	db.wal = WAL.GenerateWAL(uint16(db.config[walMaxSeg]), uint16(db.config[walLowMark]))
	return db
}

func (db *DataBase) Put(key string, value []byte) {
	db.cache.InsertElement(key, value)
	db.wal.InsertData(key, value)
	node := db.memtable.Find(key)
	if node != nil {
		// Change
		db.memtable.Modify(key, value)
		fmt.Println("Element sa ovim kljucem vec postoji u memtable te je izmenjen!")
	} else {
		// Insert
		success := db.memtable.Insert(key, value)
		if !success {
			db.Flush()
			db.memtable.Insert(key, value)
		}
		fmt.Println("Dodali smo dati element!")
	}
	if db.memtable.IsReadyToFlush() {
		db.Flush()
	}
}

func (db *DataBase) Get(key string) (bool, []byte) {
	//TODO Get path
	return false, nil
}

func (db *DataBase) Delete(key string) bool {
	found, _ := db.Get(key)
	if found {
		db.memtable.Delete(key)
		db.cache.RemoveElement(key)
		return true
	}
	return false
}

func (db *DataBase) Flush() {
	//TODO Flush
}

func (db *DataBase) Quit() {
	db.Flush()
}

// ------------------------------------------------------------------------- Main Menu
func printStandardMenu() {
	fmt.Println("\n--------- [ MAIN MENU ] ---------")
	fmt.Println("Dostupne komande: ")
	fmt.Println(" > PUT|key|value")
	fmt.Println(" > GET|key")
	fmt.Println(" > DELETE|key")
	fmt.Println(" > 10+")
	fmt.Println("Unesite komandu (X za izadji): ")
}
func main() {
	app := GenerateDataBase()
	reader := bufio.NewReader(os.Stdin)
	for {
		printStandardMenu()
		input, _ := reader.ReadString('\n')
		input = strings.Replace(input, "\n", "", 1)
		input = strings.Replace(input, "\r", "", 1)
		if input == "X" {
			break
		} else if input == "10+" {
			plus10Menu(&app)
			continue
		}
		inputSplit := strings.Split(input, "|")
		if inputSplit[0] == "PUT" {
			if app.tokenBucket.Update() {
				key := inputSplit[1]
				value := []byte(inputSplit[2])
				app.Put(key, value)
			} else {
				fmt.Println("Dostigli ste maksimalan broj zahteva\nMolimo pokusajte kasnije.")
			}
		} else if inputSplit[0] == "GET" {
			if app.tokenBucket.Update() {
				found, value := app.Get(inputSplit[1])
				if found {
					fmt.Println("Pronasli smo podatak sa kljucem " + inputSplit[1] + " : " + string(value))
				} else {
					fmt.Println("Dati kljuc nije pronadjen! :(")
				}
			} else {
				fmt.Println("Dostigli ste maksimalan broj zahteva\nMolimo pokusajte kasnije.")
			}
		} else if inputSplit[0] == "DELETE" {
			if app.tokenBucket.Update() {
				isDeleted := app.Delete(inputSplit[1])
				if isDeleted {
					fmt.Println("Obrisali smo podatak sa kljucem " + inputSplit[1])
				} else {
					fmt.Println("Dati kljuc nije pronadjen! :(")
				}
			} else {
				fmt.Println("Dostigli ste maksimalan broj zahteva\nMolimo pokusajte kasnije.")
			}
		} else {
			fmt.Println("Uneli ste nepoznatu komandu.")
		}
	}
	app.Quit()
}

// ------------------------------------------------------------------------- 10+ Menu
func print10plusMenu() {
	fmt.Println("\n----------- [ 10+ MENU ] -----------")
	fmt.Println("Dostupne komande: ")
	fmt.Println(" > PUT_HLL|key|precision[4-8]")
	fmt.Println(" > GET_HLL|key")
	fmt.Println(" > PUT_CMS|key|epsilon(0,1)|delta(0,1)")
	fmt.Println(" > GET_CMS|key")
	fmt.Println(" > TEST_PUT_HLL|key")
	fmt.Println(" > TEST_GET_HLL|key")
	fmt.Println(" > TEST_PUT_CMS|key")
	fmt.Println(" > TEST_GET_CMS|key")
	fmt.Println("Unesite komandu (N za nazad): ")
}
func plus10Menu(app *DataBase) {
	reader := bufio.NewReader(os.Stdin)
	for {
		print10plusMenu()
		input, _ := reader.ReadString('\n')
		input = strings.Replace(input, "\n", "", 1)
		input = strings.Replace(input, "\r", "", 1)
		if input == "N" {
			return
		}
		inputSplit := strings.Split(input, "|")
		if inputSplit[0] == "PUT_HLL" {
			Put_HLL(app, inputSplit)
		} else if inputSplit[0] == "GET_HLL" {
			Get_HLL(app, inputSplit)
		} else if inputSplit[0] == "PUT_CMS" {
			Put_CMS(app, inputSplit)
		} else if inputSplit[0] == "GET_CMS" {
			Get_CMS(app, inputSplit)
		} else if inputSplit[0] == "TEST_PUT_HLL" {
			Test_put_HLL(app, inputSplit)
		} else if inputSplit[0] == "TEST_GET_HLL" {
			Test_get_HLL(app, inputSplit)
		} else if inputSplit[0] == "TEST_PUT_CMS" {
			Test_put_CMS(app, inputSplit)
		} else if inputSplit[0] == "TEST_GET_CMS" {
			Test_get_CMS(app, inputSplit)
		} else {
			fmt.Println("Uneli ste nepoznatu komandu.")
		}
	}
}

func Put_HLL(app *DataBase, input []string) {
	key := input[1]
	precisionInt, _ := strconv.Atoi(input[2])
	hll := HyperLogLog.MakeHLL(uint8(precisionInt))
	hllData := hll.Encode()
	app.Put(key, hllData)
}
func Get_HLL(app *DataBase, input []string) {
	key := input[1]
	found, hllData := app.Get(key)
	if found {
		hll := HyperLogLog.HLL{}
		hll.Decode(hllData)
		fmt.Println("Pronasli smo hll sa kljucem " + key + ":")
		fmt.Println(hll)
		return
	}
	fmt.Println("Dati kljuc nije pronadjen! :(")
}
func Put_CMS(app *DataBase, input []string) {
	key := input[1]
	epsilon, _ := strconv.ParseFloat(input[2], 64)
	delta, _ := strconv.ParseFloat(input[3], 64)
	cms := CountMinSketch.NewCountMinSketch(epsilon, delta)
	cmsData := cms.Encode()
	app.Put(key, cmsData)
}
func Get_CMS(app *DataBase, input []string) {
	key := input[1]
	found, cmsData := app.Get(key)
	if found {
		cms := CountMinSketch.CountMinSketch{}
		cms.Decode(cmsData)
		fmt.Println("Pronasli smo cms sa kljucem " + key + ":")
		fmt.Println(cms)
		return
	}
	fmt.Println("Dati kljuc nije pronadjen! :(")
}
func Test_put_HLL(app *DataBase, input []string) {
	key := input[1]
	hll := HyperLogLog.MakeHLL(8)
	hll.Add("test1")
	hll.Add("test2")
	hll.Add("test3")
	hll.Add("test4")
	hll.Add("test5")
	fmt.Println("Napravili smo hll i dodali par elemenata; Estimate = ", hll.Estimate())
	hllData := hll.Encode()
	app.Put(key, hllData)
}
func Test_get_HLL(app *DataBase, input []string) {
	key := input[1]
	found, hllData := app.Get(key)
	if found {
		hll := HyperLogLog.HLL{}
		hll.Decode(hllData)
		fmt.Println("Pronasli smo hll sa kljucem "+key+", i njegov Estimate = ", hll.Estimate())
		return
	}
	fmt.Println("Dati kljuc nije pronadjen! :(")
}
func Test_put_CMS(app *DataBase, input []string) {
	key := input[1]
	cms := CountMinSketch.NewCountMinSketch(0.01, 0.01)
	cms.AddItem("test1")
	cms.AddItem("test1")
	cms.AddItem("test2")
	cms.AddItem("test1")
	fmt.Println("Napravili smo cms i dodali elemenat test1 3 puta; FrequencyOfElement =", cms.FrequencyOfElement("test1"))
	cmsData := cms.Encode()
	fmt.Println(len(cmsData))
	app.Put(key, cmsData)
}
func Test_get_CMS(app *DataBase, input []string) {
	key := input[1]
	found, cmsData := app.Get(key)
	if found {
		cms := CountMinSketch.CountMinSketch{}
		cms.Decode(cmsData)

		fmt.Println("Pronasli smo cms sa kljucem "+key+", i njegov FrequencyOfElement(test1) =", cms.FrequencyOfElement("test1"))
		return
	}
	fmt.Println("Dati kljuc nije pronadjen! :(")
}
