package main

import (
	"NASP/Structures/BloomFilter"
	"NASP/Structures/CacheLRU"
	"NASP/Structures/CountMinSketch"
	"NASP/Structures/HyperLogLog"
	"NASP/Structures/Memtable"
	"NASP/Structures/MerkleTree"
	"NASP/Structures/SSTable"
	"NASP/Structures/TokenBucket"
	"NASP/Structures/WAL"
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
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
	// Try to find in Memtable
	node := db.memtable.Find(key)
	if node != nil {
		value := node.Value()
		db.cache.InsertElement(key, value)
		return true, value
	}

	// Try to find deleted in Memtable
	node = db.memtable.FindDeleted(key)
	if node != nil {
		return false, nil
	}

	// Try to find in Cache
	found, value := db.cache.GetElement(key)
	if found {
		db.cache.InsertElement(key, value)
		return true, value
	}

	// Try to find in BloomFilter -> Summary -> Index -> Data
	for level := 1; level <= db.config[sstMaxLevel]; level++ {
		bloomFilterFiles := SSTable.GetBloomfilterFilesToSearch(level)
		for i := len(bloomFilterFiles) - 1; i >= 0; i-- {
			fileName := bloomFilterFiles[i]

			newBloomFilter := BloomFilter.Deserialize("Data/SSTableData/" + fileName)
			found = newBloomFilter.FindElem(key)

			lvl := strings.Split(fileName, "_")[1]
			IdxString := strings.Split(fileName, "_")[2]
			idx := strings.Split(IdxString, ".")[0]

			// Found in BloomFilter -> Search in SummaryFile
			if found {
				SF, error1 := os.OpenFile("Data/SSTableData/SummaryFile_"+lvl+"_"+idx+".db", os.O_RDONLY, 0777)
				if error1 != nil {
					panic(error1)
				}
				foundInSum, offset := SSTable.FindInSummary(SF, key)
				if foundInSum {

					// Find offset in Index File
					IF, error2 := os.OpenFile("Data/SSTableData/IndexFile_"+lvl+"_"+idx+".db", os.O_RDONLY, 0777)
					if error2 != nil {
						panic(error2)
					}
					newOffset := SSTable.FindInIndex(IF, offset)

					// Find data in DataFile
					DF, error3 := os.OpenFile("Data/SSTableData/DataFile_"+lvl+"_"+idx+".db", os.O_RDONLY, 0777)
					if error3 != nil {
						panic(error3)
					}
					found, value := SSTable.FindInData(DF, newOffset)

					err := SF.Close()
					if err != nil {
						fmt.Println(err.Error())
					}
					err = IF.Close()
					if err != nil {
						fmt.Println(err.Error())
					}
					err = DF.Close()
					if err != nil {
						fmt.Println(err.Error())
					}

					if found {
						return true, value
					} else {
						return false, value
					}
				}
			}
		}
	}
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
	
	if db.memtable.CurrentSize() == 0 {
		return
	}

	// Get Data
	dataset := db.memtable.Skiplist.GetAllElements()

	// Bloom Filter
	newBloomFilter := BloomFilter.NewBloom(db.memtable.MaxElements(), 0.05)
	for _, data := range dataset {
		newBloomFilter.Add(data.Key())
	}

	// Add data to MerkleTree
	node := MerkleTree.MakeNodesForMerkle(dataset)
	newMerkleTree := MerkleTree.NewMerkleTree(node)

	// Find Last File Index in first level
	// Add 1 to make New File Index in first level
	NewIndex := SSTable.FindLSMIndex(1) + 1

	// Serialize SSTable, BloomFilter, MerkleTree & TOC
	SSTable.CreateSSTable(dataset, 1, NewIndex)
	newBloomFilter.Serialize(1, NewIndex)
	newMerkleTree.Root.Serialization(1, NewIndex)
	SSTable.SerializationTOC(1, NewIndex)

	// Restart Memtable
	db.memtable.Empty()

	// Checking whether compaction must be performed
	if NewIndex == db.config[sstMaxIndex] {
		SSTable.Compactions(db.config[sstMaxIndex], db.config[sstMaxLevel], 1, db.memtable.MaxElements())
	}
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
		if strings.ToUpper(input) == "X" {
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
		if strings.ToUpper(input) == "N" {
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
		} else {
			fmt.Println("Uneli ste nepoznatu komandu.")
		}
	}
}

func Put_HLL(app *DataBase, input []string) {
	key := input[1]
	found, hllData := app.Get(key)
	hll := HyperLogLog.MakeHLL(8)
	if found {
		hll.Decode(hllData)
	}
	hll.Add(input[2])
	hllWriteData := hll.Encode()
	app.Put(key, hllWriteData)
}

func Get_HLL(app *DataBase, input []string) {
	key := input[1]
	found, hllData := app.Get(key)
	if found {
		hll := HyperLogLog.HLL{}
		hll.Decode(hllData)
		fmt.Println("Pronasli smo hll sa kljucem "+key+", i njegov Estimate = ", hll.Estimate())
		//fmt.Println(hll)
		return
	}
	fmt.Println("Dati kljuc nije pronadjen! :(")
}

func Put_CMS(app *DataBase, input []string) {
	key := input[1]
	found, cmsData := app.Get(key)
	cms := CountMinSketch.NewCountMinSketch(0.01, 0.01)
	if found {
		cms.Decode(cmsData)
	}
	cms.AddItem(input[2])
	cmsWriteData := cms.Encode()
	app.Put(key, cmsWriteData)
}

func Get_CMS(app *DataBase, input []string) {
	key := input[1]
	found, cmsData := app.Get(key)
	if found {
		cms := CountMinSketch.CountMinSketch{}
		cms.Decode(cmsData)
		fmt.Println("Pronasli smo cms sa kljucem "+key+","+
			" i njegov FrequencyOfElement za '"+input[2]+"' = ", cms.FrequencyOfElement(input[2]))
		//fmt.Println(cms)
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
