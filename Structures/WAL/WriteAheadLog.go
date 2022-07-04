package WAL

import (
	"encoding/binary"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

/*
   CRC (4B)   | Timestamp (8B) | Tombstone (1B) | Key Size (8B) | Value Size (8B) | Key | Value
   CRC = 32bit hash for error correction
   Timestamp = Timestamp of the operation in seconds
   Tombstone = If this record was deleted and has a value
   Key Size = Length of the Key data
   Value Size = Length of the Value data
*/

type WriteAheadLog struct {
	maxSegments uint16
	curSegment  uint16
	lowMark     uint16
	file        *os.File
	dataMap     map[string][]byte
}

//Private Functions
func _packData(key string, value []byte, isTombstone bool) []byte {
	//CRC 32
	crc := make([]byte, 4)
	binary.LittleEndian.PutUint32(crc, crc32.ChecksumIEEE(value))

	//Timestamp
	timestamp := make([]byte, 8)

	//Tombstone
	tombstone := make([]byte, 1)
	if isTombstone {
		tombstone = []byte{1}
	}

	//Key Size
	keySize := make([]byte, 8)
	binary.LittleEndian.PutUint64(keySize, uint64(len([]byte(key))))

	//Value Size
	valueSize := make([]byte, 8)
	binary.LittleEndian.PutUint64(valueSize, uint64(len(value)))

	//Generate data
	dataToInsert := make([]byte, 0)
	dataToInsert = append(dataToInsert, crc...)
	dataToInsert = append(dataToInsert, timestamp...)
	dataToInsert = append(dataToInsert, tombstone...)
	dataToInsert = append(dataToInsert, keySize...)
	dataToInsert = append(dataToInsert, valueSize...)
	dataToInsert = append(dataToInsert, []byte(key)...)
	dataToInsert = append(dataToInsert, value...)

	return dataToInsert
}
func _writeData(file *os.File, data []byte) error {
	//Get the current file size
	info, err := file.Stat()
	if err != nil {
		return err
	}
	curSize := info.Size()
	//Extend the file for the length of new data
	err = file.Truncate(curSize + int64((len(data))))
	if err != nil {
		return err
	}
	//Write the new data to the file using mmap
	fileMap, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	copy(fileMap[curSize:], data)
	err = fileMap.Flush()
	if err != nil {
		return err
	}
	err = fileMap.Unmap()
	if err != nil {
		return err
	}
	return nil
}
func (wal *WriteAheadLog) _generateNewFile() error {
	currentFullName := wal.file.Name()
	currentName := strings.Replace(currentFullName, ".bin", "", 1)
	currentName = strings.Split(currentName, "WAL_")[1]
	currentIdx, _ := strconv.Atoi(currentName)
	newFileName := "WAL_" + strconv.Itoa(currentIdx+1) + ".bin"
	err := wal.file.Close()
	if err != nil {
		return err
	}
	wal.file, err = os.Create("Data/WAL/" + newFileName)
	if err != nil {
		return err
	}
	wal.curSegment = 0
	return nil
}

//Public Functions
func GenerateWAL(maxSeg uint16, lowMark uint16) *WriteAheadLog {
	files, _ := ioutil.ReadDir("Data/WAL")
	//Figure out the idx of the wal
	newIdx := 0
	for _, f := range files {
		fullName := f.Name()
		name := strings.Replace(fullName, ".bin", "", 1)
		name = strings.Split(name, "WAL_")[1]
		idx, _ := strconv.Atoi(name)
		if idx > newIdx {
			newIdx = idx
		}
	}
	wal := WriteAheadLog{}
	wal.maxSegments = maxSeg
	wal.lowMark = lowMark
	wal.dataMap = make(map[string][]byte) //TODO Run map reconstruction before return?
	if newIdx == 0 {
		wal.file, _ = os.Create("Data/WAL/WAL_1.bin")
	} else {
		newName := "WAL_" + strconv.Itoa(newIdx) + ".bin"
		wal.file, _ = os.OpenFile("Data/WAL/"+newName, os.O_RDWR, 0777)
	}
	return &wal
}
func (wal *WriteAheadLog) InsertData(key string, value []byte) bool {
	// Generate byte data
	dataToInsert := _packData(key, value, false)
	// Write
	err := _writeData(wal.file, dataToInsert)
	if err != nil {
		return false
	}
	wal.curSegment += 1
	if wal.curSegment >= wal.maxSegments { //We've filled current WAL file so we make a new one
		err = wal._generateNewFile()
		if err != nil {
			return false
		}
	}
	wal.dataMap[key] = value
	return true
}
func (wal *WriteAheadLog) DeleteData(key string, value []byte) bool {
	// Generate byte data
	dataToInsert := _packData(key, value, true)
	// Write
	err := _writeData(wal.file, dataToInsert)
	if err != nil {
		return false
	}
	wal.curSegment += 1
	if wal.curSegment >= wal.maxSegments { //We've filled current WAL file so we make a new one
		err = wal._generateNewFile()
		if err != nil {
			return false
		}
	}
	delete(wal.dataMap, key)
	return true
}
func (wal *WriteAheadLog) ReconstructMap() {
	newDataMap := make(map[string][]byte)
	processedEntries := 0
	files, _ := ioutil.ReadDir("Data/WAL/")
	for _, dat := range files {
		fileName := dat.Name()
		file, _ := os.OpenFile("Data/WAL/"+fileName, os.O_RDONLY, 0777)
		file.Seek(0, 0)
		processedEntries = 0
		for {
			//CRC
			crc := make([]byte, 4)
			_, err := file.Read(crc)
			if err == io.EOF {
				break
			}
			processedEntries += 1

			//Skip Timestamp
			file.Seek(8, 1)

			//Tombstone
			isTombstoneB := make([]byte, 1)
			file.Read(isTombstoneB)
			isTombstone := false
			if isTombstoneB[0] == 1 {
				isTombstone = true
			}

			//Key Size
			keySizeB := make([]byte, 8)
			file.Read(keySizeB)
			keySize := binary.LittleEndian.Uint64(keySizeB)

			//Value Size
			valueSizeB := make([]byte, 8)
			file.Read(valueSizeB)
			valueSize := binary.LittleEndian.Uint64(valueSizeB)

			//Key
			key := make([]byte, keySize)
			file.Read(key)

			//Value
			value := make([]byte, valueSize)
			file.Read(value)

			//isTombstone
			if isTombstone {
				delete(newDataMap, string(key))
			} else {
				newDataMap[string(key)] = value
			}
		}
	}
	wal.curSegment = uint16(processedEntries)
	wal.dataMap = newDataMap
}
func (wal *WriteAheadLog) ReadData(key string) ([]byte, bool) {
	value, exist := wal.dataMap[key]
	return value, exist
}
func (wal *WriteAheadLog) DeleteWALFiles() {
	// First we delete set amount of files
	for i := 1; uint16(i) < wal.lowMark; i++ {
		nameToDelete := "WAL_" + strconv.Itoa(i) + ".bin"
		err := os.Remove("Data/WAL/" + nameToDelete)
		if err != nil {
			panic(err)
		}
	}
	// Then we rename the remaining ones
	wal.file.Close() //TODO reopen the current wal file?
	wal.file.Sync()
	remaining, _ := ioutil.ReadDir("Data/WAL/")
	for id, dat := range remaining {
		fileName := dat.Name()
		newFileName := "WAL_" + strconv.Itoa(id+1) + ".bin"
		err := os.Rename("Data/WAL/"+fileName, "Data/WAL/"+newFileName)
		if err != nil {
			panic(err)
		}
	}
}
func (wal *WriteAheadLog) Reset() *WriteAheadLog {
	wal.file.Close()
	wal.file.Sync()
	err := os.RemoveAll("Data/WAL/")
	if err != nil {
		panic(err)
	}
	err = os.MkdirAll("Data/WAL/", 0777)
	if err != nil {
		panic(err)
	}
	newWal := GenerateWAL(5, 3)
	return newWal
}
func (wal *WriteAheadLog) Close() {
	wal.file.Close()
	wal.file.Sync()
}

//TEST
func initTest(curWal WriteAheadLog) {
	curWal.InsertData("test1", []byte("data1"))
	curWal.InsertData("test2", []byte("data2"))
	curWal.InsertData("test3", []byte("data3"))
	curWal.InsertData("test4", []byte("data4"))
	curWal.InsertData("test5", []byte("data5"))
	curWal.InsertData("test6", []byte("data6"))
	curWal.InsertData("test7", []byte("data7"))
	curWal.DeleteData("test4", []byte("data4"))
}
func findTest(curWal WriteAheadLog) {
	curWal.ReconstructMap()
	value, exist := curWal.ReadData("test5")
	if exist {
		fmt.Println(string(value))
	} else {
		fmt.Println("Data not found!")
	}
}
func test() {
	curWal := GenerateWAL(5, 3)
	curWal.Reset() //TODO How to connect new wal to old wal reference?
	//initTest(curWal)
	//findTest(curWal)
	//curWal.DeleteWALFiles()
	//fmt.Println(curWal.file.Name())
}
