package SSTable

import (
	"NASP/Structures/BloomFilter"
	"NASP/Structures/MerkleTree"
	"NASP/Structures/SkipList"
	"encoding/binary"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ SSTable
func CreateSSTable(dataset []*SkipList.Node, level int, index int) {

	//CREATE & OPEN FILES
	DataFile, error1 := os.OpenFile("Data/SSTableData/DataFile_lvl"+strconv.Itoa(level)+"_idx"+strconv.Itoa(index)+".db", os.O_CREATE|os.O_WRONLY, 0777)
	if error1 != nil {
		panic(error1)
	}

	IndexFile, error2 := os.OpenFile("Data/SSTableData/IndexFile_lvl"+strconv.Itoa(level)+"_idx"+strconv.Itoa(index)+".db", os.O_CREATE|os.O_WRONLY, 0777)
	if error2 != nil {
		panic(error2)
	}

	SummaryFile, error3 := os.OpenFile("Data/SSTableData/SummaryFile_lvl"+strconv.Itoa(level)+"_idx"+strconv.Itoa(index)+".db", os.O_CREATE|os.O_WRONLY, 0777)
	if error3 != nil {
		panic(error3)
	}

	// Write two first data in SummaryFile
	FirstDataSize := make([]byte, 8)
	FirstData_size := uint64(len(dataset[0].Key()))
	binary.LittleEndian.PutUint64(FirstDataSize, FirstData_size)

	LastDataSize := make([]byte, 8)
	LastData_size := uint64(len(dataset[len(dataset)-1].Key()))
	binary.LittleEndian.PutUint64(LastDataSize, LastData_size)

	SummaryFile.Write(FirstDataSize)
	SummaryFile.Write([]byte(dataset[0].Key()))
	SummaryFile.Write(LastDataSize)
	SummaryFile.Write([]byte(dataset[len(dataset)-1].Key()))

	// Set offset in DataFile and IndexFile on zero
	var OffsetDataFile uint64 = 0
	var OffsetIndexFile uint64 = 0

	for _, data := range dataset {

		// Write to DataFile

		CRC := make([]byte, 4)
		binary.LittleEndian.PutUint32(CRC, crc32.ChecksumIEEE(data.Value()))

		Timestamp := make([]byte, 8)
		binary.LittleEndian.PutUint64(Timestamp, uint64(data.Timestamp()))

		Tombstone := make([]byte, 1)
		if data.Tombstone() {
			Tombstone[0] = 1
		}

		KeySize := make([]byte, 8)
		KeySize_size := uint64(len(data.Key()))
		binary.LittleEndian.PutUint64(KeySize, KeySize_size)

		ValueSize := make([]byte, 8)
		ValueSize_size := uint64(len(data.Value()))
		binary.LittleEndian.PutUint64(ValueSize, ValueSize_size)

		DataFile.Write(CRC)
		DataFile.Write(Timestamp)
		DataFile.Write(Tombstone)
		DataFile.Write(KeySize)
		DataFile.Write(ValueSize)
		DataFile.Write([]byte(data.Key()))
		DataFile.Write(data.Value())
		dataSize := 4 + 8 + 1 + 8 + 8 + KeySize_size + ValueSize_size

		// Write to IndexFile
		OffsetDF := make([]byte, 8)
		binary.LittleEndian.PutUint64(OffsetDF, OffsetDataFile)

		IndexFile.Write(KeySize)
		IndexFile.Write([]byte(data.Key()))
		IndexFile.Write(OffsetDF)
		indexSize := 8 + KeySize_size + 8

		OffsetDataFile = OffsetDataFile + dataSize

		// Write to SummaryFile
		OffsetIF := make([]byte, 8)
		binary.LittleEndian.PutUint64(OffsetIF, OffsetIndexFile)

		SummaryFile.Write(KeySize)
		SummaryFile.Write([]byte(data.Key()))
		SummaryFile.Write(OffsetIF)

		OffsetIndexFile = OffsetIndexFile + indexSize
	}

	// CLOSE FILES
	DataFile.Close()
	IndexFile.Close()
	SummaryFile.Close()
}

//  Return : Found & offset in Index File
func FindInSummary(file *os.File, key string) (bool, uint64) {
	// Read first data
	FirstDataSizeB := make([]byte, 8)
	file.Read(FirstDataSizeB)
	FirstDataSize := binary.LittleEndian.Uint64(FirstDataSizeB)

	FirstKeyB := make([]byte, FirstDataSize)
	file.Read(FirstKeyB)
	FirstKey := string(FirstKeyB)

	// Read last data
	LastDataSizeB := make([]byte, 8)
	file.Read(LastDataSizeB)
	LastDataSize := binary.LittleEndian.Uint64(LastDataSizeB)

	LastKeyB := make([]byte, LastDataSize)
	file.Read(LastKeyB)
	LastKey := string(LastKeyB)

	// Check all data
	if (key >= FirstKey) && (key <= LastKey) {
		for {
			KeySizeB := make([]byte, 8)
			_, Error := file.Read(KeySizeB)
			if Error == io.EOF {
				return false, 0
			}
			KeySize := binary.LittleEndian.Uint64(KeySizeB)

			KeyB := make([]byte, KeySize)
			file.Read(KeyB)
			Key := string(KeyB)

			if Key == key {

				OffsetB := make([]byte, 8)
				file.Read(OffsetB)
				Offset := binary.LittleEndian.Uint64(OffsetB)

				return true, Offset

			} else {
				file.Seek(8, 1)
			}
		}
	}
	return false, 0
}

func FindInIndex(file *os.File, offset uint64) uint64 {
	file.Seek(int64(offset), 0)
	KeySizeB := make([]byte, 8)
	_, Error := file.Read(KeySizeB)
	if Error != nil {
		panic(Error)
	}
	KeySize := binary.LittleEndian.Uint64(KeySizeB)

	file.Seek(int64(KeySize), 1)

	OffsetB := make([]byte, 8)
	file.Read(OffsetB)
	Offset := binary.LittleEndian.Uint64(OffsetB)

	return Offset
}

//4 + 8 + 1 + 8 + 8 + KeySize_size + ValueSize_size
func FindInData(file *os.File, offset uint64) (bool, []byte) {
	file.Seek(int64(offset), 0)
	file.Seek(4+8, 1)

	TumbstoneB := make([]byte, 1)
	file.Read(TumbstoneB)
	if byteToBool(TumbstoneB) {
		return false, nil
	}

	KeySizeB := make([]byte, 8)
	file.Read(KeySizeB)
	KeySize := binary.LittleEndian.Uint64(KeySizeB)

	ValueSizeB := make([]byte, 8)
	file.Read(ValueSizeB)
	ValueSize := binary.LittleEndian.Uint64(ValueSizeB)

	file.Seek(int64(KeySize), 1)

	Value := make([]byte, ValueSize)
	file.Read(Value)

	return true, Value
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Compactions
func Compactions(NumberOfFiles int, MaxLevel int, level int, maxBloomFilterElem int) {
	CurrentFile := 0
	for CurrentFile < NumberOfFiles {

		// OPEN FILES
		CurrentFile += 1
		DataFile1, error1 := os.OpenFile("Data/SSTableData/DataFile_lvl"+strconv.Itoa(level)+"_idx"+strconv.Itoa(CurrentFile)+".db", os.O_RDONLY, 0777)
		if error1 != nil {
			panic(error1)
		}

		CurrentFile += 1
		DataFile2, error2 := os.OpenFile("Data/SSTableData/DataFile_lvl"+strconv.Itoa(level)+"_idx"+strconv.Itoa(CurrentFile)+".db", os.O_RDONLY, 0777)
		if error2 != nil {
			panic(error2)
		}

		newSkipList := SkipList.NewSkipList(11)
		newBloomFilter := BloomFilter.NewBloom(2*maxBloomFilterElem, 0.05) //// TODO : ?????????????????????????????????????????????????

		for {
			// Convert data from bytes
			Timestamp1, Tombstone1, KeySize1, ValueSize1, Key1, Value1, Error1 := ReadDataFromFile(DataFile1)
			Timestamp2, Tombstone2, KeySize2, ValueSize2, Key2, Value2, Error2 := ReadDataFromFile(DataFile2)

			// Check if EOF
			if Error1 || Error2 {
				if !Error1 {
					Offset := 4 + 8 + 1 + 8 + 8 + int64(KeySize1) + int64(ValueSize1)
					DataFile1.Seek(-Offset, 1)
				} else if !Error2 {
					Offset := 4 + 8 + 1 + 8 + 8 + int64(KeySize2) + int64(ValueSize2)
					DataFile2.Seek(-Offset, 1)
				}
				break
			}

			if Key1 == Key2 {
				if Tombstone1[0] != Tombstone2[0] {
					continue
				} else {
					if Timestamp1 > Timestamp2 {
						newSkipList.Insert(Key1, Value1, false)
						newBloomFilter.Add(Key1)
					} else {
						newSkipList.Insert(Key2, Value2, false)
						newBloomFilter.Add(Key2)
					}
				}
			} else {
				if Key1 > Key2 {
					newSkipList.Insert(Key2, Value2, byteToBool(Tombstone2))
					newBloomFilter.Add(Key2)

					Offset := 4 + 8 + 1 + 8 + 8 + int64(KeySize1) + int64(ValueSize1)
					DataFile1.Seek(-Offset, 1)
				} else {
					newSkipList.Insert(Key1, Value1, byteToBool(Tombstone1))
					newBloomFilter.Add(Key1)

					Offset := 4 + 8 + 1 + 8 + 8 + int64(KeySize2) + int64(ValueSize2)
					DataFile2.Seek(-Offset, 1)
				}
			}
		}

		for {
			_, Tombstone, _, _, Key, Value, Error := ReadDataFromFile(DataFile1)
			if Error {
				break
			}

			newSkipList.Insert(Key, Value, byteToBool(Tombstone))
			newBloomFilter.Add(Key)
		}

		for {
			_, Tombstone, _, _, Key, Value, Error := ReadDataFromFile(DataFile2)
			if Error {
				break
			}

			newSkipList.Insert(Key, Value, byteToBool(Tombstone))
			newBloomFilter.Add(Key)
		}

		// CLOSE FILES
		DataFile1.Close()
		DataFile2.Close()

		// Delete old Files
		DeleteOldFiles(level, CurrentFile)

		// Find index for new Files
		NewIndex := FindLSMIndex(level+1) + 1

		// Serialize Files
		newBloomFilter.Serialize(level+1, NewIndex)

		dataset := newSkipList.GetAllElements()
		CreateSSTable(dataset, level+1, NewIndex)

		node := MerkleTree.MakeNodesForMerkle(dataset)
		newMarkleTree := MerkleTree.NewMerkleTree(node)
		newMarkleTree.Root.Serialization(level+1, NewIndex)

		// Serialize TOCFiles
		SerializationTOC(level+1, NewIndex)
	}

	// Next Compaction
	NewIndex := FindLSMIndex(level + 1)
	if NewIndex == NumberOfFiles && (level+1) < MaxLevel {
		Compactions(NumberOfFiles, MaxLevel, level+1, 2*maxBloomFilterElem)
	}

}

func byteToBool(Tombstone []byte) bool {
	isTombstone := false
	if Tombstone[0] == 1 {
		isTombstone = true
	}
	return isTombstone
}

// Convert data from bytes
func ReadDataFromFile(File *os.File) (uint64, []byte, uint64, uint64, string, []byte, bool) {

	crc := make([]byte, 4)
	_, Error := File.Read(crc)
	if Error == io.EOF {
		return 0, nil, 0, 0, "", nil, true
	}
	CRC := binary.LittleEndian.Uint32(crc)

	timestamp := make([]byte, 8)
	File.Read(timestamp)
	Timestamp := binary.LittleEndian.Uint64(timestamp)

	Tombstone := make([]byte, 1)
	File.Read(Tombstone)

	keySize := make([]byte, 8)
	File.Read(keySize)
	KeySize := binary.LittleEndian.Uint64(keySize)

	valueSize := make([]byte, 8)
	File.Read(valueSize)
	ValueSize := binary.LittleEndian.Uint64(valueSize)

	Key := make([]byte, KeySize)
	File.Read(Key)

	Value := make([]byte, ValueSize)
	File.Read(Value)

	if crc32.ChecksumIEEE(Value) != CRC {
		panic("Corupted")
	}

	return Timestamp, Tombstone, KeySize, ValueSize, string(Key), Value, false
}

func GetBloomfilterFilesToSearch(level int) []string {
	bloomFilterFiles := make([]string, 0)
	allFiles, _ := ioutil.ReadDir("Data/SSTableData")
	for _, file := range allFiles {
		fullFileName := file.Name()
		fileName := strings.Replace(fullFileName, "lvl", "", 1)
		fileName = strings.Replace(fileName, "idx", "", 1)
		splited := strings.Split(fileName, "_")
		bloomLevel, _ := strconv.Atoi(splited[1])
		if splited[0] == "BloomFilterFile" && bloomLevel == level {
			bloomFilterFiles = append(bloomFilterFiles, fullFileName)
		}
	}
	return bloomFilterFiles
}

// Find new LSM index
func FindLSMIndex(level int) int {

	AllFiles, _ := ioutil.ReadDir("Data/SSTableData")

	index := 0

	for _, File := range AllFiles {
		FullFileName := File.Name()
		FileName := strings.Replace(FullFileName, "lvl", "", 1)
		FileName = strings.Replace(FileName, "idx", "", 1)
		if strings.Split(FileName, "_")[0] == "DataFile" {
			lvl, _ := strconv.Atoi(strings.Split(FileName, "_")[1])
			IdxString := strings.Split(FileName, "_")[2]
			idx, _ := strconv.Atoi(strings.Split(IdxString, ".")[0])

			if (level == lvl) && (index < idx) {
				index = idx
			}
		}
	}
	return index
}

// Serialize TOCFile
func SerializationTOC(level int, index int) {

	File, err := os.OpenFile("Data/TOCFiles/TOCFile_lvl"+strconv.Itoa(level)+"_idx"+strconv.Itoa(index)+".txt", os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		panic(err)
	}

	_, err = File.Write([]byte("Data/SSTableData/DataFile_lvl" + strconv.Itoa(level) + "_idx" + strconv.Itoa(index) + ".db\n"))
	if err != nil {
		log.Fatal(err)
	}
	err = File.Sync()
	if err != nil {
		log.Fatal(err)
	}

	_, err = File.Write([]byte("Data/SSTableData/IndexFile_lvl" + strconv.Itoa(level) + "_idx" + strconv.Itoa(index) + ".db\n"))
	if err != nil {
		log.Fatal(err)
	}
	err = File.Sync()
	if err != nil {
		log.Fatal(err)
	}

	_, err = File.Write([]byte("Data/SSTableData/SummaryFile_lvl" + strconv.Itoa(level) + "_idx" + strconv.Itoa(index) + ".db\n"))
	if err != nil {
		log.Fatal(err)
	}
	err = File.Sync()
	if err != nil {
		log.Fatal(err)
	}

	_, err = File.Write([]byte("Data/SSTableData/BloomFilterFile_lvl" + strconv.Itoa(level) + "_idx" + strconv.Itoa(index) + ".db\n"))
	if err != nil {
		log.Fatal(err)
	}
	err = File.Sync()
	if err != nil {
		log.Fatal(err)
	}

	_, err = File.Write([]byte("Data/SSTableData/MerkleTreeFile_lvl" + strconv.Itoa(level) + "_idx" + strconv.Itoa(index) + ".txt\n"))
	if err != nil {
		log.Fatal(err)
	}
	err = File.Sync()
	if err != nil {
		log.Fatal(err)
	}

	File.Close()
}

// Delete old Files
func DeleteOldFiles(level int, index int) {

	// Delete DataFiles
	error1 := os.Remove("Data/SSTableData/DataFile_lvl" + strconv.Itoa(level) + "_idx" + strconv.Itoa(index-1) + ".db")
	if error1 != nil {
		log.Fatal(error1)
	}
	error2 := os.Remove("Data/SSTableData/DataFile_lvl" + strconv.Itoa(level) + "_idx" + strconv.Itoa(index) + ".db")
	if error2 != nil {
		log.Fatal(error2)
	}

	//Delete IndexFiles
	error3 := os.Remove("Data/SSTableData/IndexFile_lvl" + strconv.Itoa(level) + "_idx" + strconv.Itoa(index-1) + ".db")
	if error3 != nil {
		log.Fatal(error3)
	}
	error4 := os.Remove("Data/SSTableData/IndexFile_lvl" + strconv.Itoa(level) + "_idx" + strconv.Itoa(index) + ".db")
	if error4 != nil {
		log.Fatal(error4)
	}

	//Delete SummaryFiles
	error5 := os.Remove("Data/SSTableData/SummaryFile_lvl" + strconv.Itoa(level) + "_idx" + strconv.Itoa(index-1) + ".db")
	if error5 != nil {
		log.Fatal(error5)
	}
	error6 := os.Remove("Data/SSTableData/SummaryFile_lvl" + strconv.Itoa(level) + "_idx" + strconv.Itoa(index) + ".db")
	if error6 != nil {
		log.Fatal(error6)
	}

	// Delete BloomFilterFiles
	error7 := os.Remove("Data/SSTableData/BloomFilterFile_lvl" + strconv.Itoa(level) + "_idx" + strconv.Itoa(index-1) + ".db")
	if error7 != nil {
		log.Fatal(error7)
	}
	error8 := os.Remove("Data/SSTableData/BloomFilterFile_lvl" + strconv.Itoa(level) + "_idx" + strconv.Itoa(index) + ".db")
	if error8 != nil {
		log.Fatal(error8)
	}

	// Delete MerkleTreeFiles
	error9 := os.Remove("Data/SSTableData/MerkleTreeFile_lvl" + strconv.Itoa(level) + "_idx" + strconv.Itoa(index-1) + ".txt")
	if error9 != nil {
		log.Fatal(error9)
	}
	error10 := os.Remove("Data/SSTableData/MerkleTreeFile_lvl" + strconv.Itoa(level) + "_idx" + strconv.Itoa(index) + ".txt")
	if error10 != nil {
		log.Fatal(error10)
	}

	// Delete TOCFiles
	error11 := os.Remove("Data/TOCFiles/TOCFile_lvl" + strconv.Itoa(level) + "_idx" + strconv.Itoa(index-1) + ".txt")
	if error11 != nil {
		log.Fatal(error11)
	}
	error12 := os.Remove("Data/TOCFiles/TOCFile_lvl" + strconv.Itoa(level) + "_idx" + strconv.Itoa(index) + ".txt")
	if error12 != nil {
		log.Fatal(error12)
	}
}

func test() {
	SerializationTOC(1, 1)
	SerializationTOC(5, 5)
	SerializationTOC(20, 12)
}
