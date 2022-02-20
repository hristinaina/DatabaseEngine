package BloomFilter

import (
	"encoding/gob"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"os"
	"strconv"
	"time"
)

func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

func CreateHashFunctions(k uint, ts uint) []hash.Hash32 {
	h := []hash.Hash32{}
	//Ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(ts+i)))
	}
	return h
}

type BloomFilter struct {
	ExpectedElem      int
	FalsePositiveRate float64
	M                 uint
	K                 uint
	Ts                uint
	HashFunc          []hash.Hash32
	Bytes             []byte
}

func NewBloom(expectedElem int, falsePositiveRate float64) BloomFilter {
	m := CalculateM(expectedElem, falsePositiveRate)
	k := CalculateK(expectedElem, m)
	//if Ts == 0 {
	ts := uint(time.Now().Unix())
	//}
	hashFuncs := CreateHashFunctions(k, ts)
	bytes := make([]byte, m)
	return BloomFilter{expectedElem, falsePositiveRate, m, k, ts, hashFuncs, bytes}
}

func (bloom *BloomFilter) Add(item string) {
	for i := uint(0); i < bloom.K; i++ {
		bloom.HashFunc[i].Reset()
		bloom.HashFunc[i].Write([]byte(item))
		j := bloom.HashFunc[i].Sum32() % uint32(bloom.M)
		bloom.Bytes[j] = 1
	}
}

func (bloom *BloomFilter) FindElem(item string) bool {
	for i := uint(0); i < bloom.K; i++ {
		bloom.HashFunc[i].Reset()
		bloom.HashFunc[i].Write([]byte(item))
		j := bloom.HashFunc[i].Sum32() % uint32(bloom.M)
		if bloom.Bytes[j] == 0 {
			return false
		}
	}
	return true
}

func (bloom *BloomFilter) Serialize(level int, index int) {
	file, err := os.OpenFile("Data/SSTableData/BloomFilterFile_lvl"+strconv.Itoa(level)+"_idx"+strconv.Itoa(index)+".db", os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		panic(err)
	}

	bloom.HashFunc = nil
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(bloom)
	if err != nil {
		panic(err)
	}

	file.Close()
}

func Deserialize(fileName string) *BloomFilter {
	file, err := os.OpenFile(fileName, os.O_RDWR, 0777)
	if err != nil {
		panic(err)
	}

	bloom := BloomFilter{}
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&bloom)
	if err != nil {
		panic(err)
	}
	hashFuncs := CreateHashFunctions(bloom.K, bloom.Ts)
	bloom.HashFunc = hashFuncs

	file.Close()

	return &bloom
}
