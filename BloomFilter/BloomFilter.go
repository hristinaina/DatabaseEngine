package main

import (
	"encoding/gob"
	"fmt"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"os"
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
	//ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(ts+i)))
	}
	return h
}

type BloomFilter struct {
	expectedElem int
	falsePositiveRate float64
	m uint
	k uint
	ts uint
	HashFunc []hash.Hash32
	bytes []byte
}

func NewBloom(expectedElem int, falsePositiveRate float64) BloomFilter {
	m := CalculateM(expectedElem, falsePositiveRate)
	k := CalculateK(expectedElem, m)
	//if ts == 0 {
		ts := uint(time.Now().Unix())
	//}
	hashFuncs := CreateHashFunctions(k, ts)
	bytes := make([]byte, m)
	return BloomFilter{expectedElem, falsePositiveRate, m, k, ts, hashFuncs, bytes}
}

func (bloom *BloomFilter) Add(item string) {
	for i := uint(0); i < bloom.k; i++ {
		bloom.HashFunc[i].Reset()
		bloom.HashFunc[i].Write([]byte(item))
		j := bloom.HashFunc[i].Sum32() % uint32(bloom.m)
		bloom.bytes[j] = 1
	}
}

func (bloom *BloomFilter) FindElem(item string) bool{
	for i := uint(0); i < bloom.k; i++ {
		bloom.HashFunc[i].Reset()
		bloom.HashFunc[i].Write([]byte(item))
		j := bloom.HashFunc[i].Sum32() % uint32(bloom.m)
		if bloom.bytes[j] == 0 {
			return false
		}
	}
	return true
}

func (bloom *BloomFilter) Serialize() {
	file, err := os.OpenFile("Data/bloom.db", os.O_WRONLY|os.O_CREATE, 0777)
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

func Deserialize() BloomFilter {
	file, err := os.OpenFile("Data/bloom.db", os.O_RDWR, 0777)
	if err != nil {
		panic(err)
	}

	bloom := new(BloomFilter)
	//bloom := new(BloomFilter)
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&bloom)
	if err != nil {
		panic(err)
	}
	hashFuncs := CreateHashFunctions(bloom.k, bloom.ts)
	bloom.HashFunc = hashFuncs

	file.Close()

	return *bloom
}

func main() {
	bloom := NewBloom(10, 2)
	//fmt.Println(bloom.bytes)
	//Add(bloom, "m")
	//fmt.Println(bloom.bytes)
	/*for i := uint(0); i < bloom.k; i++ {
		fmt.Println(bloom.HashFunc[i])
		fmt.Println(bloom.HashFunc[i].Sum32())
		fmt.Println(bloom.HashFunc[i].Sum32() % uint32(bloom.m))
		fmt.Println("======================================")
		//position := bloom.HashFunc[](item) % bloom.m
	}*/
	bloom.Add("mark")
	fmt.Println(bloom.bytes)
	fmt.Println(bloom.FindElem("mark"))
	fmt.Println(bloom.FindElem("po"))
	bloom.Add("po")
	fmt.Println(bloom.bytes)
	bloom.Serialize()
	bloom2 := Deserialize()
	fmt.Println(bloom2)
	//fmt.Println(bloom.FindElem("mark"))
	//fmt.Println(bloom.FindElem("po"))

}
