package main

// ovo je moja verzija
import (
	"fmt"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"time"
)

func CalculateM(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

func CalculateK(delta float64) uint {
	return uint(math.Ceil(math.Log(math.E / delta)))
}

func CreateHashFunctions(k uint) []hash.Hash32 {
	h := []hash.Hash32{}
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(ts+i)))
	}
	return h
}

type CountMinSketch struct {
	epsilon float64
	delta float64
	matrix [][]uint
	m uint
	k uint
	hashFunctions []hash.Hash32
}

func NewCountMinSketch(epsilon, delta float64) *CountMinSketch {
	m := CalculateM(epsilon)
	k := CalculateK(delta)
	hashes := CreateHashFunctions(k)
	matrix := make([][]uint, k)
	for i := range matrix {
		matrix[i] = make([]uint, m)
	}
	return &CountMinSketch{epsilon: epsilon, delta: delta, matrix: matrix, m: m, k: k, hashFunctions: hashes}
}

func (countMinSketch *CountMinSketch) AddItem(item string)  {
	for j := 0; j < int(countMinSketch.k); j++ {
		countMinSketch.hashFunctions[j].Reset()
		countMinSketch.hashFunctions[j].Write([]byte(item))
		i := countMinSketch.hashFunctions[j].Sum32() % uint32(countMinSketch.m)
		countMinSketch.matrix[j][i] += 1
	}
}

func (countMinSketch *CountMinSketch) frequencyOfElement(element string) uint {

	a := make([]uint, countMinSketch.k, countMinSketch.k)
	for j := 0; j < int(countMinSketch.k); j++ {
		countMinSketch.hashFunctions[j].Reset()
		countMinSketch.hashFunctions[j].Write([]byte(element))
		i := countMinSketch.hashFunctions[j].Sum32() % uint32(countMinSketch.m)
		a[j] = countMinSketch.matrix[j][i]
	}

	min := a[0]
	for k := 1; k < len(a); k++ {
		if a[k] < min {
			min = a[k]
		}
	}

	return min

}

func main() {
	countMinSketch := NewCountMinSketch(0.01, 0.01)
	fmt.Println(countMinSketch.matrix)
	countMinSketch.AddItem("ananas")
	fmt.Println(countMinSketch.matrix)
	countMinSketch.AddItem("ananas")
	fmt.Println(countMinSketch.matrix)
	fmt.Println(countMinSketch.frequencyOfElement("ananas"))
	fmt.Println(countMinSketch.frequencyOfElement("nema"))
}