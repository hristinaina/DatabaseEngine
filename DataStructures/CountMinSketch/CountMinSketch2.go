package CountMinSketch

// ovo je moja verzija
import (
	"bytes"
	"encoding/gob"
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

func CreateHashFunctions(k uint, ts uint) []hash.Hash32 {
	h := []hash.Hash32{}
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(ts+i)))
	}
	return h
}

type CountMinSketch struct {
	Epsilon       float64
	Delta         float64
	Matrix        [][]uint
	M             uint
	K             uint
	Ts            uint
	hashFunctions []hash.Hash32
}

func NewCountMinSketch(epsilon, delta float64) *CountMinSketch {
	m := CalculateM(epsilon)
	k := CalculateK(delta)
	ts := uint(time.Now().Unix())
	hashes := CreateHashFunctions(k, ts)
	matrix := make([][]uint, k)
	for i := range matrix {
		matrix[i] = make([]uint, m)
	}
	return &CountMinSketch{Epsilon: epsilon, Delta: delta, Matrix: matrix, M: m, K: k, Ts: ts, hashFunctions: hashes}
}

func (countMinSketch *CountMinSketch) AddItem(item string) {
	for j := 0; j < int(countMinSketch.K); j++ {
		countMinSketch.hashFunctions[j].Reset()
		countMinSketch.hashFunctions[j].Write([]byte(item))
		i := countMinSketch.hashFunctions[j].Sum32() % uint32(countMinSketch.M)
		countMinSketch.Matrix[j][i] += 1
	}
}

func (countMinSketch *CountMinSketch) FrequencyOfElement(element string) uint {

	a := make([]uint, countMinSketch.K, countMinSketch.K)
	for j := 0; j < int(countMinSketch.K); j++ {
		countMinSketch.hashFunctions[j].Reset()
		countMinSketch.hashFunctions[j].Write([]byte(element))
		i := countMinSketch.hashFunctions[j].Sum32() % uint32(countMinSketch.M)
		a[j] = countMinSketch.Matrix[j][i]
	}

	min := a[0]
	for k := 1; k < len(a); k++ {
		if a[k] < min {
			min = a[k]
		}
	}

	return min

}

func (CMS *CountMinSketch) Encode() []byte {
	// Encode
	encoded := bytes.Buffer{}
	encoder := gob.NewEncoder(&encoded)
	err := encoder.Encode(CMS)
	if err != nil {
		fmt.Print(err.Error())
	}
	return encoded.Bytes()
}

func (CMS *CountMinSketch) Decode(data []byte) {
	// Decode
	encoded := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(encoded)
	err := decoder.Decode(CMS)
	if err != nil {
		fmt.Print(err.Error())
	}
	CMS.hashFunctions = CreateHashFunctions(CMS.K, CMS.Ts)
}

func test() {
	countMinSketch := NewCountMinSketch(0.01, 0.01)
	fmt.Println(countMinSketch.Matrix)
	countMinSketch.AddItem("ananas")
	fmt.Println(countMinSketch.Matrix)
	countMinSketch.AddItem("ananas")
	fmt.Println(countMinSketch.Matrix)
	fmt.Println(countMinSketch.FrequencyOfElement("ananas"))
	fmt.Println(countMinSketch.FrequencyOfElement("nema"))
}
