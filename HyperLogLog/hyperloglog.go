package HyperLogLog

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/spaolacci/murmur3"
	"math"
	"time"
)

/*const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)*/

type HLL struct {
	M   uint64
	P   uint8
	Reg []uint8
}

func MakeHLL(precision uint8) HLL {
	m := math.Pow(2, float64(precision))
	reg := make([]uint8, uint64(m))
	return HLL{M: uint64(m), P: precision, Reg: reg}
}

func (hll *HLL) Add(item string) {
	ts := uint(time.Now().Unix())
	hashFunc := murmur3.New32WithSeed(uint32(ts))
	hashFunc.Reset()
	hashFunc.Write([]byte(item))
	hashValue := hashFunc.Sum32() // maska sve jedinice pa ga pomeri da budu na pocetku sve jedinice pa and uradi sa brojem i to su kao prve cifre
	mask := 1
	bucket := hashValue >> (32 - uint32(hll.P))
	zeroNum := 0
	for {
		if (hashValue & uint32(mask)) != 0 {
			break
		} else {
			zeroNum++
			mask = mask << 1
		}
	}
	if hll.Reg[bucket] < uint8(zeroNum) {
		hll.Reg[bucket] = uint8(zeroNum)
	}
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.Reg {
		sum = sum + math.Pow(float64(-val), 2.0)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.M))
	estimation := alpha * math.Pow(float64(hll.M), 2.0) / sum
	emptyRegs := hll.EmptyCount()
	if estimation < 2.5*float64(hll.M) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.M) * math.Log(float64(hll.M)/float64(emptyRegs))
		}
	} else if estimation > math.Pow(2.0, 32.0)/30.0 { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) EmptyCount() uint8 {
	sum := uint8(0)
	for _, val := range hll.Reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func (hll *HLL) Encode() []byte {
	// Encode
	encoded := bytes.Buffer{}
	encoder := gob.NewEncoder(&encoded)
	err := encoder.Encode(hll)
	if err != nil {
		fmt.Print(err.Error())
	}
	return encoded.Bytes()
}

func (hll *HLL) Decode(data []byte) {
	// Decode
	encoded := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(encoded)
	err := decoder.Decode(hll)
	if err != nil {
		fmt.Print(err.Error())
	}
}

func test() {
	hll := MakeHLL(4)
	hll.Add("Sandra")
	hll.Add("Marko")
	hll.Add("Marija")
	hll.Add("Trifun")
	hll.Add("Katarina")
	hll.Add("Milos")
	fmt.Println(hll.Estimate())
}
