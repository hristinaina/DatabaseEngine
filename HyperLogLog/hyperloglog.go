package main
import(
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
	m   uint64
	p   uint8
	reg []uint8
}

func MakeHLL(precision uint8) HLL {
	m := math.Pow(2, float64(precision))
	reg := make([]uint8, uint64(m))
	return HLL{m: uint64(m), p: precision, reg: reg}
}

func (hll *HLL) add(item string) {
	ts := uint(time.Now().Unix())
	hashFunc := murmur3.New32WithSeed(uint32(ts))
	hashFunc.Reset()
	hashFunc.Write([]byte(item))
	hashValue := hashFunc.Sum32()  // maska sve jedinice pa ga pomeri da budu na pocetku sve jedinice pa and uradi sa brojem i to su kao prve cifre
	mask := 1
	bucket := hashValue >> (32 - uint32(hll.p))
	zeroNum := 0
	for {
		if (hashValue & uint32(mask)) != 0 {
			break
		} else {
			zeroNum ++
			mask = mask << 1
		}
	}
	 if hll.reg[bucket] < uint8(zeroNum) {
		 hll.reg[bucket] = uint8(zeroNum)
	 }
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum = sum + math.Pow(float64(-val), 2.0)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation < 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > math.Pow(2.0, 32.0)/30.0 { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() uint8 {
	sum := uint8(0)
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}


func main() {
	hll := MakeHLL(4)
	hll.add("Sandra")
	hll.add("Marko")
	hll.add("Marija")
	hll.add("Trifun")
	hll.add("Katarina")
	hll.add("Milos")
	fmt.Println(hll.Estimate())
}
