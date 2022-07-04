package Memtable

import (
	"NASP/Structures/SkipList"
	"fmt"
)

const (
	maxLevel = 15
)

type Memtable struct {
	Skiplist    *SkipList.SkipList // podaci
	threshold   int                // maksimalni kapacitet(u bajtovima) tj. prag zapisa (kad se dosegne, vrsi se flush)
	currSize    int                // trenutni kapacitet tj. broj uskladistenih bajtova
	numElements int                // trenutni broj elemenata
	maxElements int                // maksimalan broj elemenata (kad se dosegne, vrsi se flush)
}

func NewMemtable(maxLevel int, threshold int, maxElem int) *Memtable {
	return &Memtable{
		Skiplist:    SkipList.NewSkipList(maxLevel),
		threshold:   threshold,
		currSize:    0,
		numElements: 0,
		maxElements: maxElem,
	}
}

func (mt *Memtable) NumElements() int {
	return mt.numElements
}

func (mt *Memtable) MaxElements() int {
	return mt.maxElements
}

func (mt *Memtable) CurrentSize() int {
	return mt.currSize
}

func (mt *Memtable) IsReadyToFlush() bool {
	if mt.threshold <= mt.currSize || mt.numElements >= mt.maxElements {
		return true
	} else {
		return false
	}
}

func (mt *Memtable) Insert(key string, value []byte) bool {
	node := mt.Find(key)
	if node != nil {
		fmt.Println("Element vec postoji. Dodavanje nije moguce.")
		return false
	}
	dataSize := len(key) + len(value)
	if mt.threshold >= mt.currSize+dataSize && mt.numElements < mt.maxElements {
		mt.currSize += dataSize
		mt.numElements += 1
		mt.Skiplist.Insert(key, value, false)
		return true
	}
	fmt.Println("Memtable je dostigla makismalni kapacitet, potrebno je pozvati Flush metodu.")
	return false
}

func (mt *Memtable) insertDeleted(key string, value []byte) bool {
	dataSize := len(key) + len(value)
	if mt.threshold >= mt.currSize+dataSize && mt.numElements < mt.maxElements {
		mt.currSize += dataSize
		mt.numElements += 1
		mt.Skiplist.Insert(key, value, true)
		return true
	}
	fmt.Println("Memtable je dostigla makismalni kapacitet, potrebno je pozvati Flush metodu.")
	return false
}

func (mt *Memtable) Find(key string) *SkipList.Node {
	return mt.Skiplist.Find(key)
}

func (mt *Memtable) FindDeleted(key string) *SkipList.Node {
	return mt.Skiplist.FindDeleted(key)
}

func (mt *Memtable) Delete(key string) {
	nodeToDelete := mt.Find(key)
	if nodeToDelete == nil {
		mt.insertDeleted(key, []byte(""))
		return
	}
	mt.Skiplist.RemoveLog(key)
// 	mt.currSize = mt.currSize - len(nodeToDelete.Key()) - len(nodeToDelete.Value())
// 	mt.numElements -= 1
	return
}

func (mt *Memtable) Modify(key string, value []byte) bool {
	node := mt.Find(key)
	if node == nil {
		fmt.Println("Element ne postoji u strukturi. Izmjena nije moguca.")
		return false
	}
	oldSize := len(node.Key()) + len(node.Value())
	mt.currSize -= oldSize
	mt.Skiplist.Insert(key, value, false)
	mt.currSize += len(key) + len(value)
	return true
}

func (mt *Memtable) Empty() {
	mt.currSize = 0
	mt.numElements = 0
	mt.Skiplist.Empty()
}

func (mt *Memtable) PrintMt() {
	fmt.Println("Threshold", mt.threshold)
	fmt.Println("Current size of Memtable: ", mt.currSize)
	fmt.Println("Number of elements: ", mt.numElements)
	mt.Skiplist.PrintSL()
}

func test() {
	mt := NewMemtable(maxLevel, 100, 20)
	mt.Insert("1", []byte("pozdrav1"))
	mt.Insert("2", []byte("pozdrav2"))
	mt.Insert("4", []byte("pozdrav4"))
	mt.Insert("6", []byte("pozdrav6"))
	mt.Insert("5", []byte("pozdrav5"))
	mt.Insert("3", []byte("pozdrav3"))

	node := mt.Find("2")
	fmt.Printf(string(node.Value()) + "\n")

	mt.Delete("6")
	mt.PrintMt()

	mt.Delete("2")
	fmt.Println(" ")
	mt.PrintMt()

	mt.Modify("2", []byte("hehehe"))
	mt.Modify("5", []byte("hehehe5"))
	node = mt.Find("5")
	fmt.Printf(string(node.Value()) + "\n")

	fmt.Println(" ")
	mt.PrintMt()
}
