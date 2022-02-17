package main

import (
	"../SkipList"
	"fmt"
)

const (
	maxLevel = 15
)

type Memtable struct {
	skiplist *SkipList.SkipList  // podaci
	threshold int  // maksimalni kapacitet(u bajtovima) tj. prag zapisa (kad se dosegne, vrsi se flush)
	currSize int    // trenutni kapacitet tj. broj uskladistenih bajtova
	numElements int  // trenutni broj elemenata
}

func NewMemtable(maxLevel int, threshold int) *Memtable {
	return &Memtable{
		skiplist: SkipList.NewSkipList(maxLevel),
		threshold: threshold,
		currSize: 0,
		numElements: 0,
	}
}

func (mt *Memtable) NumElements() int {
	return mt.numElements
}

func (mt *Memtable) Insert(key string, value []byte) bool {
	node := mt.Find(key)
	if node != nil{
		fmt.Println("Element vec postoji. Dodavanje nije moguce.")
		return false
	}
	dataSize := len(key) + len(value)
	if mt.threshold >= mt.currSize + dataSize{
		mt.currSize += dataSize
		mt.numElements += 1
		mt.skiplist.Insert(key, value)
		return true
	}
	fmt.Println("Memtable je dostigla makismalni kapacitet, potrebno je pozvati Flush metodu.")
	return false
}

func (mt *Memtable) Find(key string) *SkipList.Node {
	return mt.skiplist.Find(key)
}

func (mt *Memtable) Delete(key string) bool {
	nodeToDelete := mt.Find(key)
	if nodeToDelete == nil{
		fmt.Println("Uklanjanje nepostojeceg elemeneta nije moguce.")
		return false
	}
	mt.skiplist.RemoveLog(key)
	mt.currSize = mt.currSize - len(nodeToDelete.Key()) - len(nodeToDelete.Value())
	mt.numElements -= 1
	return true
}

func (mt *Memtable) Modify(key string, value []byte) bool {
	node := mt.Find(key)
	if node == nil{
		fmt.Println("Element ne postoji u strukturi. Izmjena nije moguca.")
		return false
	}
	oldSize := len(node.Key()) + len(node.Value())
	mt.currSize -= oldSize
	mt.skiplist.Insert(key, value)
	mt.currSize += len(key) + len(value)
	return true
}

func (mt *Memtable) Empty() {
	mt.currSize = 0
	mt.numElements = 0
	mt.skiplist.Empty()
}

func (mt *Memtable) PrintMt() {
	fmt.Println("Threshold",  mt.threshold)
	fmt.Println("Current size of Memtable: ", mt.currSize)
	fmt.Println("Number of elements: ", mt.numElements)
	mt.skiplist.PrintSL()
}

func main() {
	mt := NewMemtable(maxLevel, 100)
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


