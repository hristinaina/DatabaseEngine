package main

import (
	"fmt"
	"math/rand"
	"time"
)

type Node struct {
	key       string
	value     []byte
	timestamp int64
	tombstone bool
	next      []*Node
}

func NewNode(key string, value []byte, level int, timestamp int64) *Node {
	return &Node{
		key:       key,
		value:     value,
		timestamp: timestamp,
		tombstone: false,
		next:      make([]*Node, level),
	}
}

func NewHead(level int) *Node {
	return &Node{
		key:       "",
		value:     nil,
		timestamp: 0,
		tombstone: false,
		next: make([]*Node, level),
	}
}

func (n *Node) Key() string {
	return n.key
}

func (n *Node) Value() []byte {
	return n.value
}

func (n *Node) Timestamp() int64 {
	return n.timestamp
}

func (n *Node) Tombstone() bool {
	return n.tombstone
}

type SkipList struct {
	head *Node
	maxLevel int
	level int
	len int    // broj elemenata na 0-tom nivou
}

func NewSkipList(maxLevel int) *SkipList {
	header := NewHead(maxLevel+1)
	return &SkipList{
		head: header,
		maxLevel: maxLevel,
		level: 1,
		len: 0,
	}
}

func (sl *SkipList) Length() int {
	return sl.len
}

func (sl *SkipList) RandomLevels() int {
	lvl := 1
	rand.Seed(time.Now().UnixNano())

	for ; rand.Int31n(2) == 1; lvl++ {      // vraca int32 u opsegu [0,2) tj ili 0 ili 1
	}
	if lvl > sl.level {
		sl.level = sl.level+1
		lvl = sl.level
	}
	if lvl > sl.maxLevel {
		sl.level = sl.maxLevel
		lvl = sl.level
	}
	return lvl
}

func (sl *SkipList) Find(key string) *Node{
	curr := sl.head
	for i := sl.level; i >= 0; i-- {
		for ; curr.next[i] != nil; curr = curr.next[i] {
			if curr.next[i].key > key {
				break
			} else if curr.next[i].key == key && curr.next[i].tombstone == false{
				return curr.next[i]
			}
		}
	}
	return nil
}

func (sl *SkipList) Contains(key string) bool {
	return sl.Find(key) != nil
}

func (sl *SkipList) Insert(key string, value []byte) bool {
	node := sl.Find(key)
	// ako node postoji u skip listi, vrsi se AZURIRANJE
	if node != nil{
// 		if node.tombstone == true{  // ako je bio logicki obrisan
// 			sl.len ++
// 		}
		node.tombstone = false
		now := time.Now()
		node.timestamp = now.Unix()
		node.value = value
		return true
	}
	// ako node ne postoji u skip listi, vrsi se dodavanje
	lvl := sl.RandomLevels()
	now := time.Now()
	timestamp := now.Unix()
	node = NewNode(key, value, lvl+1, timestamp)

	// na svakom nivou treba prepaviti pokazivace (da prethodni ukazuje na node i node na sljedeci)
	previous := sl.GetPrevious(key, lvl)
	for i := 0; i < lvl; i++ {
		node.next[i] = previous[i].next[i]
		previous[i].next[i] = node
	}
	sl.len++
	return true
}

// funckija dobavlja sve cvorove (na svim nivoima) koji su neposredno prije proslijedjenog
func (sl *SkipList) GetPrevious(key string, lvl int) []*Node {
	previous := make([]*Node, lvl+1)
	curr := sl.head

	for i := sl.level; i >= 0; i-- {
		for ; curr.next[i] != nil; curr = curr.next[i] {
			if curr.next[i].key >= key{
				break
			}
		}
		if i <= lvl {
			previous[i] = curr
		}
	}

	return previous
}

// fizicko brisanje - element se uklanja iz skipListe
func (sl *SkipList) RemovePh(key string) bool {
	node := sl.Find(key)
	if node == nil {
		fmt.Println("Brisanje elementa nije moguce jer ne postoji u listi.")
		return false
	}
	previous := sl.GetPrevious(key, len(node.next))
	for i := len(previous)-2; i >= 0; i-- {
		if sl.head.next[i] == nil{
			// treba ukloniti suvisne nivoe
			sl.level = sl.level - 1
		}else {
			previous[i].next[i] = node.next[i]
		}
	}
	sl.len--
	return true
}

// logicko brisanje - samo se mijenja tombstone
func (sl *SkipList) RemoveLog(key string) bool {
	node := sl.Find(key)
	if node == nil {
		fmt.Println("Brisanje elementa nije moguce jer ne postoji u listi.")
		return false
	}
	if node.tombstone == false {
		node.tombstone = true
		now := time.Now()
		node.timestamp = now.Unix()
	}
	sl.len--
	return true
}

func (sl *SkipList) Empty() {
	sl.head = NewHead(sl.maxLevel)
	sl.level = 1
	sl.len = 0
}

func (sl *SkipList) PrintSL() {
	// ne ispisuje logicki obrisane
	for i := sl.level; i >= 0; i-- {
		curr := sl.head
		fmt.Print("[")
		for curr.next[i] != nil{
			if curr.next[i].tombstone == false{
				fmt.Print(curr.next[i].key + ", ")
			}
			curr = curr.next[i]
		}
		fmt.Print("]\n")
	}
}

func main() {
	sl := NewSkipList(10)
	sl.Insert("1", []byte("pozdrav1"))
	sl.Insert("2", []byte("pozdrav2"))
	sl.Insert("4", []byte("pozdrav4"))
	sl.Insert("6", []byte("pozdrav6"))
	sl.Insert("5", []byte("pozdrav5"))
	sl.Insert("3", []byte("pozdrav3"))
	sl.PrintSL()

	node := sl.Find("2")
	fmt.Printf(string(node.value) + "\n")

	sl.RemovePh("6")
	sl.PrintSL()

	sl.RemoveLog("2")
	fmt.Println(" ")
	sl.PrintSL()

	node = sl.Find("2")
	fmt.Println(node)

	sl.RemoveLog("3")

	sl.Insert("2", []byte("poyyy"))
	sl.PrintSL()

	fmt.Println(" ")
	sl.RemovePh("4")
	sl.PrintSL()
}
