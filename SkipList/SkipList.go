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

func newNode(key string, value []byte, level int, timestamp int64) *Node {
	return &Node{
		key:       key,
		value:     value,
		timestamp: timestamp,
		tombstone: false,
		next:      make([]*Node, level),
	}
}

func newHead(level int) *Node {
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

func newSkipList(maxLevel int) *SkipList {
	header := newHead(maxLevel+1)
	return &SkipList{
		head: header,
		maxLevel: maxLevel,
		level: 1,
		len: 0,
	}
}

func (sl *SkipList) length() int {
	return sl.len
}

func (sl *SkipList) randomLevels() int {
	lvl := 1
	rand.Seed(time.Now().UnixNano())
	// vraca int32 u opsegu [0,2) tj ili 0 ili 1
	for ; rand.Int31n(2) == 1; lvl++ {
	}
	if lvl > sl.level {
		sl.level = sl.level+1
		lvl = sl.level
	}
	return lvl

	/*for ; rand.Int31n(2) == 1; level++ {
		if level > sl.level {
			sl.level = level
			fmt.Println(sl.level)
			fmt.Printf("konacan level insertovanog cvora:")
			fmt.Println(level)
			return level
		}
	}
	fmt.Println(sl.level)
	fmt.Printf("konacan level insertovanog cvora:")
	fmt.Println(level)
	return level*/
}

func (sl *SkipList) find(key string) *Node{
	curr := sl.head
	for i := sl.level; i >= 0; i-- {
		for ; curr.next[i] != nil; curr = curr.next[i] {
			if curr.next[i].key > key {
				break
			} else if curr.next[i].key == key{
				return curr.next[i]
			}
		}
	}
	return nil
}

func (sl *SkipList) contains(key string) bool {
	return sl.find(key) != nil
}

func (sl *SkipList) insert(key string, value []byte) bool {
	node := sl.find(key)
	// ako node postoji bilo da je logicki obrisan ili ne - vrsi se AZURIRANJE
	if node != nil{
		if node.tombstone == true{  // ako je bio logicki obrisan
			sl.len ++
		}
		node.tombstone = false
		now := time.Now()
		node.timestamp = now.Unix()
		node.value = value
		return true
	}
	// ako node ne postoji u skip listi, vrsi se dodavanje
	lvl := sl.randomLevels()
	fmt.Println(lvl)
	now := time.Now()
	timestamp := now.Unix()
	node = newNode(key, value, lvl+1, timestamp)

	// na svakom nivou treba prepaviti pokazivace (da prethodni ukazuje na node i node na sljedeci)
	previous := sl.getPrevious(key, lvl)
	for i := 0; i < lvl; i++ {
		node.next[i] = previous[i].next[i]
		previous[i].next[i] = node
	}
	sl.len++
	return true
}

// funckija dobavlja sve cvorove (na svim nivoima) koji su neposredno prije proslijedjenog
func (sl *SkipList) getPrevious(key string, lvl int) []*Node {
	previous := make([]*Node, lvl+1)
	curr := sl.head

	for i := sl.level; i >= 0; i-- {
		for ; curr.next[i] != nil; curr = curr.next[i] {
			if curr.next[i].key >= key {
				break
			}
		}
		if i <= lvl {
			previous[i] = curr
		}
	}

	return previous
}

// fizicko brisanje - element se zaprave brise iz skipListe
func (sl *SkipList) removePh(key string) bool {
	node := sl.find(key)
	if node == nil {
		fmt.Println("Brisanje elementa nije moguce jer ne postoji u listi.")
		return false
	}
	previous := sl.getPrevious(key, len(node.next))
	for i := len(previous)-2; i >= 0; i-- {
		if sl.head.next[i] == nil{
			// jer treba ukloniti suvisne nivoe
			sl.level = sl.level - 1
		}else {
			previous[i].next[i] = node.next[i]
		}
	}
	sl.len--
	return true
}

// logicko brisanje - samo se mijenja tombstone
func (sl *SkipList) removeLog(key string) bool {
	node := sl.find(key)
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

func (sl *SkipList) empty() {
	sl.head = newHead(sl.maxLevel)
	sl.level = 1
	sl.len = 0
}

func (sl *SkipList) printSL() {
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
	sl := newSkipList(10)
	sl.insert("1", []byte("pozdrav1"))
	sl.insert("2", []byte("pozdrav2"))
	sl.insert("4", []byte("pozdrav4"))
	sl.insert("6", []byte("pozdrav6"))
	sl.insert("5", []byte("pozdrav5"))
	sl.insert("3", []byte("pozdrav3"))
	sl.printSL()

	node := sl.find("2")
	fmt.Printf(string(node.value) + "\n")

	sl.removePh("6")
	sl.printSL()

	sl.removeLog("2")
	fmt.Println(" ")
	sl.printSL()

	fmt.Println(" ")
	sl.removePh("4")
	sl.printSL()
}
