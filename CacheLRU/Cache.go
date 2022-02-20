package CacheLRU

import (
	"fmt"
)

// --- [ Doubly linked list ] ---
type Node struct {
	key      string
	value    []byte
	next     *Node
	previous *Node
}
type DLList struct {
	newest *Node
	oldest *Node
}

// Private Functions
func (list *DLList) _findNode(key string) (*Node, bool) {
	curNode := list.newest
	var node *Node
	for curNode != nil {
		if curNode.key == key {
			node = curNode
			break
		}
		curNode = curNode.next
	}
	if node != nil {
		return node, true
	}
	return nil, false
}
func (list *DLList) _unlinkNode(node *Node) {
	if node.next != nil {
		node.next.previous = node.previous
	} else {
		list.oldest = node.previous
	}
	if node.previous != nil {
		node.previous.next = node.next
	} else {
		list.newest = node.next
	}
}

// Public Functions
func GenerateNode(key string, value []byte) Node {
	node := Node{}
	node.key = key
	node.value = value
	node.next = nil
	node.previous = nil
	return node
}
func GenerateDLList() DLList {
	list := DLList{}
	list.newest = nil
	list.oldest = nil
	return list
}
func (list *DLList) printElements() {
	fmt.Println("List:")
	node := list.newest
	fmt.Println("HEAD Key: " + list.newest.key)
	for node != nil {
		if node.previous == nil {
			fmt.Println("Key: " + node.key + " | next: " + node.next.key)
		} else if node.next == nil {
			fmt.Println("Key: " + node.key + " | prev: " + node.previous.key)
		} else {
			fmt.Println("Key: " + node.key + " | next: " + node.next.key + " | prev: " + node.previous.key)
		}
		node = node.next
	}
	fmt.Println("TAIL Key: " + list.oldest.key)

}
func (list *DLList) moveToNewest(key string) {
	// Locate existing node & unlink from the list
	node, _ := list._findNode(key)
	list._unlinkNode(node)
	// Link found node to the front
	node.previous = nil
	node.next = list.newest
	list.newest.previous = node
	list.newest = node
}
func (list *DLList) removeOldest() {
	list.oldest = list.oldest.previous
	list.oldest.next = nil
}
func (list *DLList) InsertElement(key string, value []byte) {
	node := GenerateNode(key, value)
	if list.newest != nil {
		node.next = list.newest
		list.newest.previous = &node
		list.newest = &node
	} else {
		list.newest = &node
		return
	}
	if list.oldest == nil {
		list.oldest = list.newest.next
	}
}
func (list *DLList) removeElement(key string) {
	// Locate node & unlink from the list
	node, _ := list._findNode(key)
	list._unlinkNode(node)
}

// --- [ Cache ] ---
type Cache struct {
	maxSegments uint32
	curSegments uint32
	list        DLList
	hashMap     map[string][]byte
}

func GenerateCache(maxSeg uint32) *Cache {
	cache := Cache{}
	cache.maxSegments = maxSeg
	cache.list = GenerateDLList()
	cache.hashMap = make(map[string][]byte, cache.maxSegments)
	return &cache
}
func (cache *Cache) InsertElement(key string, value []byte) bool {
	_, exist := cache.hashMap[key]
	if exist && cache.curSegments > 1 {
		// Move to the newest place
		cache.list.moveToNewest(key)
	} else {
		// Insert new element
		cache.hashMap[key] = value
		cache.curSegments++
		cache.list.InsertElement(key, value)
	}
	// Check for overflow &/| remove oldest
	if cache.curSegments > cache.maxSegments {
		print("REMOVE LAST\n")
		oldestKey := cache.list.oldest.key
		delete(cache.hashMap, oldestKey)
		cache.curSegments--
		cache.list.removeOldest()
	}
	return true
}
func (cache *Cache) RemoveElement(key string) bool {
	_, exist := cache.hashMap[key]
	if exist {
		delete(cache.hashMap, key)
		cache.curSegments--
		cache.list.removeElement(key)
		return true
	}
	return false
}
func (cache *Cache) GetElement(key string) (bool, []byte) {
	value, exist := cache.hashMap[key]
	if exist {
		cache.list.moveToNewest(key)
		return true, value
	}
	return false, nil
}

//TEST
func test() {
	cache := GenerateCache(5)
	cache.InsertElement("1", []byte("Test1"))
	cache.InsertElement("2", []byte("Test2"))
	cache.InsertElement("3", []byte("Test3"))
	cache.InsertElement("4", []byte("Test4"))
	cache.InsertElement("5", []byte("Test5"))
	cache.InsertElement("6", []byte("Test6"))
	fmt.Println("\nInsert data 1 trough 6")
	cache.list.printElements()

	fmt.Println("\nInsert data 3")
	cache.InsertElement("3", []byte("Test3"))
	cache.list.printElements()

	fmt.Println("\nGet data 5")
	fmt.Println(cache.GetElement("5"))
	cache.list.printElements()

	fmt.Println("\nRemove data 40, 4")
	cache.RemoveElement("40")
	cache.RemoveElement("4")
	cache.list.printElements()

	fmt.Println("\nRemove data 2")
	cache.RemoveElement("2")
	cache.list.printElements()

	fmt.Println("\nInsert data 1")
	cache.InsertElement("1", []byte("Test1"))
	cache.list.printElements()

	fmt.Println("\nInsert data 6")
	cache.InsertElement("6", []byte("Test6"))
	cache.list.printElements()

	fmt.Println("\nRemove data 6")
	cache.RemoveElement("6")
	cache.list.printElements()
}
