package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

type MerkleRoot struct {
	Root *NodeMerkle
}

func (mr *MerkleRoot) String() string{
	return mr.Root.String()
}

type Node struct {
	key       string
	value     []byte
	timestamp int64
	tombstone bool
	next      []*Node
}

type NodeMerkle struct {
	value []byte
	left  *NodeMerkle
	right *NodeMerkle
}

func (n *NodeMerkle) String() string {
	return hex.EncodeToString(n.value[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

// Serialization main func for file witting
func (n *NodeMerkle) Serialization(){
	file, err := os.OpenFile("Data/metadata1.txt", os.O_WRONLY|os.O_CREATE, 0777)
	//file, err := os.OpenFile("Data/proba.db", os.O_WRONLY|os.O_CREATE, 0777)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)
	if err != nil {
		panic(err)
	}
	n.PreorderSerialisation(file)
}

// PreorderSerialisation helper func for file witting
func (n *NodeMerkle) PreorderSerialisation(file *os.File) {
	fmt.Println(n.value)
	file.Write([]byte(n.String()))
	file.Write([]byte(";"))
	if n.left != nil {
		n.left.PreorderSerialisation(file)
	}
	if n.right != nil {
		n.right.PreorderSerialisation(file)
	}

	//if not self.is_empty():
	//for c in x.children:
	//  self.postorder(c)
	//  print(x.value)
}

func Deserialization()  {
	//file, err := os.OpenFile("Data/proba.db", os.O_RDONLY|os.O_CREATE, 0777)
	file, err := os.OpenFile("Data/metadata1.txt", os.O_RDONLY|os.O_CREATE, 0777)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)
	if err != nil {
		panic(err)
	}
	line, err := ioutil.ReadAll(file)
	nodes := strings.Split(string(line), ";")
	nodes = nodes[:len(nodes) - 1]  // zato sto se fajl zavrsava sa ; pa je poslednji elem prazan
	//fmt.Println(nodes)
}


// MakeNodes func that makes the upper levels of the three
func MakeNodes(parts []NodeMerkle) []NodeMerkle {
	next_gen := []NodeMerkle{}
	if len(parts) % 2 == 1{
		parts = append(parts, NodeMerkle{value: []byte("")})
	}
	counter := 0 //da bi znali dokle smo stigli
	for len(parts) > counter {
		currentParents := parts[counter:counter + 2]
		left := currentParents[0]
		right := currentParents[1]
		childrenVal := append(left.value[:], right.value[:]...)
		hashVal := Hash(childrenVal)
		if len(right.value) == 0{
			next_gen = append(next_gen, NodeMerkle{value: hashVal[:], left: &left, right: nil})
		}else {next_gen = append(next_gen, NodeMerkle{value: hashVal[:], left: &left, right: &right})}
		counter += 2
	}
	if len(next_gen) == 1 {
		return next_gen
	}else{
		return MakeNodes(next_gen)
	}
}

func NewMerkleTree(parts []NodeMerkle) *MerkleRoot {
	elems := MakeNodes(parts)
	return &MerkleRoot{Root: &elems[0]}
}

// MakeNodesForMerkle converting nodes from skip list to the new format for merkle
func MakeNodesForMerkle(nodes []Node) []NodeMerkle {
	merkleNodes := []NodeMerkle{}
	for i := 0; i < len(nodes); i++ {
		merkleNodes = append(merkleNodes, NodeMerkle{value: nodes[i].value, left: nil, right: nil})
	}
	return merkleNodes
}

func main() {
	fmt.Println([]byte(""))
	nodes := []Node{
		{key: "", value: []byte("a"), timestamp: 0, tombstone: false, next: nil},
		{key: "", value: []byte("a"), timestamp: 0, tombstone: false, next: nil},
		{key: "", value: []byte("a"), timestamp: 0, tombstone: false, next: nil},
		{key: "", value: []byte("a"), timestamp: 0, tombstone: false, next: nil},
		{key: "", value: []byte("a"), timestamp: 0, tombstone: false, next: nil},
	}
	newNodes := MakeNodesForMerkle(nodes)

	r := NewMerkleTree(newNodes)
	//fmt.Println(r.Root.value)
	//fmt.Println(r.Root.left.value)
	//fmt.Println(r.Root.right.value)
	r.Root.Serialization()
	Deserialization()
}


