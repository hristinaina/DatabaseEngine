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
	root *Node
}

func (mr *MerkleRoot) String() string{
	return mr.root.String()
}

type Node struct {
	data []byte
	left *Node
	right *Node
}

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

func (n *Node) Serialization(){
	file, err := os.OpenFile("Data/metadata.txt", os.O_WRONLY|os.O_CREATE, 0777)
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

func (n *Node) PreorderSerialisation(file *os.File) {
	fmt.Println(n.data)
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
	//  print(x.data)
}

func Deserialization()  {
	//file, err := os.OpenFile("Data/proba.db", os.O_RDONLY|os.O_CREATE, 0777)
	file, err := os.OpenFile("Data/metadata.txt", os.O_RDONLY|os.O_CREATE, 0777)
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

func MakeNodes(parts []Node) []Node {
	next_gen := []Node{}
	if len(parts) % 2 == 1{
		parts = append(parts, Node{data: []byte("")})
	}
	counter := 0 //da bi znali dokle smo stigli
	for len(parts) > counter {
		currentParents := parts[counter:counter + 2]
		left := currentParents[0]
		right := currentParents[1]
		childrenVal := append(left.data[:], right.data[:]...)
		hashVal := Hash(childrenVal)
		if len(right.data) == 0{
			next_gen = append(next_gen, Node{data: hashVal[:], left: &left, right: nil})
		}else {next_gen = append(next_gen, Node{data: hashVal[:], left: &left, right: &right})}
		counter += 2
	}
	if len(next_gen) == 1 {
		return next_gen
	}else{
		return MakeNodes(next_gen)
	}
}

func NewMerkleTree(parts []Node) *MerkleRoot {
	elems := MakeNodes(parts)
	return &MerkleRoot{root: &elems[0]}
}

func main() {
	fmt.Println([]byte(""))
	nodes := []Node{
		{data: []byte("a"), left: nil, right: nil},
		{data:[]byte("b"), left: nil, right: nil},
		{data:[]byte("c"), left: nil, right: nil},
		{data:[]byte("d"), left: nil, right: nil},
		{data:[]byte("e"), left: nil, right: nil},
	}

	r := NewMerkleTree(nodes)
	fmt.Println(r.root.data)
	fmt.Println(r.root.left.data)
	fmt.Println(r.root.right.data)
	r.root.Serialization()
	Deserialization()
}
