package list

type LinkedList interface {
	Len() int
	Add(val interface{})
	RemoveNode(node *Node)
	Set(index int, val interface{})
	Get(index int) interface{}
	Insert(index int, val interface{})
	Find(index int) *Node
}