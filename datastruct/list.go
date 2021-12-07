package datastruct

type List struct {
	First, Last *Node
	Size        int
}

type Node struct {
	Val        interface{}
	Prev, Next *Node
}

func (list *List) Len() int {
	if list == nil {
		panic("List is nil")
	}
	return list.Size
}

func (list *List) Add(val interface{}) {
	if list == nil {
		panic("List is nil")
	}
	node := &Node{
		Val: val,
	}
	// Judge whether the linked list is empty.
	if list.Last == nil {
		list.First = node
		list.Last = node
	} else {
		node.Prev = list.Last
		list.Last.Next = node
		list.Last = node
	}
	list.Size++
}

func (list *List) RemoveNode(node *Node) {
	if node.Prev == nil {
		list.First = node.Next
	} else {
		node.Prev.Next = node.Next
	}
	if node.Next == nil {
		list.Last = node.Prev
	} else {
		node.Next.Prev = node.Prev
	}
	// GC
	node.Prev = nil
	node.Next = nil
	list.Size--
}

func (list *List) Set(index int, val interface{}) {
	if list == nil {
		panic("List is nil")
	}
	if index < 0 || index > list.Size {
		panic("Index out of bound")
	}
	node := list.Find(index)
	node.Val = val
}

func (list *List) Get(index int) interface{} {
	if list == nil {
		panic("List is nil")
	}
	if index < 0 || index > list.Size {
		panic("Index out of bound")
	}
	return list.Find(index).Val
}

func (list *List) Insert(index int, val interface{}) {
	if list == nil {
		panic("List is nil")
	}
	if index < 0 || index > list.Size {
		panic("Index out of bound")
	}
	// Index is last
	if index == list.Size {
		list.Add(val)
		return
	}
	// List is not empty
	pivot := list.Find(index)
	node := &Node{
		Val:  val,
		Prev: pivot.Prev,
		Next: pivot,
	}
	// Index is zero
	if pivot.Prev == nil {
		list.First = node
	} else {
		pivot.Prev.Next = node
	}
	pivot.Prev = node
	list.Size++
}

func (list *List) Remove(index int) interface{} {
	if list == nil {
		panic("List is nil")
	}
	if index < 0 || index > list.Size {
		panic("Index out of bound")
	}
	node := list.Find(index)
	list.RemoveNode(node)
	return node.Val
}

func (list *List) Find(index int) *Node {
	node := list.First
	if index < list.Size/2 {
		for i := 0; i < index; i++ {
			node = node.Next
		}
	} else {
		node = list.Last
		for i := list.Size - 1; i > index; i-- {
			node = node.Prev
		}
	}
	return node
}

func (list *List) RemoveLast() interface{} {
	if list == nil {
		panic("List is nil")
	}
	if list.Last == nil {
		// Empty list
		return nil
	}
	node := list.Last
	list.RemoveNode(node)
	return node.Val
}
