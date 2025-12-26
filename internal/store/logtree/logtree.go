package logtree

import (
	"github.com/hastyy/murakami/internal/assert"
)

// LogTree nodes are stored in a tree structure.
// Each node's key is the lexicographically next key in the tree.
// The sum of keys from root to leaf is an originally inserted key.
// Insertion keys have a fixed length. (e.g. a timestamp is a 13 byte string).
// Keys are ASCII strings, meaning they can be compared byte by byte.
//
// Nodes have either children or values, but not both.
// Nodes without children are leaf nodes and contain values.
//
// The root node always exists and its key is the empty string.
// It is the only node that can have no children and values.
// Root node will never have values.
// Root node has children if the size of the LogTree is greater than 0.
//
// Nodes with children are non-leaf nodes and contain no values.
// Non-leaf nodes have at least two children. If at some point they have only one child, they will be collapsed into a single node.
// If at some point they cease to have children, they are removed.
//
// Nodes with values are leaf nodes and contain the inserted values.
// Leaf nodes have at most one value. If at some point they have no values, they are removed.
// When a leaf node has more than one child it means that multiple inserts with the same key were made. In this case,
// the index of the value should be used to provide uniqueness and determine order in the values.
// E.g. if we insert two values under the key "1737238769766", then their unique ids are "1737238769766-0" and "1737238769766-1",
// and they are stored under the same key "1737238769766" in the leaf node will value indexes 0 and 1 respectively. The value
// with index 0 was inserted first and the value with index 1 was inserted second.
//
// Nodes also store the length of their subtree, which is the sum of the lengths of their children's subtrees.
// Leaf nodes length is equal to the length of their values slice.
//
// Leaf nodes also track the sequence number of their first value via firstSeqNum.
// This is used to map logical sequence numbers (from IDs like "1000-2") to physical array indices.
// For example, if firstSeqNum=2 and values=[v2, v3, v4], then:
//   - values[0] has logical seqNum=2
//   - values[1] has logical seqNum=3
//   - values[2] has logical seqNum=4
//
// The node invariant is as follows:
// Given node n:
// - len(n.children) > 0 <-> len(n.values) == 0
// - len(n.values) > 0 <-> len(n.children) == 0
type node[T any] struct {
	key         Key
	children    []*node[T]
	values      []T
	firstSeqNum int
	length      int
}

// readStackEntry is used to store the nodes and values that need to be read from the tree.
// It is only used for the LogTree.Read() method.
type readStackEntry[T any] struct {
	node              *node[T]
	remainingChildren []*node[T]
	remainingValues   []T
}

// LogTree is a radix-tree inspired data structure which only adds new values to the rightmost side of the tree in an append-only manner.
// It acts as an in-memory append-only log with extra queriable properties.
// It always has a sentinel root node which never counts towards the length of the tree.
type LogTree[T any] struct {
	root    *node[T]
	lastKey Key
}

func New[T any]() *LogTree[T] {
	return &LogTree[T]{
		root: &node[T]{},
	}
}

// Append adds values to the rightmost side of the tree.
// Keys should be monotonically increasing. Speficilly, the key should be greater OR EQUAL than the previous appended key.
// Append is not thread-safe. There should be no concurrent calls to Append.
func (tree *LogTree[T]) Append(key Key, values []T) {
	assert.OK(key >= tree.lastKey, "key is not greater than or equal to the previous appended key")
	tree.lastKey = key

	assert.OK(tree.root.key == "", "root key is not empty")
	assert.OK(len(tree.root.values) == 0, "root has values")

	initialLength := tree.Length()
	defer func() {
		assert.OK(tree.root.key == "", "root key is not empty")
		assert.OK(len(tree.root.values) == 0, "root has values")
		assert.OK(tree.Length() == initialLength+len(values), "tree length is not incremented correctly")
	}()

	// Starting at the root, we navigate down the tree using the provided key until we
	// either find a leaf node or a node whose key doesn't match the remaining key portion.
	// We always leave this loop with the exact node where we want to append the value from,
	// be it already the correct leaf node, a node that needs to be split, or a node from which
	// we can simply append a new child node.
	// These are the escape conditions:
	// - If the LogTree is empty, we never enter the loop and proceed with root as currNode;
	// - If we navigate down the right-side of the tree and find a leaf node, then it means that
	//   	the key is already in the LogTree and we can append the value to this leaf;
	// - If we navigate down the right-side of the tree and find a node whose key isn't a prefix of
	//   		the remaining key portion, then it means that we need to split the node and append a new child node.
	// - If we navigate down the right-side of the tree and find a node whose key is a prefix of the remaining key portion,
	//  		BUT its rightmost child's key has no overlap with the remaining key portion, then in this case navigating down this
	//  		node would leave us in a place where we can't do anything, and therefore we need to append on the parent (currNode).
	currNode := tree.root
	for len(currNode.children) > 0 && currNode.key.IsPrefixOf(key) {
		rightmostChild := currNode.children[len(currNode.children)-1]
		nextKey := key[len(currNode.key):]

		// We need to check if the rightmost child's key overlaps the remaining key portion.
		// This is because if we don't, we would simply navigate down and then understand
		// the next node key is completely different (in this case it's guaranteed to be strictly
		// smaller, lexicographically speaking) and we wouldn't be able to navigate up again.
		// So this condition is to cover the special case where we would normally navigate down to
		// later understand it's the wrong path, and that we want to append a new child to the parent node,
		// which in the current iteration is still currNode.
		if rightmostChild.key[0] != nextKey[0] {
			break
		}

		currNode.length += len(values) // Increment length as we go down the tree
		currNode = rightmostChild
		key = nextKey
	}

	assert.OK(currNode == tree.root || currNode.key[0] == key[0], "currNode is not the root and currNode.key is not a prefix of key")
	assert.OK(len(key) >= len(currNode.key), "key is shorter than currNode.key")
	assert.OK(key[:len(currNode.key)] >= currNode.key, "key is lexicographically smaller than currNode.key")

	if currNode == tree.root {
		// We only get into this case if either the tree is empty or the rightmost child from root
		// has a completely different key with 0 prefix overlap with the key we want to append.
		// In either case that means that we want to append a new leaf node to root children.
		//
		// NOTE: To have a 0-length prefix overlap can be reduced to having a different 1st character
		// 		in the key.
		currNode.children = append(currNode.children, &node[T]{key: key, values: values, firstSeqNum: 0, length: len(values)})
		currNode.length += len(values) // Increment root's length since we added new values
		return
	}

	if currNode.key == key {
		// We only get into this case if we've navigated the right-side of the tree down into a leaf node.
		// In this case we know the key already existed and we want to append the values to currNode.
		currNode.values = append(currNode.values, values...)
		currNode.length += len(values) // Increment length since we added new values
		return
	}

	// At this point we know that currNode.key is a prefix of key, but not equal, meaning there is a partial overlap.
	// In order to not exist a full overlap, then either one of these is true:
	// - len(currNode.key) < len(key)
	// - given the characters composing currNode.key, we can find a character in the range key[0: len(currNode.key)]
	// 		that is different from the corresponding character in currNode.key.
	var prefixLength int
	for prefixLength < len(currNode.key) && currNode.key[prefixLength] == key[prefixLength] {
		prefixLength++
	}

	if prefixLength == len(currNode.key) {
		// In this case currNode.key is fully contained as a prefix in key.
		// Therefore we want to remove that prefix from key (because it's redundant) and append a new child node to currNode
		// whose key is the remaining part of the key.
		// We also know that in order to get into this branch we had to escape the loop earlier in the case where currNode.key
		// is prefix of key, but the rightmost child's key doesn't overlap the remaining key portion.
		currNode.children = append(currNode.children, &node[T]{key: key[prefixLength:], values: values, firstSeqNum: 0, length: len(values)})
		currNode.length += len(values) // Increment length since we added a new child with values
		return
	}

	// At this point we know that currNode.key and key share a prefix of length prefixLength, but that prefix is not the full
	// length of currNode.key or key.
	prefixKey := currNode.key[:prefixLength]
	suffixKey := currNode.key[prefixLength:]

	// In this case we need to split currNode into two nodes, passing the suffix portion of currNode.key
	// as well as its children or values to a new node which will be its left-most child.
	// We then append a new leaf node to currNode whose key is the remaining part of the key.
	// This will be the right-most child of currNode and the right-most node in the tree.
	// The split node always ends up with two children.
	children := currNode.children
	currNode.children = make([]*node[T], 2)
	currNode.children[0] = &node[T]{key: suffixKey, children: children, values: currNode.values, firstSeqNum: currNode.firstSeqNum, length: currNode.length} // Preserve the length and firstSeqNum in the left child
	currNode.children[1] = &node[T]{key: key[prefixLength:], values: values, firstSeqNum: 0, length: len(values)}                                            // New right child holds the new values
	currNode.length += len(values)                                                                                                                           // Increment length since we added new values

	currNode.key = prefixKey
	currNode.values = nil
}

// Read returns a slice of values from the tree starting from the given key and sequence number
// and up to <limit> values.
func (tree *LogTree[T]) Read(from Key, seqNum int, limit int) []T {
	assert.OK(seqNum >= 0, "seqNum is negative")
	assert.OK(limit > 0, "limit is not positive")

	initialLength := tree.Length()
	defer func() {
		assert.OK(tree.root.key == "", "root key is not empty")
		assert.OK(len(tree.root.values) == 0, "root has values")
		assert.OK(tree.Length() == initialLength, "tree length changed in a read-only operation")
	}()

	// If the provided key+seqNum is greater than the last key+seqNum, then there are no entries to read.
	lastKey, lastSeqNum := tree.LastPosition()
	if from > lastKey || (from == lastKey && seqNum > lastSeqNum) {
		return nil
	}

	// The provided key+seqNum define a lower-bound in the existing sequence, meaning we are only interested in values residing in leaf nodes
	// with keys greater than or equal to the provided key and at slice indexes greater than or equal to the provided seqNum.
	// In order to avoid extra work, we start by stacking the sequence of nodes that define this lower-bound (from root until the first leaf node).
	// The alternative would be to stack all the nodes from right-to-left, excluding the nodes that hit the lower-bound at the left. But by doing this
	// we could be stacking more nodes than we actually need to visit since we can end up returning much earler due to the specified limit.
	//
	// Here we define the stack and populate it with this first chain of nodes defining the lower-bound.
	stack := newStack[*readStackEntry[T]]()
	for currNode, exactKeyMatch := tree.root, true; ; {
		idx, isKeyMatch := findNodeIndexWithKeyGreaterOrEqualThan(currNode.children, from)
		remainingChildren := currNode.children[min(len(currNode.children), idx+1):]
		remainingValues := currNode.values
		if len(remainingValues) > 0 && exactKeyMatch {
			// Convert logical seqNum to physical array index using firstSeqNum
			physicalIndex := seqNum - currNode.firstSeqNum
			if physicalIndex < 0 {
				physicalIndex = 0
			}
			remainingValues = remainingValues[min(len(remainingValues), physicalIndex):]
		}

		stack.Push(&readStackEntry[T]{
			node:              currNode,
			remainingChildren: remainingChildren,
			remainingValues:   remainingValues,
		})

		exactKeyMatch = exactKeyMatch && isKeyMatch

		if len(currNode.children) > 0 {
			currNode = currNode.children[idx]
			from = from[len(currNode.key):]
		} else {
			break
		}
	}

	// We then traverse the stack, popping nodes and adding their children to the stack until we either run out of nodes or reach the limit.
	// While traversing, we add the values to the entries slice until we reach the limit.
	entries := make([]T, 0, limit)
	for !stack.IsEmpty() && len(entries) < limit {
		currEntry := stack.Peek()

		if len(currEntry.remainingChildren) > 0 {
			nextNode := currEntry.remainingChildren[0]
			stack.Push(&readStackEntry[T]{
				node:              nextNode,
				remainingChildren: nextNode.children,
				remainingValues:   nextNode.values,
			})
			currEntry.remainingChildren = currEntry.remainingChildren[1:]
			continue
		}

		if len(currEntry.remainingValues) > 0 {
			entries = append(entries, currEntry.remainingValues[:min(len(currEntry.remainingValues), limit-len(entries))]...)
		}

		_ = stack.Pop()
	}

	return entries
}

// Trim removes all values from the tree that are lexicographically strictly less than the provided key and sequence number.
// All values with keys < until are removed, and for the leaf node with key == until, values at indices [0:seqNum] are removed.
// The value at the exact key and seqNum is preserved.
//
// COMMENT written by hastyy:
// Trim implementation was written by Claude with a very detailed prompt of the implementation strategy, as well as the intuitions
// behind the tree navigation, defining the trim limits and reconciling the tree invariants.
func (tree *LogTree[T]) Trim(until Key, seqNum int) {
	assert.OK(seqNum >= 0, "seqNum is negative")

	initialLength := tree.Length()
	defer func() {
		assert.OK(tree.root.key == "", "root key is not empty")
		assert.OK(len(tree.root.values) == 0, "root has values")
		// Length should have decreased or stayed the same
		assert.OK(tree.Length() <= initialLength, "tree length increased in a trim operation")
	}()

	// If the tree is empty, nothing to trim
	if tree.Length() == 0 {
		return
	}

	// If until is less than the first entry, nothing to trim
	// We need to check if until is actually in range that makes sense to trim
	if until < tree.root.children[0].key {
		return
	}

	// First pass: Navigate down and trim nodes
	trimNode(tree.root, until, seqNum, true)

	// Second pass: Cleanup to maintain invariants
	// - Merge nodes with only 1 child
	// - Remove empty nodes (except root)
	tree.cleanup(tree.root)
}

// Length returns the length (number of values) of the tree.
func (tree *LogTree[T]) Length() int {
	return tree.root.length
}

// LastPosition returns the last key and sequence number in the tree.
// The sequence number is calculated from the rightmost leaf's firstSeqNum + number of values - 1.
func (tree *LogTree[T]) LastPosition() (Key, int) {
	if tree.Length() == 0 {
		return "", -1
	}
	rightmostLeaf := tree.rightmostNode()
	lastSeqNum := rightmostLeaf.firstSeqNum + len(rightmostLeaf.values) - 1
	return tree.lastKey, lastSeqNum
}

// findNodeIndexWithKeyGreaterOrEqualThan finds the index of the node with the key greater than or equal to the provided key.
// It returns the index and a boolean indicating if the key is equal to the node's key.
// It uses a binary search to find the index.
func findNodeIndexWithKeyGreaterOrEqualThan[T any](nodes []*node[T], key Key) (idx int, equal bool) {
	if len(nodes) == 0 {
		return 0, false
	}

	low := 0
	high := len(nodes) - 1

	for low <= high {
		mid := (low + high) / 2
		currChildKey := nodes[mid].key
		keyPrefix := key[:len(currChildKey)]

		if keyPrefix == currChildKey {
			return mid, true
		}

		if currChildKey < keyPrefix {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return low, false
}

// rightmostNode returns the rightmost node in the tree.
func (tree *LogTree[T]) rightmostNode() *node[T] {
	currNode := tree.root
	for len(currNode.children) > 0 {
		currNode = currNode.children[len(currNode.children)-1]
	}
	return currNode
}

// trimNode recursively trims the subtree rooted at n.
// It returns the number of values trimmed from this subtree.
func trimNode[T any](n *node[T], until Key, seqNum int, exactKeyMatch bool) int {
	// If this is a leaf node, trim values if we have an exact key match
	if len(n.children) == 0 {
		if exactKeyMatch && len(n.values) > 0 {
			// Convert logical seqNum to physical array index using firstSeqNum
			// We want to remove all values with logical seqNum < requested seqNum
			// So if firstSeqNum=2 and seqNum=4, we remove values at physical indices [0, 1]
			// (which have logical seqNums 2, 3)
			physicalIndex := seqNum - n.firstSeqNum
			if physicalIndex <= 0 {
				// seqNum is before or at the first value in this node, nothing to trim
				return 0
			}
			trimCount := min(physicalIndex, len(n.values))
			n.values = n.values[trimCount:]
			n.firstSeqNum += trimCount // Update firstSeqNum after trimming
			n.length -= trimCount
			return trimCount
		}
		return 0
	}

	// Non-leaf node: find where to trim
	idx, isKeyMatch := findNodeIndexWithKeyGreaterOrEqualThan(n.children, until)

	totalTrimmed := 0

	// Remove all children to the left of idx (they are all < until)
	if idx > 0 {
		for i := 0; i < idx; i++ {
			totalTrimmed += n.children[i].length
		}
		n.children = n.children[idx:]
	}

	// If we found an exact match, recursively trim that child
	if len(n.children) > 0 && isKeyMatch {
		childKey := n.children[0].key
		remainingKey := until[len(childKey):]
		trimmed := trimNode(n.children[0], remainingKey, seqNum, exactKeyMatch && isKeyMatch)
		totalTrimmed += trimmed
	}

	// Update this node's length
	n.length -= totalTrimmed
	return totalTrimmed
}

// cleanup performs housekeeping on the tree to maintain invariants:
// - Nodes with only 1 child are merged with that child (except root)
// - Empty nodes (no children and no values) are removed (except root)
func (tree *LogTree[T]) cleanup(n *node[T]) {
	if len(n.children) == 0 {
		return
	}

	// Recursively cleanup all children first
	for i := 0; i < len(n.children); i++ {
		tree.cleanup(n.children[i])
	}

	// Remove empty children (nodes with no children and no values)
	newChildren := make([]*node[T], 0, len(n.children))
	for _, child := range n.children {
		if len(child.children) > 0 || len(child.values) > 0 {
			newChildren = append(newChildren, child)
		}
	}
	n.children = newChildren

	// If this node (and it's not root) has exactly one child, merge with it
	// This maintains the invariant that non-leaf nodes have at least 2 children
	if len(n.children) == 1 && n != tree.root {
		child := n.children[0]
		// Merge: concatenate keys, take child's children/values, length stays the same
		n.key = n.key + child.key
		n.children = child.children
		n.values = child.values
		// n.length stays the same since it already represents the subtree
	}
}
