package logtree

import (
	"testing"
)

func TestAppend(t *testing.T) {
	// Setup
	tree := New[int]()

	// Only root
	validateNode(t, tree.root, nodeExpectations{key: "", children: 0, values: 0, length: 0})
	if tree.Length() != 0 {
		t.Errorf("Expected tree length to be 0, got %d", tree.Length())
	}

	// Append 1737238769766
	tree.Append(Key("1737238769766"), []int{1})
	validateNode(t, tree.root, nodeExpectations{key: "", children: 1, values: 0, length: 1})
	validateNode(t, tree.root.children[0], nodeExpectations{key: Key("1737238769766"), children: 0, values: 1, length: 1})
	if tree.Length() != 1 {
		t.Errorf("Expected tree length to be 1, got %d", tree.Length())
	}

	// Append 1737238769786
	tree.Append(Key("1737238769786"), []int{2})
	validateNode(t, tree.root, nodeExpectations{key: "", children: 1, values: 0, length: 2})
	validateNode(t, tree.root.children[0], nodeExpectations{key: Key("17372387697"), children: 2, values: 0, length: 2})
	validateNode(t, tree.root.children[0].children[0], nodeExpectations{key: Key("66"), children: 0, values: 1, length: 1})
	validateNode(t, tree.root.children[0].children[1], nodeExpectations{key: Key("86"), children: 0, values: 1, length: 1})
	if tree.Length() != 2 {
		t.Errorf("Expected tree length to be 2, got %d", tree.Length())
	}

	// Append 1737238769806
	tree.Append(Key("1737238769806"), []int{3})
	validateNode(t, tree.root, nodeExpectations{key: "", children: 1, values: 0, length: 3})
	validateNode(t, tree.root.children[0], nodeExpectations{key: Key("1737238769"), children: 2, values: 0, length: 3})
	validateNode(t, tree.root.children[0].children[0], nodeExpectations{key: Key("7"), children: 2, values: 0, length: 2})
	validateNode(t, tree.root.children[0].children[1], nodeExpectations{key: Key("806"), children: 0, values: 1, length: 1})
	validateNode(t, tree.root.children[0].children[0].children[0], nodeExpectations{key: Key("66"), children: 0, values: 1, length: 1})
	validateNode(t, tree.root.children[0].children[0].children[1], nodeExpectations{key: Key("86"), children: 0, values: 1, length: 1})
	if tree.Length() != 3 {
		t.Errorf("Expected tree length to be 3, got %d", tree.Length())
	}

	// Append 1737238769806 again
	tree.Append(Key("1737238769806"), []int{4})
	validateNode(t, tree.root, nodeExpectations{key: "", children: 1, values: 0, length: 4})
	validateNode(t, tree.root.children[0], nodeExpectations{key: Key("1737238769"), children: 2, values: 0, length: 4})
	validateNode(t, tree.root.children[0].children[0], nodeExpectations{key: Key("7"), children: 2, values: 0, length: 2})
	validateNode(t, tree.root.children[0].children[1], nodeExpectations{key: Key("806"), children: 0, values: 2, length: 2})
	validateNode(t, tree.root.children[0].children[0].children[0], nodeExpectations{key: Key("66"), children: 0, values: 1, length: 1})
	validateNode(t, tree.root.children[0].children[0].children[1], nodeExpectations{key: Key("86"), children: 0, values: 1, length: 1})
	if tree.Length() != 4 {
		t.Errorf("Expected tree length to be 4, got %d", tree.Length())
	}

	// Append 1737238769816
	tree.Append(Key("1737238769816"), []int{5})
	validateNode(t, tree.root, nodeExpectations{key: "", children: 1, values: 0, length: 5})
	validateNode(t, tree.root.children[0], nodeExpectations{key: Key("1737238769"), children: 2, values: 0, length: 5})
	validateNode(t, tree.root.children[0].children[0], nodeExpectations{key: Key("7"), children: 2, values: 0, length: 2})
	validateNode(t, tree.root.children[0].children[1], nodeExpectations{key: Key("8"), children: 2, values: 0, length: 3})
	validateNode(t, tree.root.children[0].children[0].children[0], nodeExpectations{key: Key("66"), children: 0, values: 1, length: 1})
	validateNode(t, tree.root.children[0].children[0].children[1], nodeExpectations{key: Key("86"), children: 0, values: 1, length: 1})
	validateNode(t, tree.root.children[0].children[1].children[0], nodeExpectations{key: Key("06"), children: 0, values: 2, length: 2})
	validateNode(t, tree.root.children[0].children[1].children[1], nodeExpectations{key: Key("16"), children: 0, values: 1, length: 1})
	if tree.Length() != 5 {
		t.Errorf("Expected tree length to be 5, got %d", tree.Length())
	}

	// Append 1737242309766
	tree.Append(Key("1737242309766"), []int{6})
	validateNode(t, tree.root, nodeExpectations{key: "", children: 1, values: 0, length: 6})
	validateNode(t, tree.root.children[0], nodeExpectations{key: Key("17372"), children: 2, values: 0, length: 6})
	validateNode(t, tree.root.children[0].children[0], nodeExpectations{key: Key("38769"), children: 2, values: 0, length: 5})
	validateNode(t, tree.root.children[0].children[1], nodeExpectations{key: Key("42309766"), children: 0, values: 1, length: 1})
	validateNode(t, tree.root.children[0].children[0].children[0], nodeExpectations{key: Key("7"), children: 2, values: 0, length: 2})
	validateNode(t, tree.root.children[0].children[0].children[1], nodeExpectations{key: Key("8"), children: 2, values: 0, length: 3})
	validateNode(t, tree.root.children[0].children[0].children[0].children[0], nodeExpectations{key: Key("66"), children: 0, values: 1, length: 1})
	validateNode(t, tree.root.children[0].children[0].children[0].children[1], nodeExpectations{key: Key("86"), children: 0, values: 1, length: 1})
	validateNode(t, tree.root.children[0].children[0].children[1].children[0], nodeExpectations{key: Key("06"), children: 0, values: 2, length: 2})
	validateNode(t, tree.root.children[0].children[0].children[1].children[1], nodeExpectations{key: Key("16"), children: 0, values: 1, length: 1})
	if tree.Length() != 6 {
		t.Errorf("Expected tree length to be 6, got %d", tree.Length())
	}

	// Append 1737252309766
	tree.Append(Key("1737252309766"), []int{7})
	validateNode(t, tree.root, nodeExpectations{key: "", children: 1, values: 0, length: 7})
	validateNode(t, tree.root.children[0], nodeExpectations{key: Key("17372"), children: 3, values: 0, length: 7})
	validateNode(t, tree.root.children[0].children[0], nodeExpectations{key: Key("38769"), children: 2, values: 0, length: 5})
	validateNode(t, tree.root.children[0].children[1], nodeExpectations{key: Key("42309766"), children: 0, values: 1, length: 1})
	validateNode(t, tree.root.children[0].children[2], nodeExpectations{key: Key("52309766"), children: 0, values: 1, length: 1})
	validateNode(t, tree.root.children[0].children[0].children[0], nodeExpectations{key: Key("7"), children: 2, values: 0, length: 2})
	validateNode(t, tree.root.children[0].children[0].children[1], nodeExpectations{key: Key("8"), children: 2, values: 0, length: 3})
	validateNode(t, tree.root.children[0].children[0].children[0].children[0], nodeExpectations{key: Key("66"), children: 0, values: 1, length: 1})
	validateNode(t, tree.root.children[0].children[0].children[0].children[1], nodeExpectations{key: Key("86"), children: 0, values: 1, length: 1})
	validateNode(t, tree.root.children[0].children[0].children[1].children[0], nodeExpectations{key: Key("06"), children: 0, values: 2, length: 2})
	validateNode(t, tree.root.children[0].children[0].children[1].children[1], nodeExpectations{key: Key("16"), children: 0, values: 1, length: 1})
	if tree.Length() != 7 {
		t.Errorf("Expected tree length to be 7, got %d", tree.Length())
	}

	// Append 2000000000001
	tree.Append(Key("2000000000001"), []int{8})
	validateNode(t, tree.root, nodeExpectations{key: "", children: 2, values: 0, length: 8})
	validateNode(t, tree.root.children[0], nodeExpectations{key: Key("17372"), children: 3, values: 0, length: 7})
	validateNode(t, tree.root.children[1], nodeExpectations{key: Key("2000000000001"), children: 0, values: 1, length: 1})
	validateNode(t, tree.root.children[0].children[0], nodeExpectations{key: Key("38769"), children: 2, values: 0, length: 5})
	validateNode(t, tree.root.children[0].children[1], nodeExpectations{key: Key("42309766"), children: 0, values: 1, length: 1})
	validateNode(t, tree.root.children[0].children[2], nodeExpectations{key: Key("52309766"), children: 0, values: 1, length: 1})
	validateNode(t, tree.root.children[0].children[0].children[0], nodeExpectations{key: Key("7"), children: 2, values: 0, length: 2})
	validateNode(t, tree.root.children[0].children[0].children[1], nodeExpectations{key: Key("8"), children: 2, values: 0, length: 3})
	validateNode(t, tree.root.children[0].children[0].children[0].children[0], nodeExpectations{key: Key("66"), children: 0, values: 1, length: 1})
	validateNode(t, tree.root.children[0].children[0].children[0].children[1], nodeExpectations{key: Key("86"), children: 0, values: 1, length: 1})
	validateNode(t, tree.root.children[0].children[0].children[1].children[0], nodeExpectations{key: Key("06"), children: 0, values: 2, length: 2})
	validateNode(t, tree.root.children[0].children[0].children[1].children[1], nodeExpectations{key: Key("16"), children: 0, values: 1, length: 1})
	if tree.Length() != 8 {
		t.Errorf("Expected tree length to be 8, got %d", tree.Length())
	}
}

func TestRead(t *testing.T) {
	// Setup
	tree := New[int]()

	// Create tree
	tree.Append(Key("1737238769766"), []int{1})
	tree.Append(Key("1737238769786"), []int{2})
	tree.Append(Key("1737238769806"), []int{3})
	tree.Append(Key("1737238769806"), []int{4})
	tree.Append(Key("1737238769816"), []int{5})
	tree.Append(Key("1737242309766"), []int{6})
	tree.Append(Key("1737252309766"), []int{7})
	tree.Append(Key("2000000000001"), []int{8})

	// Read from 1737238769766, seqNum 0, limit 1
	entries := tree.Read(Key("1737238769766"), 0, 1)
	if len(entries) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(entries))
	}
	if entries[0] != 1 {
		t.Errorf("Expected entry to be 1, got %d", entries[0])
	}

	// Read from 1737238769000, seqNum 0, limit 7
	entries = tree.Read(Key("1737238769000"), 0, 7)
	if len(entries) != 7 {
		t.Errorf("Expected 7 entries, got %d", len(entries))
	}
	if entries[0] != 1 {
		t.Errorf("Expected entry to be 1, got %d", entries[0])
	}
	if entries[1] != 2 {
		t.Errorf("Expected entry to be 2, got %d", entries[1])
	}
	if entries[2] != 3 {
		t.Errorf("Expected entry to be 3, got %d", entries[2])
	}
	if entries[3] != 4 {
		t.Errorf("Expected entry to be 4, got %d", entries[3])
	}
	if entries[4] != 5 {
		t.Errorf("Expected entry to be 5, got %d", entries[4])
	}
	if entries[5] != 6 {
		t.Errorf("Expected entry to be 6, got %d", entries[5])
	}
	if entries[6] != 7 {
		t.Errorf("Expected entry to be 7, got %d", entries[6])
	}

	// Read from 1737238769000, seqNum 0, limit 2
	entries = tree.Read(Key("1737238769000"), 0, 2)
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(entries))
	}
	if entries[0] != 1 {
		t.Errorf("Expected entry to be 1, got %d", entries[0])
	}
	if entries[1] != 2 {
		t.Errorf("Expected entry to be 2, got %d", entries[1])
	}

	// Read from 1737238769000, seqNum 0, limit 10
	entries = tree.Read(Key("1737238769000"), 0, 10)
	if len(entries) != 8 {
		t.Errorf("Expected 1 entry, got %d", len(entries))
	}
	if entries[0] != 1 {
		t.Errorf("Expected entry to be 1, got %d", entries[0])
	}
	if entries[1] != 2 {
		t.Errorf("Expected entry to be 2, got %d", entries[1])
	}
	if entries[2] != 3 {
		t.Errorf("Expected entry to be 3, got %d", entries[2])
	}
	if entries[3] != 4 {
		t.Errorf("Expected entry to be 4, got %d", entries[3])
	}
	if entries[4] != 5 {
		t.Errorf("Expected entry to be 5, got %d", entries[4])
	}
	if entries[5] != 6 {
		t.Errorf("Expected entry to be 6, got %d", entries[5])
	}
	if entries[6] != 7 {
		t.Errorf("Expected entry to be 7, got %d", entries[6])
	}
	if entries[7] != 8 {
		t.Errorf("Expected entry to be 8, got %d", entries[7])
	}

	// Read from 1737238769786, seqNum 5, limit 1
	entries = tree.Read(Key("1737238769786"), 5, 1)
	if len(entries) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(entries))
	}
	if entries[0] != 3 {
		t.Errorf("Expected entry to be 3, got %d", entries[0])
	}

	// Read from 1737238769666, seqNum 5, limit 1
	entries = tree.Read(Key("1737238769666"), 5, 1)
	if len(entries) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(entries))
	}
	if entries[0] != 1 {
		t.Errorf("Expected entry to be 1, got %d", entries[0])
	}
}

// Trim tests were written by Claude
func TestTrim(t *testing.T) {
	// Setup - create a tree with multiple entries
	tree := New[int]()
	tree.Append(Key("1737238769766"), []int{1})
	tree.Append(Key("1737238769786"), []int{2})
	tree.Append(Key("1737238769806"), []int{3})
	tree.Append(Key("1737238769806"), []int{4}) // Same key, different seqNum
	tree.Append(Key("1737238769816"), []int{5})
	tree.Append(Key("1737242309766"), []int{6})
	tree.Append(Key("1737252309766"), []int{7})
	tree.Append(Key("2000000000001"), []int{8})

	// Verify initial state
	if tree.Length() != 8 {
		t.Errorf("Expected tree length to be 8, got %d", tree.Length())
	}

	// Test 1: Trim with key less than first entry - should do nothing
	tree.Trim(Key("1000000000000"), 0)
	if tree.Length() != 8 {
		t.Errorf("Expected tree length to be 8 after trim with small key, got %d", tree.Length())
	}

	// Test 2: Trim first entry (key "1737238769766", seqNum 1)
	// This removes values strictly before seqNum 1, which is just the value at seqNum 0
	tree.Trim(Key("1737238769766"), 1)
	if tree.Length() != 7 {
		t.Errorf("Expected tree length to be 7 after trimming first entry, got %d", tree.Length())
	}
	entries := tree.Read(Key("1737238769000"), 0, 10)
	if len(entries) != 7 {
		t.Errorf("Expected 7 entries after trim, got %d", len(entries))
	}
	if entries[0] != 2 {
		t.Errorf("Expected first entry to be 2, got %d", entries[0])
	}

	// Test 3: Trim one of the duplicate entries (key "1737238769806", seqNum 1)
	// This should remove entry 3 (at seqNum 0) but keep entry 4 (at seqNum 1)
	tree.Trim(Key("1737238769806"), 1)
	if tree.Length() != 5 {
		t.Errorf("Expected tree length to be 5 after trimming, got %d", tree.Length())
	}
	entries = tree.Read(Key("1737238769000"), 0, 10)
	if len(entries) != 5 {
		t.Errorf("Expected 5 entries after trim, got %d", len(entries))
	}
	// Should have removed 2, 3, and kept 4, 5, 6, 7, 8
	if entries[0] != 4 {
		t.Errorf("Expected first entry to be 4, got %d", entries[0])
	}
	if entries[1] != 5 {
		t.Errorf("Expected second entry to be 5, got %d", entries[1])
	}

	// Test 4: Trim with a key that doesn't exist (between entries)
	// Should trim everything up to and including 1737238769816
	tree.Trim(Key("1737238769820"), 0)
	if tree.Length() != 3 {
		t.Errorf("Expected tree length to be 3 after trimming, got %d", tree.Length())
	}
	entries = tree.Read(Key("1737238769000"), 0, 10)
	if len(entries) != 3 {
		t.Errorf("Expected 3 entries after trim, got %d", len(entries))
	}
	if entries[0] != 6 || entries[1] != 7 || entries[2] != 8 {
		t.Errorf("Expected entries [6, 7, 8], got %v", entries)
	}

	// Test 5: Trim all but last entry
	// To remove entry 7, we need to use seqNum 1 (which removes everything before seqNum 1, i.e., seqNum 0)
	tree.Trim(Key("1737252309766"), 1)
	if tree.Length() != 1 {
		t.Errorf("Expected tree length to be 1 after trimming, got %d", tree.Length())
	}
	entries = tree.Read(Key("1000000000000"), 0, 10)
	if len(entries) != 1 {
		t.Errorf("Expected 1 entry after trim, got %d", len(entries))
	}
	if entries[0] != 8 {
		t.Errorf("Expected entry to be 8, got %d", entries[0])
	}

	// Test 6: Trim the last entry
	// To remove entry 8 at seqNum 0, we need to use seqNum 1
	tree.Trim(Key("2000000000001"), 1)
	if tree.Length() != 0 {
		t.Errorf("Expected tree length to be 0 after trimming all, got %d", tree.Length())
	}
	entries = tree.Read(Key("1000000000000"), 0, 10)
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries after trimming all, got %d", len(entries))
	}

	// Test 7: Trim on empty tree - should not panic
	tree.Trim(Key("3000000000000"), 0)
	if tree.Length() != 0 {
		t.Errorf("Expected tree length to remain 0, got %d", tree.Length())
	}
}

// TestTrimPreservesExactKeyAndSeqNum explicitly tests that trimming at an exact key+seqNum
// preserves the value at that position
func TestTrimPreservesExactKeyAndSeqNum(t *testing.T) {
	tree := New[string]()
	tree.Append(Key("1000000000000"), []string{"a"})           // key 1000000000000, seqNum 0
	tree.Append(Key("2000000000000"), []string{"b"})           // key 2000000000000, seqNum 0
	tree.Append(Key("3000000000000"), []string{"c", "d", "e"}) // key 3000000000000, seqNum 0, 1, 2
	tree.Append(Key("4000000000000"), []string{"f"})           // key 4000000000000, seqNum 0

	if tree.Length() != 6 {
		t.Errorf("Expected tree length to be 6, got %d", tree.Length())
	}

	// Trim at exact position: key "3000000000000", seqNum 1
	// This should remove "a", "b", and "c" (seqNum 0 of key 3000000000000)
	// But preserve "d" (seqNum 1 of key 3000000000000), "e", and "f"
	tree.Trim(Key("3000000000000"), 1)

	if tree.Length() != 3 {
		t.Errorf("Expected tree length to be 3 after trim, got %d", tree.Length())
	}

	entries := tree.Read(Key("1000000000000"), 0, 10)
	if len(entries) != 3 {
		t.Errorf("Expected 3 entries after trim, got %d", len(entries))
	}

	// Verify the exact value at the trim position (seqNum 1) is preserved
	if entries[0] != "d" {
		t.Errorf("Expected first entry to be 'd' (the value at exact trim position), got %s", entries[0])
	}
	if entries[1] != "e" {
		t.Errorf("Expected second entry to be 'e', got %s", entries[1])
	}
	if entries[2] != "f" {
		t.Errorf("Expected third entry to be 'f', got %s", entries[2])
	}

	// Trim at another exact position: key "3000000000000", seqNum 2
	// After the first trim, "d" still has logical seqNum=1 and "e" still has logical seqNum=2
	// (sequence numbers are logical and don't change after trimming)
	// This should remove "d" (seqNum 1), and preserve "e" (seqNum 2) and "f"
	tree.Trim(Key("3000000000000"), 2)

	if tree.Length() != 2 {
		t.Errorf("Expected tree length to be 2 after second trim, got %d", tree.Length())
	}

	entries = tree.Read(Key("1000000000000"), 0, 10)
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries after second trim, got %d", len(entries))
	}

	// Verify the exact value at the second trim position (seqNum 2) is preserved
	if entries[0] != "e" {
		t.Errorf("Expected first entry to be 'e' (the value at exact second trim position), got %s", entries[0])
	}
	if entries[1] != "f" {
		t.Errorf("Expected second entry to be 'f', got %s", entries[1])
	}
}

// Trim tests were written by Claude
func TestTrimWithMultipleValuesPerKey(t *testing.T) {
	// Test trimming when multiple values share the same key
	tree := New[string]()
	tree.Append(Key("1000000000000"), []string{"a", "b", "c"}) // seqNum 0, 1, 2
	tree.Append(Key("2000000000000"), []string{"d", "e"})      // seqNum 0, 1
	tree.Append(Key("3000000000000"), []string{"f"})           // seqNum 0

	if tree.Length() != 6 {
		t.Errorf("Expected tree length to be 6, got %d", tree.Length())
	}

	// Trim first two values from first key (seqNum 0 and 1, preserve seqNum 2)
	tree.Trim(Key("1000000000000"), 2)
	if tree.Length() != 4 {
		t.Errorf("Expected tree length to be 4 after trim, got %d", tree.Length())
	}
	entries := tree.Read(Key("1000000000000"), 0, 10)
	if len(entries) != 4 {
		t.Errorf("Expected 4 entries, got %d", len(entries))
	}
	if entries[0] != "c" {
		t.Errorf("Expected first entry to be 'c', got %s", entries[0])
	}
	if entries[1] != "d" || entries[2] != "e" || entries[3] != "f" {
		t.Errorf("Expected entries [c, d, e, f], got %v", entries)
	}

	// Trim all of first key and part of second (preserve seqNum 1 of second key)
	tree.Trim(Key("2000000000000"), 1)
	if tree.Length() != 2 {
		t.Errorf("Expected tree length to be 2 after trim, got %d", tree.Length())
	}
	entries = tree.Read(Key("1000000000000"), 0, 10)
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(entries))
	}
	if entries[0] != "e" || entries[1] != "f" {
		t.Errorf("Expected entries [e, f], got %v", entries)
	}
}

// Trim tests were written by Claude
func TestTrimMaintainsTreeInvariants(t *testing.T) {
	// Test that trimming maintains tree structure invariants
	tree := New[int]()
	tree.Append(Key("1737238769766"), []int{1})
	tree.Append(Key("1737238769786"), []int{2})
	tree.Append(Key("1737238769806"), []int{3})
	tree.Append(Key("1737238769816"), []int{4})
	tree.Append(Key("1737242309766"), []int{5})

	// After trimming, verify root invariants
	tree.Trim(Key("1737238769786"), 1)

	// Root should always have empty key and no values, with length matching remaining entries
	if tree.root.key != "" {
		t.Errorf("Expected root key to be empty, got %s", tree.root.key)
	}
	if len(tree.root.values) != 0 {
		t.Errorf("Expected root to have no values, got %d", len(tree.root.values))
	}
	if tree.root.length != 3 {
		t.Errorf("Expected root length to be 3, got %d", tree.root.length)
	}

	// All non-leaf nodes should have at least 2 children (except root which can have 1)
	validateTreeInvariants(t, tree.root)
}

// Trim tests were written by Claude
func validateTreeInvariants[T any](t *testing.T, n *node[T]) {
	// Check that node has either children or values, not both (except root can have neither)
	if len(n.children) > 0 && len(n.values) > 0 {
		t.Errorf("Node has both children and values")
	}

	// Check length consistency
	if len(n.children) > 0 {
		totalLength := 0
		for _, child := range n.children {
			totalLength += child.length
		}
		if totalLength != n.length {
			t.Errorf("Node length %d doesn't match sum of children lengths %d", n.length, totalLength)
		}

		// Recursively validate children
		for _, child := range n.children {
			validateTreeInvariants(t, child)
		}
	} else if len(n.values) > 0 {
		if n.length != len(n.values) {
			t.Errorf("Leaf node length %d doesn't match values length %d", n.length, len(n.values))
		}
	}
}

type nodeExpectations struct {
	key      Key
	children int
	values   int
	length   int
}

func validateNode[T any](t *testing.T, n *node[T], expected nodeExpectations) {
	if n.key != expected.key {
		t.Errorf("Expected node to have key %s, got %s", expected.key, n.key)
	}
	if len(n.children) != expected.children {
		t.Errorf("Expected node to have %d children, got %d", expected.children, len(n.children))
	}
	if len(n.values) != expected.values {
		t.Errorf("Expected node to have %d values, got %d", expected.values, len(n.values))
	}
	if n.length != expected.length {
		t.Errorf("Expected node to have length %d, got %d", expected.length, n.length)
	}
}
