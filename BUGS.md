# List of problems I run into while testing

- LogTree doesn't deal well with keys that have a lower length than the existing keys on the tree, e.g. key has keys of len 13, like 1731456789012, but I pass it the key 0 (len 1) {fix by having fixed ms length of 20chars (max uint64 length)}
-
