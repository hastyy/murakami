package logtree

import "strings"

type Key string

func (k Key) HasPrefix(prefix Key) bool {
	return strings.HasPrefix(string(k), string(prefix))
}

func (k Key) IsPrefixOf(otherKey Key) bool {
	return strings.HasPrefix(string(otherKey), string(k))
}
