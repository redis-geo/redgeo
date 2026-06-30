package crdtstore

import (
	"fmt"
	"strconv"
)

// lockKey is the node-local lock identity for a logical (db, key). All
// sub-keys of one Redis key share it so RMW sequences are atomic per node.
func lockKey(db int, key string) string {
	return fmt.Sprintf("%d/%s", db, key)
}

// itoa / ftoa format integer and float register values the way Redis does
// (matching core.ToBytes' float formatting).
func itoa(n int) string { return strconv.Itoa(n) }

func ftoa(f float64) []byte {
	return []byte(strconv.FormatFloat(f, 'f', -1, 64))
}
