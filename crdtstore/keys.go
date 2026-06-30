package crdtstore

import (
	"encoding/base32"
	"fmt"
	"hash/fnv"

	ds "github.com/ipfs/go-datastore"
)

// NumBuckets is the partition bucket count (DESIGN §11, decided: 256,
// effectively immutable once data exists).
const NumBuckets = 256

// seg32 encodes an arbitrary binary key/field/member segment into a single
// path-safe ds.Key component. Redis keys are arbitrary binary but ds.Key is
// '/'-delimited text (DESIGN §5.4), so we base32-encode (A-Z2-7, no '/', no
// padding) which is reversible and can never collide or break prefix scans.
var seg32 = base32.StdEncoding.WithPadding(base32.NoPadding)

func encSeg(s string) string { return seg32.EncodeToString([]byte(s)) }

func decSeg(s string) (string, error) {
	b, err := seg32.DecodeString(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// bucket maps (db, key) to a partition bucket in [0, NumBuckets). It must be a
// pure, stable function of db+key so every sub-key of one logical Redis key
// lands in the same partition (DESIGN §5.1). FNV-1a is deterministic across
// nodes and architectures.
func bucket(db int, key string) int {
	h := fnv.New32a()
	var dbb [8]byte
	for i := 0; i < 8; i++ {
		dbb[i] = byte(db >> (8 * i))
	}
	_, _ = h.Write(dbb[:])
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % NumBuckets)
}

// part returns the leading "/{P}" partition segment for (db, key).
func part(db int, key string) string {
	return fmt.Sprintf("/%02x", bucket(db, key))
}

// Key path builders (DESIGN §5.1). All take the logical (db, key) so the
// partition bucket is consistent. {replicaID} slots hold per-replica LWW
// envelopes (§6.7); presence-only keys (set members) carry no replica segment.

// metaBase is the prefix holding a key's per-replica KeyMeta slots:
// /{P}/m/{db}/{key}/
func metaBase(db int, key string) string {
	return fmt.Sprintf("%s/m/%d/%s/", part(db, key), db, encSeg(key))
}

// metaSlot is one replica's KeyMeta slot: /{P}/m/{db}/{key}/{replicaID}
func metaSlot(db int, key, replica string) ds.Key {
	return ds.NewKey(metaBase(db, key) + encSeg(replica))
}

// strBase is the prefix holding a string's per-replica value slots:
// /{P}/d/{db}/{key}/v/
func strBase(db int, key string) string {
	return fmt.Sprintf("%s/d/%d/%s/v/", part(db, key), db, encSeg(key))
}

// strSlot is one replica's string value slot: /{P}/d/{db}/{key}/v/{replicaID}
func strSlot(db int, key, replica string) ds.Key {
	return ds.NewKey(strBase(db, key) + encSeg(replica))
}

// counterBase / counterSlot hold per-replica PN-counter components (§6.4):
// /{P}/d/{db}/{key}/c/{replicaID}
func counterBase(db int, key string) string {
	return fmt.Sprintf("%s/d/%d/%s/c/", part(db, key), db, encSeg(key))
}

func counterSlot(db int, key, replica string) ds.Key {
	return ds.NewKey(counterBase(db, key) + encSeg(replica))
}

// hashBase / hashFieldBase / hashSlot hold per-field, per-replica LWW slots:
// /{P}/d/{db}/{key}/h/{field}/{replicaID}
func hashBase(db int, key string) string {
	return fmt.Sprintf("%s/d/%d/%s/h/", part(db, key), db, encSeg(key))
}

func hashFieldBase(db int, key, field string) string {
	return hashBase(db, key) + encSeg(field) + "/"
}

func hashSlot(db int, key, field, replica string) ds.Key {
	return ds.NewKey(hashFieldBase(db, key, field) + encSeg(replica))
}

// hashFieldCounterBase / hashFieldCounterSlot hold a hash field's per-replica
// PN-counter components (§6.4): /{P}/d/{db}/{key}/h/{field}/c/{replicaID}.
// The literal "c" segment can't collide with an encoded replica ID (base32
// never produces a lone "c").
func hashFieldCounterBase(db int, key, field string) string {
	return hashFieldBase(db, key, field) + "c/"
}

func hashFieldCounterSlot(db int, key, field, replica string) ds.Key {
	return ds.NewKey(hashFieldCounterBase(db, key, field) + encSeg(replica))
}

// setBase / setMember hold presence-only OR-Set members (§6.3):
// /{P}/d/{db}/{key}/e/{member}
func setBase(db int, key string) string {
	return fmt.Sprintf("%s/d/%d/%s/e/", part(db, key), db, encSeg(key))
}

func setMember(db int, key, member string) ds.Key {
	return ds.NewKey(setBase(db, key) + encSeg(member))
}

// zsetBase / zsetMemberBase / zsetSlot hold per-member, per-replica score
// slots (§6.6): /{P}/d/{db}/{key}/z/{member}/{replicaID}
func zsetBase(db int, key string) string {
	return fmt.Sprintf("%s/d/%d/%s/z/", part(db, key), db, encSeg(key))
}

func zsetMemberBase(db int, key, member string) string {
	return zsetBase(db, key) + encSeg(member) + "/"
}

func zsetSlot(db int, key, member, replica string) ds.Key {
	return ds.NewKey(zsetMemberBase(db, key, member) + encSeg(replica))
}

// listBase / listElem hold fractional-index list elements (§6.5):
// /{P}/d/{db}/{key}/l/{posKey}
func listBase(db int, key string) string {
	return fmt.Sprintf("%s/d/%d/%s/l/", part(db, key), db, encSeg(key))
}

func listElem(db int, key, posKey string) ds.Key {
	return ds.NewKey(listBase(db, key) + posKey)
}

// dbMetaPrefixes returns the per-partition meta prefixes for a db-wide scan
// (DESIGN §6.9: a db-wide scan iterates all {P} buckets). Each entry is
// "/{P}/m/{db}/".
func dbMetaPrefixes(db int) []string {
	out := make([]string, NumBuckets)
	for p := 0; p < NumBuckets; p++ {
		out[p] = fmt.Sprintf("/%02x/m/%d/", p, db)
	}
	return out
}
