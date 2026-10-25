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

// hllBase / hllSlot hold per-replica HyperLogLog sketches (§6.4-style
// single-writer components, merged by register-wise max):
// /{P}/d/{db}/{key}/p/{replicaID}
func hllBase(db int, key string) string {
	return fmt.Sprintf("%s/d/%d/%s/p/", part(db, key), db, encSeg(key))
}

func hllSlot(db int, key, replica string) ds.Key {
	return ds.NewKey(hllBase(db, key) + encSeg(replica))
}

// bitBase / bitChunkBase / bitSlot hold per-replica bitmap chunk slots:
// /{P}/d/{db}/{key}/b/{chunk:%08x}/{replicaID}
func bitBase(db int, key string) string {
	return fmt.Sprintf("%s/d/%d/%s/b/", part(db, key), db, encSeg(key))
}

func bitChunkBase(db int, key string, chunk int) string {
	return fmt.Sprintf("%s%08x/", bitBase(db, key), chunk)
}

func bitSlot(db int, key string, chunk int, replica string) ds.Key {
	return ds.NewKey(bitChunkBase(db, key, chunk) + encSeg(replica))
}

// streamBase / streamSlot hold stream entries as per-replica LWW slots keyed by
// ID (ms and seq as fixed-width hex so lexicographic order is numeric):
// /{P}/d/{db}/{key}/x/{ms:%016x}-{seq:%016x}/{replicaID}
// streamLastBase / streamLastSlot hold each replica's highest known ID, a
// max-merged register used to mint monotonic IDs and to define existence.
// /{P}/d/{db}/{key}/xl/{replicaID}
func streamBase(db int, key string) string {
	return fmt.Sprintf("%s/d/%d/%s/x/", part(db, key), db, encSeg(key))
}

func streamSlot(db int, key string, ms, seq uint64, replica string) ds.Key {
	return ds.NewKey(fmt.Sprintf("%s%016x-%016x/%s", streamBase(db, key), ms, seq, encSeg(replica)))
}

func streamLastBase(db int, key string) string {
	return fmt.Sprintf("%s/d/%d/%s/xl/", part(db, key), db, encSeg(key))
}

func streamLastSlot(db int, key, replica string) ds.Key {
	return ds.NewKey(streamLastBase(db, key) + encSeg(replica))
}

// replicaRegPrefix / replicaRegKey form the replica registry: each node writes
// one presence key so every other node (and compaction, package rotation) can
// enumerate the cluster's members with one tiny prefix query. It lives in
// bucket 00 outside the /m/ and /d/ keyspaces, so keyspace commands never see it.
const replicaRegPrefix = "/00/r/"

func replicaRegKey(replica string) ds.Key { return ds.NewKey(replicaRegPrefix + encSeg(replica)) }

// Stream consumer-group state, all under /{P}/d/{db}/{key}/xg/{group}/:
//
//	m/{replica}                 group existence (LWW slot)
//	l/{replica}                 last-delivered (epoch, id), max-merged
//	p/{ms}-{seq}/{replica}      pending entry (LWW slot)
//	c/{consumer}/{replica}      consumer (LWW slot)
func groupsBase(db int, key string) string {
	return fmt.Sprintf("%s/d/%d/%s/xg/", part(db, key), db, encSeg(key))
}

func groupBase(db int, key, group string) string { return groupsBase(db, key) + encSeg(group) + "/" }

func groupMetaSlot(db int, key, group, replica string) ds.Key {
	return ds.NewKey(groupBase(db, key, group) + "m/" + encSeg(replica))
}

func groupLastSlot(db int, key, group, replica string) ds.Key {
	return ds.NewKey(groupBase(db, key, group) + "l/" + encSeg(replica))
}

func groupPelSlot(db int, key, group string, ms, seq uint64, replica string) ds.Key {
	return ds.NewKey(fmt.Sprintf("%sp/%016x-%016x/%s", groupBase(db, key, group), ms, seq, encSeg(replica)))
}

func groupConsumerSlot(db int, key, group, consumer, replica string) ds.Key {
	return ds.NewKey(groupBase(db, key, group) + "c/" + encSeg(consumer) + "/" + encSeg(replica))
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
