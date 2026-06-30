package crdtstore

import (
	redis "github.com/redis-geo/redgeo/redisapi"
)

// Redka returns the six storage interfaces bound to logical database db
// (DESIGN §6.11). Each connection calls this with its selected DB so every
// key carries the right {db} segment.
//
// Repositories are wired in one phase at a time; until a type's repo is
// implemented its slot here is nil and its commands aren't registered.
func (s *Store) Redka(db int) redis.Redka {
	return redis.New(
		strRepo{s: s, db: db},  // RStr  — Phase 1
		keyRepo{s: s, db: db},  // RKey  — Phase 1
		hashRepo{s: s, db: db}, // RHash — Phase 2
		setRepo{s: s, db: db},  // RSet  — Phase 2
		zsetRepo{s: s, db: db}, // RZSet — Phase 5
		nil,                    // RList — Phase 6
	)
}
