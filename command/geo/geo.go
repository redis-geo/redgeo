package geo

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/redis-geo/redgeo/core"
	redis "github.com/redis-geo/redgeo/redisapi"
)

func parseCoord(lonB, latB []byte) (lon, lat float64, err error) {
	lon, e1 := strconv.ParseFloat(string(lonB), 64)
	lat, e2 := strconv.ParseFloat(string(latB), 64)
	if e1 != nil || e2 != nil {
		return 0, 0, redis.ErrInvalidFloat
	}
	if !validCoord(lon, lat) {
		return 0, 0, fmt.Errorf("ERR invalid longitude,latitude pair %f,%f", lon, lat)
	}
	return lon, lat, nil
}

// score looks up a member's geohash; ok=false if the member is absent.
func score(red redis.Redka, key, member string) (bits uint64, ok bool, err error) {
	sc, err := red.ZSet().GetScore(key, member)
	if err == core.ErrNotFound {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return uint64(sc), true, nil
}

// GeoAdd adds members with coordinates to a geo set.
// GEOADD key [NX | XX] [CH] longitude latitude member [...]
// https://redis.io/commands/geoadd
type GeoAdd struct {
	redis.BaseCmd
	key    string
	nx, xx bool
	ch     bool
	items  []geoItem
}

type geoItem struct {
	member string
	bits   uint64
}

func ParseGeoAdd(b redis.BaseCmd) (GeoAdd, error) {
	cmd := GeoAdd{BaseCmd: b}
	args := cmd.Args()
	if len(args) < 4 {
		return GeoAdd{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(args[0])
	args = args[1:]
opts:
	for len(args) > 0 {
		switch strings.ToLower(string(args[0])) {
		case "nx":
			cmd.nx = true
		case "xx":
			cmd.xx = true
		case "ch":
			cmd.ch = true
		default:
			break opts
		}
		args = args[1:]
	}
	if cmd.nx && cmd.xx {
		return GeoAdd{}, fmt.Errorf("ERR XX and NX options at the same time are not compatible")
	}
	if len(args) == 0 || len(args)%3 != 0 {
		return GeoAdd{}, redis.ErrSyntaxError
	}
	for ; len(args) > 0; args = args[3:] {
		lon, lat, err := parseCoord(args[0], args[1])
		if err != nil {
			return GeoAdd{}, err
		}
		cmd.items = append(cmd.items, geoItem{string(args[2]), encode(lon, lat)})
	}
	return cmd, nil
}

func (cmd GeoAdd) Run(w redis.Writer, red redis.Redka) (any, error) {
	added, changed := 0, 0
	for _, it := range cmd.items {
		old, err := red.ZSet().GetScore(cmd.key, it.member)
		exists := err == nil
		if err != nil && err != core.ErrNotFound {
			w.WriteError(cmd.Error(err))
			return nil, err
		}
		if (cmd.nx && exists) || (cmd.xx && !exists) {
			continue
		}
		if _, err := red.ZSet().Add(cmd.key, it.member, float64(it.bits)); err != nil {
			w.WriteError(cmd.Error(err))
			return nil, err
		}
		switch {
		case !exists:
			added++
			changed++
		case uint64(old) != it.bits:
			changed++
		}
	}
	n := added
	if cmd.ch {
		n = changed
	}
	w.WriteInt(n)
	return n, nil
}

// GeoPos returns the longitude and latitude of members.
// GEOPOS key [member [member ...]]
// https://redis.io/commands/geopos
type GeoPos struct {
	redis.BaseCmd
	key     string
	members []string
}

func ParseGeoPos(b redis.BaseCmd) (GeoPos, error) {
	cmd := GeoPos{BaseCmd: b}
	if len(cmd.Args()) < 1 {
		return GeoPos{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	for _, a := range cmd.Args()[1:] {
		cmd.members = append(cmd.members, string(a))
	}
	return cmd, nil
}

func (cmd GeoPos) Run(w redis.Writer, red redis.Redka) (any, error) {
	type pos struct {
		ok       bool
		lon, lat float64
	}
	out := make([]pos, len(cmd.members))
	for i, m := range cmd.members {
		bits, ok, err := score(red, cmd.key, m)
		if err != nil {
			w.WriteError(cmd.Error(err))
			return nil, err
		}
		if ok {
			out[i].ok = true
			out[i].lon, out[i].lat = decode(bits)
		}
	}
	w.WriteArray(len(out))
	for _, p := range out {
		if !p.ok {
			w.WriteNull()
			continue
		}
		w.WriteArray(2)
		w.WriteBulkString(fmtCoord(p.lon))
		w.WriteBulkString(fmtCoord(p.lat))
	}
	return nil, nil
}

// GeoDist returns the distance between two members.
// GEODIST key member1 member2 [M | KM | FT | MI]
// https://redis.io/commands/geodist
type GeoDist struct {
	redis.BaseCmd
	key    string
	m1, m2 string
	unit   float64
}

func ParseGeoDist(b redis.BaseCmd) (GeoDist, error) {
	cmd := GeoDist{BaseCmd: b}
	args := cmd.Args()
	if len(args) < 3 || len(args) > 4 {
		return GeoDist{}, redis.ErrInvalidArgNum
	}
	cmd.key, cmd.m1, cmd.m2 = string(args[0]), string(args[1]), string(args[2])
	cmd.unit = 1
	if len(args) == 4 {
		var err error
		if cmd.unit, err = unitMeters(string(args[3])); err != nil {
			return GeoDist{}, err
		}
	}
	return cmd, nil
}

func (cmd GeoDist) Run(w redis.Writer, red redis.Redka) (any, error) {
	b1, ok1, err := score(red, cmd.key, cmd.m1)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	b2, ok2, err := score(red, cmd.key, cmd.m2)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	if !ok1 || !ok2 {
		w.WriteNull()
		return nil, nil
	}
	lon1, lat1 := decode(b1)
	lon2, lat2 := decode(b2)
	s := fmt.Sprintf("%.4f", distance(lon1, lat1, lon2, lat2)/cmd.unit)
	w.WriteBulkString(s)
	return s, nil
}

// GeoHash returns the geohash strings of members.
// GEOHASH key [member [member ...]]
// https://redis.io/commands/geohash
type GeoHash struct {
	redis.BaseCmd
	key     string
	members []string
}

func ParseGeoHash(b redis.BaseCmd) (GeoHash, error) {
	p, err := ParseGeoPos(b)
	return GeoHash{BaseCmd: p.BaseCmd, key: p.key, members: p.members}, err
}

func (cmd GeoHash) Run(w redis.Writer, red redis.Redka) (any, error) {
	out := make([]string, len(cmd.members))
	found := make([]bool, len(cmd.members))
	for i, m := range cmd.members {
		bits, ok, err := score(red, cmd.key, m)
		if err != nil {
			w.WriteError(cmd.Error(err))
			return nil, err
		}
		if ok {
			found[i], out[i] = true, hashString(bits)
		}
	}
	w.WriteArray(len(out))
	for i, s := range out {
		if found[i] {
			w.WriteBulkString(s)
		} else {
			w.WriteNull()
		}
	}
	return nil, nil
}
