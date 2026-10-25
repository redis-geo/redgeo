package geo

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	redis "github.com/redis-geo/redgeo/redisapi"
	"github.com/redis-geo/redgeo/restypes"
)

// search holds a parsed GEOSEARCH / GEOSEARCHSTORE query.
type search struct {
	member        string // FROMMEMBER
	lon, lat      float64
	fromMember    bool
	fromLonLat    bool
	radius        float64 // BYRADIUS, metres
	byRadius      bool
	width, height float64 // BYBOX, metres
	byBox         bool
	unit          float64
	asc, desc     bool
	count         int
	any           bool
	withCoord     bool
	withDist      bool
	withHash      bool
	storeDist     bool // GEOSEARCHSTORE only
}

var errSearchFrom = errors.New("ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH")
var errSearchBy = errors.New("ERR exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH")

func parseSearch(args [][]byte, store bool) (search, error) {
	var s search
	f := func(b []byte) (float64, error) {
		v, err := strconv.ParseFloat(string(b), 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, redis.ErrInvalidFloat
		}
		return v, nil
	}
	need := func(i, n int) error {
		if i+n >= len(args) {
			return redis.ErrSyntaxError
		}
		return nil
	}
	for i := 0; i < len(args); i++ {
		var err error
		switch strings.ToLower(string(args[i])) {
		case "frommember":
			if err = need(i, 1); err != nil {
				return s, err
			}
			s.member, s.fromMember = string(args[i+1]), true
			i++
		case "fromlonlat":
			if err = need(i, 2); err != nil {
				return s, err
			}
			if s.lon, s.lat, err = parseCoord(args[i+1], args[i+2]); err != nil {
				return s, err
			}
			s.fromLonLat = true
			i += 2
		case "byradius":
			if err = need(i, 2); err != nil {
				return s, err
			}
			if s.radius, err = f(args[i+1]); err != nil {
				return s, err
			}
			if s.unit, err = unitMeters(string(args[i+2])); err != nil {
				return s, err
			}
			s.radius *= s.unit
			s.byRadius = true
			i += 2
		case "bybox":
			if err = need(i, 3); err != nil {
				return s, err
			}
			if s.width, err = f(args[i+1]); err != nil {
				return s, err
			}
			if s.height, err = f(args[i+2]); err != nil {
				return s, err
			}
			if s.unit, err = unitMeters(string(args[i+3])); err != nil {
				return s, err
			}
			s.width, s.height = s.width*s.unit, s.height*s.unit
			s.byBox = true
			i += 3
		case "asc":
			s.asc = true
		case "desc":
			s.desc = true
		case "count":
			if err = need(i, 1); err != nil {
				return s, err
			}
			n, perr := strconv.Atoi(string(args[i+1]))
			if perr != nil {
				return s, redis.ErrInvalidInt
			}
			if n <= 0 {
				return s, errors.New("ERR COUNT must be > 0")
			}
			s.count = n
			i++
			if i+1 < len(args) && strings.EqualFold(string(args[i+1]), "any") {
				s.any = true
				i++
			}
		case "withcoord":
			s.withCoord = true
		case "withdist":
			s.withDist = true
		case "withhash":
			s.withHash = true
		case "storedist":
			if !store {
				return s, redis.ErrSyntaxError
			}
			s.storeDist = true
		default:
			return s, redis.ErrSyntaxError
		}
	}
	if s.fromMember == s.fromLonLat {
		return s, errSearchFrom
	}
	if s.byRadius == s.byBox {
		return s, errSearchBy
	}
	if s.any && s.count == 0 {
		return s, errors.New("ERR the ANY argument requires COUNT argument")
	}
	if store && (s.withCoord || s.withDist || s.withHash) {
		return s, redis.ErrSyntaxError
	}
	return s, nil
}

type hit struct {
	member   string
	bits     uint64
	dist     float64 // metres
	lon, lat float64
}

// run executes the search against a geo set.
func (s search) run(red redis.Redka, key string) ([]hit, error) {
	lon, lat := s.lon, s.lat
	if s.fromMember {
		bits, ok, err := score(red, key, s.member)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.New("ERR could not decode requested zset member")
		}
		lon, lat = decode(bits)
	}
	items, err := red.ZSet().Range(key, restypes.RangeOpts{})
	if err != nil {
		return nil, err
	}
	var hits []hit
	for _, it := range items {
		bits := uint64(it.Score)
		plon, plat := decode(bits)
		d := distance(lon, lat, plon, plat)
		in := false
		if s.byRadius {
			in = d <= s.radius
		} else {
			in = latDistance(plat, lat) <= s.height/2 &&
				distance(plon, plat, lon, plat) <= s.width/2
		}
		if !in {
			continue
		}
		hits = append(hits, hit{string(it.Elem), bits, d, plon, plat})
		if s.any && !s.asc && !s.desc && len(hits) == s.count {
			break
		}
	}
	if s.count > 0 && !s.desc {
		s.asc = true // COUNT implies closest-first
	}
	if s.asc || s.desc {
		sort.SliceStable(hits, func(i, j int) bool {
			if s.desc {
				return hits[i].dist > hits[j].dist
			}
			return hits[i].dist < hits[j].dist
		})
	}
	if s.count > 0 && len(hits) > s.count {
		hits = hits[:s.count]
	}
	return hits, nil
}

// GeoSearch queries members within a radius or box.
// GEOSEARCH key <FROMMEMBER member | FROMLONLAT lon lat>
//
//	<BYRADIUS r unit | BYBOX w h unit> [ASC | DESC] [COUNT n [ANY]]
//	[WITHCOORD] [WITHDIST] [WITHHASH]
//
// https://redis.io/commands/geosearch
type GeoSearch struct {
	redis.BaseCmd
	key string
	q   search
}

func ParseGeoSearch(b redis.BaseCmd) (GeoSearch, error) {
	cmd := GeoSearch{BaseCmd: b}
	if len(cmd.Args()) < 1 {
		return GeoSearch{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	var err error
	cmd.q, err = parseSearch(cmd.Args()[1:], false)
	return cmd, err
}

func (cmd GeoSearch) Run(w redis.Writer, red redis.Redka) (any, error) {
	hits, err := cmd.q.run(red, cmd.key)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	q := cmd.q
	extra := 0
	for _, on := range []bool{q.withDist, q.withHash, q.withCoord} {
		if on {
			extra++
		}
	}
	w.WriteArray(len(hits))
	for _, h := range hits {
		if extra == 0 {
			w.WriteBulkString(h.member)
			continue
		}
		w.WriteArray(1 + extra)
		w.WriteBulkString(h.member)
		if q.withDist {
			w.WriteBulkString(fmt.Sprintf("%.4f", h.dist/q.unit))
		}
		if q.withHash {
			w.WriteInt64(int64(h.bits))
		}
		if q.withCoord {
			w.WriteArray(2)
			w.WriteBulkString(fmtCoord(h.lon))
			w.WriteBulkString(fmtCoord(h.lat))
		}
	}
	return nil, nil
}

// GeoSearchStore is GEOSEARCH storing the result in a sorted set.
// GEOSEARCHSTORE destination source <FROMMEMBER ... | FROMLONLAT ...>
//
//	<BYRADIUS ... | BYBOX ...> [ASC | DESC] [COUNT n [ANY]] [STOREDIST]
//
// https://redis.io/commands/geosearchstore
type GeoSearchStore struct {
	redis.BaseCmd
	dest, src string
	q         search
}

func ParseGeoSearchStore(b redis.BaseCmd) (GeoSearchStore, error) {
	cmd := GeoSearchStore{BaseCmd: b}
	if len(cmd.Args()) < 2 {
		return GeoSearchStore{}, redis.ErrInvalidArgNum
	}
	cmd.dest, cmd.src = string(cmd.Args()[0]), string(cmd.Args()[1])
	var err error
	cmd.q, err = parseSearch(cmd.Args()[2:], true)
	return cmd, err
}

func (cmd GeoSearchStore) Run(w redis.Writer, red redis.Redka) (any, error) {
	hits, err := cmd.q.run(red, cmd.src)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	// The destination is replaced wholesale, whatever its previous type.
	if _, err := red.Key().Delete(cmd.dest); err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	for _, h := range hits {
		sc := float64(h.bits)
		if cmd.q.storeDist {
			sc = h.dist / cmd.q.unit
		}
		if _, err := red.ZSet().Add(cmd.dest, h.member, sc); err != nil {
			w.WriteError(cmd.Error(err))
			return nil, err
		}
	}
	w.WriteInt(len(hits))
	return len(hits), nil
}
