// Package geo implements the GEO* commands on top of the sorted-set storage
// interface: a member's score is its 52-bit interleaved geohash, exactly as in
// Redis, so geo data inherits the zset's per-member HLC-LWW convergence
// (DESIGN §6.6). Searches scan the whole zset (O(n)).
package geo

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	latMax      = 85.05112878
	latMin      = -85.05112878
	lonMax      = 180.0
	lonMin      = -180.0
	earthRadius = 6372797.560856 // metres, as used by Redis
	geoStep     = 26             // bits per coordinate
)

func spread(v uint32) uint64 {
	x := uint64(v)
	x = (x | x<<16) & 0x0000ffff0000ffff
	x = (x | x<<8) & 0x00ff00ff00ff00ff
	x = (x | x<<4) & 0x0f0f0f0f0f0f0f0f
	x = (x | x<<2) & 0x3333333333333333
	x = (x | x<<1) & 0x5555555555555555
	return x
}

func squash(x uint64) uint32 {
	x &= 0x5555555555555555
	x = (x | x>>1) & 0x3333333333333333
	x = (x | x>>2) & 0x0f0f0f0f0f0f0f0f
	x = (x | x>>4) & 0x00ff00ff00ff00ff
	x = (x | x>>8) & 0x0000ffff0000ffff
	x = (x | x>>16) & 0x00000000ffffffff
	return uint32(x)
}

// encodeRange interleaves lat (even bits) and lon (odd bits) within the given
// latitude range.
func encodeRange(lon, lat, latLo, latHi float64) uint64 {
	la := uint32((lat - latLo) / (latHi - latLo) * (1 << geoStep))
	lo := uint32((lon - lonMin) / (lonMax - lonMin) * (1 << geoStep))
	return spread(la) | spread(lo)<<1
}

// validCoord reports whether (lon, lat) is within the Redis geo bounds.
func validCoord(lon, lat float64) bool {
	return lon >= lonMin && lon <= lonMax && lat >= latMin && lat <= latMax
}

// encode returns the 52-bit geohash score of a valid coordinate.
func encode(lon, lat float64) uint64 { return encodeRange(lon, lat, latMin, latMax) }

// decode returns the centre of the geohash cell.
func decode(bits uint64) (lon, lat float64) {
	la, lo := float64(squash(bits)), float64(squash(bits>>1))
	n := float64(uint64(1) << geoStep)
	lat = latMin + (la+0.5)/n*(latMax-latMin)
	lon = lonMin + (lo+0.5)/n*(lonMax-lonMin)
	return math.Max(lonMin, math.Min(lonMax, lon)), math.Max(latMin, math.Min(latMax, lat))
}

const base32hash = "0123456789bcdefghjkmnpqrstuvwxyz"

// hashString is the standard 11-character geohash of the cell's centre.
func hashString(bits uint64) string {
	lon, lat := decode(bits)
	b := encodeRange(lon, lat, -90, 90)
	out := make([]byte, 11)
	for i := 0; i < 11; i++ {
		idx := uint64(0)
		if i < 10 {
			idx = (b >> (52 - (uint(i)+1)*5)) & 0x1f
		}
		out[i] = base32hash[idx]
	}
	return string(out)
}

func rad(d float64) float64 { return d * math.Pi / 180 }

func latDistance(lat1, lat2 float64) float64 {
	return earthRadius * math.Abs(rad(lat2)-rad(lat1))
}

// distance is the haversine distance in metres.
func distance(lon1, lat1, lon2, lat2 float64) float64 {
	v := math.Sin((rad(lon2) - rad(lon1)) / 2)
	if v == 0 {
		return latDistance(lat1, lat2)
	}
	u := math.Sin((rad(lat2) - rad(lat1)) / 2)
	a := u*u + math.Cos(rad(lat1))*math.Cos(rad(lat2))*v*v
	return 2 * earthRadius * math.Asin(math.Sqrt(a))
}

// unitMeters converts a unit name to metres.
func unitMeters(u string) (float64, error) {
	switch strings.ToLower(u) {
	case "m":
		return 1, nil
	case "km":
		return 1000, nil
	case "ft":
		return 0.3048, nil
	case "mi":
		return 1609.34, nil
	}
	return 0, fmt.Errorf("ERR unsupported unit provided. please use M, KM, FT, MI")
}

// fmtCoord prints a coordinate with up to 17 decimals, trailing zeros trimmed.
func fmtCoord(f float64) string {
	s := strconv.FormatFloat(f, 'f', 17, 64)
	if strings.Contains(s, ".") {
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	}
	return s
}
