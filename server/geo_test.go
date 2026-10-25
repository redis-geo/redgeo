package server

import (
	"strings"
	"testing"
)

func TestGeo(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	want := func(s string, args ...string) {
		t.Helper()
		if r := cl.do(t, args...); r.Str != s {
			t.Fatalf("%v = %+v want %q", args, r, s)
		}
	}
	want("2", "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	want("0", "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo") // unchanged
	want("0", "GEOADD", "Sicily", "NX", "13", "38", "Palermo")
	want("1", "GEOADD", "Sicily", "CH", "13.361389", "38.115556", "Palermo2")
	want("166274.1516", "GEODIST", "Sicily", "Palermo", "Catania")
	want("166.2742", "GEODIST", "Sicily", "Palermo", "Catania", "km")
	want("103.3182", "GEODIST", "Sicily", "Palermo", "Catania", "mi")
	if r := cl.do(t, "GEODIST", "Sicily", "Palermo", "nope"); !r.Null {
		t.Fatalf("GEODIST missing = %+v", r)
	}

	r := cl.do(t, "GEOHASH", "Sicily", "Palermo", "Catania", "nope")
	if len(r.Arr) != 3 || r.Arr[0].Str != "sqc8b49rny0" || r.Arr[1].Str != "sqdtr74hyu0" || !r.Arr[2].Null {
		t.Fatalf("GEOHASH = %+v", r)
	}
	r = cl.do(t, "GEOPOS", "Sicily", "Palermo", "nope")
	if len(r.Arr) != 2 || !strings.HasPrefix(r.Arr[0].Arr[0].Str, "13.3613893389701") ||
		!strings.HasPrefix(r.Arr[0].Arr[1].Str, "38.1155563954962") || !r.Arr[1].Null {
		t.Fatalf("GEOPOS = %+v", r)
	}

	// Radius search with distances, closest first.
	r = cl.do(t, "GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "WITHDIST", "ASC")
	if len(r.Arr) != 3 || r.Arr[0].Arr[0].Str != "Catania" || r.Arr[0].Arr[1].Str != "56.4413" ||
		r.Arr[1].Arr[0].Str != "Palermo" || r.Arr[1].Arr[1].Str != "190.4424" {
		t.Fatalf("GEOSEARCH radius = %+v", r)
	}
	r = cl.do(t, "GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "100", "km")
	if len(r.Arr) != 1 || r.Arr[0].Str != "Catania" {
		t.Fatalf("GEOSEARCH 100km = %+v", r)
	}
	// Box search from a member, with COUNT.
	r = cl.do(t, "GEOSEARCH", "Sicily", "FROMMEMBER", "Palermo", "BYBOX", "400", "400", "km", "ASC", "COUNT", "2", "WITHHASH")
	if len(r.Arr) != 2 || r.Arr[0].Arr[0].Str != "Palermo" || r.Arr[0].Arr[1].Kind != ':' {
		t.Fatalf("GEOSEARCH box = %+v", r)
	}

	// STORE variants.
	want("2", "GEOSEARCHSTORE", "near", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC", "COUNT", "2", "STOREDIST")
	if r := cl.do(t, "ZSCORE", "near", "Catania"); r.Str[:5] != "56.44" {
		t.Fatalf("stored dist = %+v", r)
	}
	want("1", "GEOSEARCHSTORE", "near2", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "100", "km")

	// Errors.
	for _, bad := range [][]string{
		{"GEOADD", "k", "181", "0", "m"},
		{"GEOADD", "k", "0", "86", "m"},
		{"GEOADD", "k", "NX", "XX", "0", "0", "m"},
		{"GEOADD", "k", "0", "0"},
		{"GEODIST", "Sicily", "Palermo", "Catania", "parsec"},
		{"GEOSEARCH", "Sicily", "BYRADIUS", "1", "km"},
		{"GEOSEARCH", "Sicily", "FROMLONLAT", "0", "0", "BYRADIUS", "1", "km", "BYBOX", "1", "1", "km"},
		{"GEOSEARCH", "Sicily", "FROMMEMBER", "nope", "BYRADIUS", "1", "km"},
		{"GEOSEARCH", "Sicily", "FROMLONLAT", "0", "0", "BYRADIUS", "1", "km", "COUNT", "0"},
		{"GEOSEARCHSTORE", "d", "Sicily", "FROMLONLAT", "0", "0", "BYRADIUS", "1", "km", "WITHDIST"},
	} {
		if r := cl.do(t, bad...); r.Kind != '-' {
			t.Fatalf("%v = %+v want error", bad, r)
		}
	}
	cl.do(t, "SET", "str", "x")
	if r := cl.do(t, "GEOPOS", "str", "a"); r.Kind != '-' {
		t.Fatalf("GEOPOS on string = %+v", r)
	}
	// Missing key: empty results.
	if r := cl.do(t, "GEOSEARCH", "nokey", "FROMLONLAT", "0", "0", "BYRADIUS", "1", "km"); r.Kind != '*' || len(r.Arr) != 0 {
		t.Fatalf("GEOSEARCH missing = %+v", r)
	}
}
