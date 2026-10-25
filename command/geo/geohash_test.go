package geo

import (
	"math"
	"testing"
)

func TestGeohashRoundTrip(t *testing.T) {
	for _, c := range [][2]float64{{13.361389, 38.115556}, {-122.4194, 37.7749}, {0, 0}, {180, 85}, {-180, -85}} {
		lon, lat := decode(encode(c[0], c[1]))
		// 26 bits per axis: cells are ~0.6 m, so well under 1e-5 degrees.
		if math.Abs(lon-c[0]) > 1e-5 || math.Abs(lat-c[1]) > 1e-5 {
			t.Fatalf("%v round-tripped to %v,%v", c, lon, lat)
		}
	}
	if encode(13.361389, 38.115556) != 3479099956230698 {
		t.Fatalf("Palermo score = %d, want Redis's 3479099956230698", encode(13.361389, 38.115556))
	}
	if h := hashString(encode(13.361389, 38.115556)); h != "sqc8b49rny0" {
		t.Fatalf("hash = %s", h)
	}
}
