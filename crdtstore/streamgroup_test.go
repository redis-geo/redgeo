package crdtstore

import (
	"fmt"
	"strings"
	"testing"
)

// ids flattens an XREADGROUP reply for one stream into "id id ...".
func readGroupIDs(t *testing.T, s *Store, consumer string) string {
	t.Helper()
	r := jcmd(t, s, "XREADGROUP", "GROUP", "g", consumer, "STREAMS", "s", ">")
	if r.Kind != '*' || len(r.Arr) == 0 {
		return ""
	}
	var out []string
	for _, e := range r.Arr[0].Arr[1].Arr {
		out = append(out, e.Arr[0].Str)
	}
	return strings.Join(out, " ")
}

// pendingOf renders "id=consumer(count)" for every pending entry.
func pendingOf(t *testing.T, s *Store) string {
	t.Helper()
	r := jcmd(t, s, "XPENDING", "s", "g", "-", "+", "100")
	var out []string
	for _, e := range r.Arr {
		out = append(out, fmt.Sprintf("%s=%s(%d)", e.Arr[0].Str, e.Arr[1].Str, e.Arr[3].Int))
	}
	return strings.Join(out, " ")
}

func lastDelivered(t *testing.T, s *Store) string {
	t.Helper()
	r := jcmd(t, s, "XINFO", "GROUPS", "s")
	if len(r.Arr) == 0 {
		return "<no group>"
	}
	return r.Arr[0].Arr[7].Str
}

func TestStreamGroupReplication(t *testing.T) {
	stores, net := newCluster(t, 2)
	a, b := stores[0], stores[1]
	for i := 1; i <= 4; i++ {
		jcmd(t, a, "XADD", "s", fmt.Sprintf("%d-1", i), "n", fmt.Sprint(i))
	}
	eventually(t, "r1 has the stream", func() bool { return jcmd(t, b, "XLEN", "s").Int == 4 })

	// A group created on one replica appears on the other.
	if r := jcmd(t, a, "XGROUP", "CREATE", "s", "g", "0"); r.Str != "OK" {
		t.Fatalf("XGROUP CREATE = %+v", r)
	}
	eventually(t, "r1 sees the group", func() bool { return lastDelivered(t, b) == "0-0" })

	// Without a partition delivery is exactly-once: a delivered entry is not
	// handed out again once the other replica has seen the pending record.
	if got := readGroupIDs(t, a, "alice"); got != "1-1 2-1 3-1 4-1" {
		t.Fatalf("alice = %q", got)
	}
	eventually(t, "r1 sees alice's deliveries", func() bool { return lastDelivered(t, b) == "4-1" })
	if got := readGroupIDs(t, b, "bob"); got != "" {
		t.Fatalf("bob re-received %q after replication", got)
	}
	// An ack on one replica clears the entry everywhere.
	if r := jcmd(t, b, "XACK", "s", "g", "1-1", "2-1"); r.Int != 2 {
		t.Fatalf("XACK = %+v", r)
	}
	eventually(t, "r0 sees the acks", func() bool { return pendingOf(t, a) == "3-1=alice(1) 4-1=alice(1)" })

	// While partitioned, both replicas can deliver the same new entries —
	// at-least-once. After healing the state converges and nothing is re-sent.
	jcmd(t, a, "XADD", "s", "5-1", "n", "5")
	jcmd(t, a, "XADD", "s", "6-1", "n", "6")
	eventually(t, "r1 has 5,6", func() bool { return jcmd(t, b, "XLEN", "s").Int == 6 })
	net.SetPartitioned(0, true)
	net.SetPartitioned(1, true)
	gotA := readGroupIDs(t, a, "alice")
	gotB := readGroupIDs(t, b, "bob")
	if gotA != "5-1 6-1" || gotB != "5-1 6-1" {
		t.Fatalf("expected both replicas to deliver 5,6 while partitioned: alice=%q bob=%q", gotA, gotB)
	}
	net.SetPartitioned(0, false)
	net.SetPartitioned(1, false)
	eventually(t, "pending lists converge", func() bool {
		pa, pb := pendingOf(t, a), pendingOf(t, b)
		return pa == pb && strings.Contains(pa, "5-1=") && strings.Contains(pa, "6-1=") && lastDelivered(t, a) == "6-1" && lastDelivered(t, b) == "6-1"
	})
	if g1, g2 := readGroupIDs(t, a, "alice"), readGroupIDs(t, b, "bob"); g1 != "" || g2 != "" {
		t.Fatalf("redelivery after convergence: %q %q", g1, g2)
	}

	// SETID rewinds the group on every replica (its epoch beats older ids).
	jcmd(t, a, "XGROUP", "SETID", "s", "g", "3-1")
	eventually(t, "r1 sees the rewind", func() bool { return lastDelivered(t, b) == "3-1" })
	if got := readGroupIDs(t, b, "bob"); got != "4-1 5-1 6-1" {
		t.Fatalf("after SETID bob = %q", got)
	}
	eventually(t, "r0 sees bob's reads", func() bool { return lastDelivered(t, a) == "6-1" })

	// Destroying the group stops reads everywhere.
	if r := jcmd(t, a, "XGROUP", "DESTROY", "s", "g"); r.Int != 1 {
		t.Fatalf("DESTROY = %+v", r)
	}
	eventually(t, "r1 sees the group gone", func() bool {
		r := jcmd(t, b, "XREADGROUP", "GROUP", "g", "bob", "STREAMS", "s", ">")
		return r.Kind == '-' && strings.HasPrefix(r.Str, "NOGROUP")
	})
}
