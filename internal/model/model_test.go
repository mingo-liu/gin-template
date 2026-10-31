package model

import "testing"

func TestDecimal(t *testing.T) {
	for _, v := range []string{"01", "-1", "1.0", "1e3", "", "9007199254740992"} {
		if _, err := Decimal(v, MaxNumber, false); err == nil {
			t.Errorf("accepted %q", v)
		}
	}
	if _, err := Decimal("18446744073709551615", ^uint64(0), true); err != nil {
		t.Fatal(err)
	}
}
func TestEvent(t *testing.T) {
	e := Event{SchemaVersion: 1, EventID: "1:2001:g7:123-0", Generation: "g7", StreamID: "123-0", UserID: "18446744073709551615", TargetID: "2001", TargetType: 1, Action: "LIKE", Version: "3"}
	if err := e.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Event){func(e *Event) { e.Action = "TOGGLE" }, func(e *Event) { e.Version = "0" }, func(e *Event) { e.EventID = "other" }, func(e *Event) { e.TargetType = 4 }, func(e *Event) { e.StreamID = "123-01" }} {
		bad := e
		mutate(&bad)
		if bad.Validate() == nil {
			t.Errorf("accepted invalid event: %+v", bad)
		}
	}
}
