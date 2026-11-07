package guard

import "testing"

func TestPolicyActive(t *testing.T) {
	cases := []struct {
		p    Policy
		want bool
	}{
		{Policy{}, false},
		{Policy{Nice: 10}, true},
		{Policy{IOPrio: "idle"}, true},
	}
	for _, c := range cases {
		if got := c.p.Active(); got != c.want {
			t.Errorf("%+v.Active() = %v, want %v", c.p, got, c.want)
		}
	}
}

func TestPolicyDescribe(t *testing.T) {
	cases := []struct {
		p    Policy
		want string
	}{
		{Policy{Nice: 10}, "nice 10"},
		{Policy{Nice: -5, IOPrio: "idle"}, "nice -5, io idle"},
		{Policy{IOPrio: "best-effort:7"}, "io best-effort:7"},
	}
	for _, c := range cases {
		if got := c.p.Describe(); got != c.want {
			t.Errorf("%+v.Describe() = %q, want %q", c.p, got, c.want)
		}
	}
}

// TestApplyIsInertWithoutPolicy pins the property that matters most: a host
// that did not ask for a lower priority must not get one. A dedicated mirror
// server should use every core it has.
func TestApplyIsInertWithoutPolicy(t *testing.T) {
	if err := Apply(Policy{}); err != nil {
		t.Fatalf("Apply(zero policy) = %v, want nil", err)
	}
}
