package version

import "testing"

func TestGetUsesInjectedValues(t *testing.T) {
	defer func(v, c string) { Version, Commit = v, c }(Version, Commit)
	Version, Commit = "v1.2.3", "abc1234def5678"

	version, commit := Get()
	if version != "v1.2.3" || commit != "abc1234" {
		t.Errorf("Get() = %q, %q; want %q, %q", version, commit, "v1.2.3", "abc1234")
	}
	if got := String(); got != "stupid-simple-s3 v1.2.3 (abc1234)" {
		t.Errorf("String() = %q", got)
	}
}
