package version

import "runtime/debug"

// Version is set at build time via ldflags (git tag)
var Version = "dev"

// Commit is set at build time via ldflags (git commit hash)
var Commit = ""

// Get returns the version and the short commit hash of the running binary.
// The commit carries a "-dirty" suffix when built from a modified working
// tree, and is empty when unknown.
func Get() (version, commit string) {
	// Prefer ldflags-injected commit, fall back to build info
	commit = Commit
	if len(commit) > 7 {
		commit = commit[:7]
	}
	dirty := false
	if commit == "" {
		commit, dirty = getBuildInfo()
	}
	if dirty {
		commit += "-dirty"
	}
	return Version, commit
}

// String returns the version string in the format "stupid-simple-s3 version (commit)"
func String() string {
	version, commit := Get()
	if commit != "" {
		return "stupid-simple-s3 " + version + " (" + commit + ")"
	}
	return "stupid-simple-s3 " + version
}

// getBuildInfo extracts VCS info from Go's embedded build info
func getBuildInfo() (commit string, dirty bool) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", false
	}

	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			commit = setting.Value
			if len(commit) > 7 {
				commit = commit[:7]
			}
		case "vcs.modified":
			dirty = setting.Value == "true"
		}
	}
	return commit, dirty
}
