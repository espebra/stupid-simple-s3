package version

import "runtime/debug"

// Version is set at build time via ldflags (git tag)
var Version = "dev"

// Commit is set at build time via ldflags (git commit hash)
var Commit = ""

// String returns the version string in the format "version (commit)"
func String() string {
	// Prefer ldflags-injected commit, fall back to build info
	commit := Commit
	if len(commit) > 7 {
		commit = commit[:7]
	}
	dirty := false
	if commit == "" {
		commit, dirty = getBuildInfo()
	}

	if Version == "dev" {
		if commit != "" {
			if dirty {
				return "dev (" + commit + "-dirty)"
			}
			return "dev (" + commit + ")"
		}
		return "dev"
	}

	if commit != "" {
		if dirty {
			return Version + " (" + commit + "-dirty)"
		}
		return Version + " (" + commit + ")"
	}
	return Version
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
