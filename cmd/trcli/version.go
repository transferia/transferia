package main

import (
	"fmt"
	"runtime/debug"
	"strings"
)

var (
	version = "dev"
	commit  = ""
	date    = ""
)

func getVersionString() string {
	baseOutputVersion := version

	var details []string

	runtimeInfo, ok := debug.ReadBuildInfo()
	if ok {
		details = append(details, fmt.Sprintf("go: %s", runtimeInfo.GoVersion))

		buildTags := "none"
		useVcs := false
		for _, setting := range runtimeInfo.Settings {
			switch setting.Key {
			case "-tags":
				if setting.Value != "" {
					buildTags = setting.Value
				}
				break
			case "vcs.revision":
				if commit == "" {
					commit = setting.Value
					useVcs = true
				}
			case "vcs.time":
				if date == "" {
					date = setting.Value
				}
			}
		}

		if useVcs {
			for _, setting := range runtimeInfo.Settings {
				switch setting.Key {
				case "vcs.modified":
					if setting.Value == "true" {
						commit += "-dirty"
					}
				}
			}
		}

		details = append(details, fmt.Sprintf("tags: %s", buildTags))
	} else {
		details = append(details, "go: unknown", "tags: unknown")
	}

	// Add Git commit if available
	if commit != "" {
		details = append(details, fmt.Sprintf("commit: %s", commit))
	}

	// Add Build date if available
	if date != "" {
		details = append(details, fmt.Sprintf("built: %s", date))
	}

	if len(details) > 0 {
		return fmt.Sprintf("%s (%s)", baseOutputVersion, strings.Join(details, ", "))
	}
	return baseOutputVersion
}
