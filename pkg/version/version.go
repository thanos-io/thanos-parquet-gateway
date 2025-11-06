// Copyright (c) The Thanos Authors.
// Licensed under the Apache License, Version 2.0.

package version

import (
	"fmt"
	"runtime/debug"

	promversion "github.com/prometheus/common/version"
)

// Build information. Populated at build-time.
var (
	Version   = "unknown"
	Revision  = "unknown"
	Branch    = "unknown"
	BuildUser = "unknown"
	BuildDate = "unknown"
)

// Info returns version, branch and revision information in a Prometheus-compatible format.
func Info() string {
	return promversion.Info()
}

// BuildContext returns build context information.
func BuildContext() string {
	return promversion.BuildContext()
}

// Print returns version information for thanos-parquet-gateway.
func Print() string {
	return promversion.Print("thanos-parquet-gateway")
}

// GetVersion returns the version string.
func GetVersion() string {
	if Version != "unknown" {
		return Version
	}
	// Fallback to build info if version wasn't injected
	if buildInfo, ok := debug.ReadBuildInfo(); ok {
		return buildInfo.Main.Version
	}
	return "unknown"
}

// GetRevision returns the revision string.
func GetRevision() string {
	if Revision != "unknown" {
		return Revision
	}
	// Fallback to build info if revision wasn't injected
	if buildInfo, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range buildInfo.Settings {
			if setting.Key == "vcs.revision" {
				if len(setting.Value) > 7 {
					return setting.Value[:7] // Short hash like git
				}
				return setting.Value
			}
		}
	}
	return "unknown"
}

// GetBranch returns the branch string.
func GetBranch() string {
	return Branch
}

// GetBuildUser returns the build user string.
func GetBuildUser() string {
	return BuildUser
}

// GetBuildDate returns the build date string.
func GetBuildDate() string {
	return BuildDate
}

// UserAgent returns a user agent string for HTTP requests.
func UserAgent() string {
	return fmt.Sprintf("thanos-parquet-gateway/%s", GetVersion())
}

func init() {
	// Set version information in prometheus/common's version variables
	// so that the Prometheus version package functions work correctly
	promversion.Version = GetVersion()
	promversion.Revision = GetRevision()
	promversion.Branch = GetBranch()
	promversion.BuildUser = GetBuildUser()
	promversion.BuildDate = GetBuildDate()
}
