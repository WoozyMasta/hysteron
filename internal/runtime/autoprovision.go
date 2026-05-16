// Copyright 20[0-9][0-9](?:-20[0-9][0-9])? (?:Sorint\.lab|WoozyMasta)(?:\nCopyright 2026 WoozyMasta)?
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

package runtime

import (
	"os"
	"regexp"
	"strings"
)

var (
	statefulSetOrdinalPattern = regexp.MustCompile(`^(.*)-([0-9]+)$`)
)

func autoProvisionTarget(target Target) Target {
	if target.Keeper == nil {
		return target
	}

	autoProvisionKeeper(target.Keeper, os.LookupEnv, os.Hostname)
	return target
}

func autoProvisionKeeper(
	opts *KeeperOptions,
	lookupEnv func(string) (string, bool),
	hostnameFn func() (string, error),
) {
	if opts == nil {
		return
	}

	if strings.TrimSpace(opts.UID) == "" {
		opts.UID = keeperUIDFromSignals(lookupEnv, hostnameFn)
	}
	if strings.TrimSpace(opts.PG.ListenAddress) == "" {
		if podIP, ok := lookupTrimmedEnv(lookupEnv, "POD_IP"); ok {
			opts.PG.ListenAddress = podIP
		}
	}
}

func keeperUIDFromSignals(
	lookupEnv func(string) (string, bool),
	hostnameFn func() (string, error),
) string {
	if podName, ok := lookupTrimmedEnv(lookupEnv, "POD_NAME"); ok {
		if uid := keeperUIDFromPodName(podName); uid != "" {
			return uid
		}
	}

	hostCandidate := ""
	if host, ok := lookupTrimmedEnv(lookupEnv, "HOSTNAME"); ok {
		hostCandidate = host
	} else if host, err := hostnameFn(); err == nil {
		hostCandidate = strings.TrimSpace(host)
	}

	if hostCandidate == "" {
		return ""
	}
	sanitized := sanitizeUIDToken(hostCandidate)
	if sanitized == "" {
		return ""
	}

	return "keeper_" + sanitized
}

func keeperUIDFromPodName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}

	match := statefulSetOrdinalPattern.FindStringSubmatch(name)
	if len(match) != 3 {
		return ""
	}

	return "keeper" + match[2]
}

func sanitizeUIDToken(in string) string {
	if in == "" {
		return ""
	}

	var b strings.Builder
	b.Grow(len(in))
	for _, r := range strings.ToLower(in) {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return ""
	}

	// Collapse repeated underscores for stable compact IDs.
	for strings.Contains(out, "__") {
		out = strings.ReplaceAll(out, "__", "_")
	}

	return out
}

func lookupTrimmedEnv(
	lookupEnv func(string) (string, bool),
	key string,
) (string, bool) {
	v, ok := lookupEnv(key)
	if !ok {
		return "", false
	}

	v = strings.TrimSpace(v)
	if v == "" {
		return "", false
	}

	return v, true
}
