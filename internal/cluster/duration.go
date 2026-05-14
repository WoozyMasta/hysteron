// Copyright 2016 Sorint.lab
// Copyright 2026 WoozyMasta
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

package cluster

import (
	"encoding/json"
	"strings"
	"time"
)

// Duration marshals/unmarshals to JSON/text as a Go duration string
// (for example "3s", "100ms") instead of raw nanoseconds.
type Duration struct {
	time.Duration
}

// MarshalJSON encodes Duration as a Go duration string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// MarshalText encodes Duration as a Go duration string.
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

// UnmarshalJSON decodes Duration from a Go duration string.
func (d *Duration) UnmarshalJSON(b []byte) error {
	return d.UnmarshalText([]byte(strings.Trim(string(b), `"`)))
}

// UnmarshalText decodes Duration from a Go duration string.
func (d *Duration) UnmarshalText(text []byte) error {
	parsedDuration, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	d.Duration = parsedDuration
	return nil
}
