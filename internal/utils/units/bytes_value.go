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

package units

import (
	"fmt"
	"strconv"
	"strings"

	humanize "github.com/dustin/go-humanize"
)

// BytesValue parses human-readable byte values for CLI flags.
type BytesValue uint64

// MarshalFlag implements flags.Marshaler.
func (v BytesValue) MarshalFlag() (string, error) {
	return strconv.FormatUint(uint64(v), 10), nil
}

// UnmarshalFlag implements flags.Unmarshaler.
func (v *BytesValue) UnmarshalFlag(value string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fmt.Errorf("%w: empty value", ErrInvalidBytesValue)
	}
	parsed, err := humanize.ParseBytes(trimmed)
	if err != nil {
		return fmt.Errorf("%w %q: %v", ErrInvalidBytesValue, value, err)
	}
	*v = BytesValue(parsed)
	return nil
}
