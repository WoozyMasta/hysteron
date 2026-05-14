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
	"unicode"
)

// ParsePostgreSQLBytes preserves wal_keep_size semantics used by PostgreSQL:
// a bare integer means megabytes, not bytes.
// Non-PostgreSQL suffixes (for example "MiB") are rejected.
func ParsePostgreSQLBytes(value string) (uint64, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, fmt.Errorf("%w: empty value", ErrInvalidBytesValue)
	}

	i := 0
	for i < len(trimmed) && unicode.IsDigit(rune(trimmed[i])) {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("%w %q: invalid numeric prefix", ErrInvalidBytesValue, value)
	}

	n, err := strconv.ParseUint(trimmed[:i], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w %q: parse numeric prefix: %v", ErrInvalidBytesValue, value, err)
	}

	unit := strings.ToUpper(strings.TrimSpace(trimmed[i:]))
	switch unit {
	case "":
		// PostgreSQL wal_keep_size defaults to MB when unit is omitted.
		return n * 1024 * 1024, nil
	case "B":
		return n, nil
	case "KB":
		return n * 1024, nil
	case "MB":
		return n * 1024 * 1024, nil
	case "GB":
		return n * 1024 * 1024 * 1024, nil
	case "TB":
		return n * 1024 * 1024 * 1024 * 1024, nil
	default:
		return 0, fmt.Errorf("%w %q: unsupported unit %q", ErrInvalidBytesValue, value, unit)
	}
}
