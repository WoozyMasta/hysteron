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

package commands

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// ListenEndpoint validates listen endpoint values in host:port form.
type ListenEndpoint string

// MarshalFlag implements flags.Marshaler.
func (e ListenEndpoint) MarshalFlag() (string, error) {
	return string(e), nil
}

// UnmarshalFlag implements flags.Unmarshaler.
func (e *ListenEndpoint) UnmarshalFlag(value string) error {
	if value == "" {
		*e = ""
		return nil
	}
	host, port, err := net.SplitHostPort(value)
	if err != nil {
		return fmt.Errorf("%w %q: %v", ErrInvalidListenEndpoint, value, err)
	}
	if strings.ContainsAny(host, " \t\r\n") {
		return fmt.Errorf("%w %q: host contains whitespace", ErrInvalidListenEndpoint, value)
	}
	portNum, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("%w %q: invalid port", ErrInvalidListenEndpoint, value)
	}
	if portNum < 0 || portNum > 65535 {
		return fmt.Errorf("%w %q: port out of range", ErrInvalidListenEndpoint, value)
	}
	*e = ListenEndpoint(value)
	return nil
}
