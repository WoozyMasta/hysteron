// Copyright 2015 Sorint.lab
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

// Package postgresql implements PostgreSQL process and protocol helpers.
package postgresql

import (
	"errors"
	"fmt"
	"maps"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"unicode"

	"github.com/woozymasta/hysteron/internal/common"
)

// This parses PostgreSQL libpq-style keyword/value connection strings.

// ConnParams is a map of PostgreSQL connection string parameters.
type ConnParams map[string]string

// Set stores a connection parameter value.
func (cp ConnParams) Set(k, v string) {
	cp[k] = v
}

// Get returns a connection parameter value.
func (cp ConnParams) Get(k string) (v string) {
	return cp[k]
}

// Del removes a connection parameter.
func (cp ConnParams) Del(k string) {
	delete(cp, k)
}

// Isset reports whether a connection parameter exists.
func (cp ConnParams) Isset(k string) bool {
	_, ok := cp[k]
	return ok
}

// Equals reports whether two connection parameter maps are equal.
func (cp ConnParams) Equals(cp2 ConnParams) bool {
	return reflect.DeepEqual(cp, cp2)
}

// Copy returns a shallow copy of the connection parameter map.
func (cp ConnParams) Copy() ConnParams {
	ncp := ConnParams{}
	maps.Copy(ncp, cp)
	return ncp
}

// scanner implements a tokenizer for libpq-style option strings.
type scanner struct {
	s []rune
	i int
}

// newScanner returns a new scanner initialized with the option string s.
func newScanner(s string) *scanner {
	return &scanner{[]rune(s), 0}
}

// Next returns the next rune.
// It returns 0, false if the end of the text has been reached.
func (s *scanner) Next() (rune, bool) {
	if s.i >= len(s.s) {
		return 0, false
	}
	r := s.s[s.i]
	s.i++
	return r, true
}

// SkipSpaces returns the next non-whitespace rune.
// It returns 0, false if the end of the text has been reached.
func (s *scanner) SkipSpaces() (rune, bool) {
	r, ok := s.Next()
	for unicode.IsSpace(r) && ok {
		r, ok = s.Next()
	}
	return r, ok
}

// ParseConnString parses the options from name and adds them to the values.
//
// The parsing code is based on conninfo_parse from libpq's fe-connect.c
func ParseConnString(name string) (ConnParams, error) {
	p := make(ConnParams)
	s := newScanner(name)

	for {
		var (
			keyRunes, valRunes []rune
			r                  rune
			ok                 bool
		)

		if r, ok = s.SkipSpaces(); !ok {
			break
		}

		// Scan the key
		for !unicode.IsSpace(r) && r != '=' {
			keyRunes = append(keyRunes, r)
			if r, ok = s.Next(); !ok {
				break
			}
		}

		// Skip any whitespace if we're not at the = yet
		if r != '=' {
			r, ok = s.SkipSpaces()
		}

		// The current character should be =
		if r != '=' || !ok {
			return nil, fmt.Errorf(`missing "=" after %q in connection info string"`, string(keyRunes))
		}

		// Skip any whitespace after the =
		if r, ok = s.SkipSpaces(); !ok {
			// If we reach the end here, the last value is just an empty string as per libpq.
			p.Set(string(keyRunes), "")
			break
		}

		if r != '\'' {
			for !unicode.IsSpace(r) {
				if r == '\\' {
					if r, ok = s.Next(); !ok {
						return nil, errors.New(`missing character after backslash`)
					}
				}
				valRunes = append(valRunes, r)

				if r, ok = s.Next(); !ok {
					break
				}
			}
		} else {
		quote:
			for {
				if r, ok = s.Next(); !ok {
					return nil, errors.New(`unterminated quoted string literal in connection string`)
				}
				switch r {
				case '\'':
					break quote
				case '\\':
					r, _ = s.Next()
					fallthrough
				default:
					valRunes = append(valRunes, r)
				}
			}
		}

		p.Set(string(keyRunes), string(valRunes))
	}

	return p, nil
}

// URLToConnParams creates the connParams from the url.
func URLToConnParams(urlStr string) (ConnParams, error) {
	p := make(ConnParams)
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "postgres" {
		return nil, fmt.Errorf("invalid connection protocol: %s", u.Scheme)
	}

	if u.User != nil {
		v := u.User.Username()
		p.Set("user", v)
		v, _ = u.User.Password()
		p.Set("password", v)
	}

	i := strings.Index(u.Host, ":")
	if i < 0 {
		p.Set("host", u.Host)
	} else {
		p.Set("host", u.Host[:i])
		p.Set("port", u.Host[i+1:])
	}

	if u.Path != "" {
		p.Set("dbname", u.Path[1:])
	}

	q := u.Query()
	for k := range q {
		p.Set(k, q.Get(k))
	}

	return p, nil
}

// ConnString returns a connection string, its entries are sorted so the
// returned string can be reproducible and comparable
func (cp ConnParams) ConnString() string {
	var kvs []string
	escaper := strings.NewReplacer(` `, `\ `, `'`, `\'`, `\`, `\\`)
	for k, v := range cp {
		if v != "" {
			kvs = append(kvs, k+"="+escaper.Replace(v))
		}
	}
	sort.Strings(kvs)
	return strings.Join(kvs, " ")
}

// LogSummaryConnParams returns allow-listed libpq connection fields for structured logs.
// Password values are never included; password_set is true when a password key is present.
func LogSummaryConnParams(cp ConnParams) map[string]any {
	if len(cp) == 0 {
		return nil
	}
	allow := []string{
		"host", "hostaddr", "port", "user", "dbname", "database",
		"application_name", "sslmode", "connect_timeout", "replication",
		"fallback_application_name", "target_session_attrs",
	}
	out := make(map[string]any)
	for _, k := range allow {
		if v := cp[k]; v != "" {
			out[k] = v
		}
	}
	if _, ok := cp["password"]; ok {
		out["password_set"] = true
	}
	return out
}

// LogSummaryRecoveryParameters returns PostgreSQL recovery parameters safe for logs.
// primary_conninfo is parsed and summarized without secrets.
func LogSummaryRecoveryParameters(p common.Parameters) map[string]any {
	if len(p) == 0 {
		return nil
	}
	out := make(map[string]any)
	for k, v := range p {
		lk := strings.ToLower(k)
		if strings.Contains(lk, "password") || lk == "passfile" {
			if v != "" {
				out[k] = "[redacted]"
			}
			continue
		}
		if k == "primary_conninfo" {
			if v == "" {
				continue
			}
			cp, err := ParseConnString(v)
			if err != nil {
				out[k] = map[string]any{"parse_error": err.Error()}
			} else {
				out[k] = LogSummaryConnParams(cp)
			}
			continue
		}
		out[k] = v
	}
	return out
}
