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

// Uint16P returns a pointer to u.
func Uint16P(u uint16) *uint16 {
	return new(u)
}

// Uint32P returns a pointer to u.
func Uint32P(u uint32) *uint32 {
	return new(u)
}

// BoolP returns a pointer to b.
func BoolP(b bool) *bool {
	return new(b)
}

// PGParameters maps PostgreSQL parameter names to values.
type PGParameters map[string]string
