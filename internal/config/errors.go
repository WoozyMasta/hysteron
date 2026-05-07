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

package config

import "errors"

var (
	// ErrClusterNameRequired reports missing cluster name.
	ErrClusterNameRequired = errors.New("cluster name required")
	// ErrExactlyOneClusterNameRequired reports multi-value input for single-name flows.
	ErrExactlyOneClusterNameRequired = errors.New("exactly one cluster name required")
	// ErrKubernetesResourceKindRequired reports missing resource kind for kubernetes backend.
	ErrKubernetesResourceKindRequired = errors.New("unspecified kubernetes resource kind")
	// ErrKubernetesResourceNameRequired reports missing resource name for kubernetes backend.
	ErrKubernetesResourceNameRequired = errors.New("unspecified kubernetes resource name")
)
