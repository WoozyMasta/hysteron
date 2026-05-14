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

package readonly

import "github.com/woozymasta/hysteron/internal/utils/units"

// PolicyOptions defines shared read-only routing policy flags.
type PolicyOptions struct {
	ReplicaPriority ReplicaPriority  `env:"REPLICA_PRIORITY" long:"replica-priority" description:"read-only replica priority policy" default:"sync" choices:"sync;async;any"`
	MaxLag          units.BytesValue `env:"MAX_LAG"          long:"max-lag"          description:"maximum standby WAL lag in bytes for read-only routing" default:"0"`
	NoFallback      bool             `env:"NO_FALLBACK"      long:"no-fallback"      description:"do not route read-only connections to primary when no eligible standby exists" xor:"read-only-primary-policy"`
	IncludePrimary  bool             `env:"INCLUDE_PRIMARY"  long:"include-primary"  description:"include primary in the normal read-only backend pool" xor:"read-only-primary-policy"`
}
