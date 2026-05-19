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

package health

import "slices"

// RouteGroup identifies a route family that can be bound to a listener.
type RouteGroup string

const (
	// RouteGroupWeb serves UI and API routes.
	RouteGroupWeb RouteGroup = "web"
	// RouteGroupMetrics serves Prometheus metrics routes.
	RouteGroupMetrics RouteGroup = "metrics"
	// RouteGroupHealth serves health probe routes.
	RouteGroupHealth RouteGroup = "health"
)

// ListenerPlan maps a listen address to the route groups that must be
// registered on that listener.
type ListenerPlan map[string][]RouteGroup

// BuildListenerPlan groups route families by listen address. Empty addresses
// are skipped. Group order inside each address is deterministic.
func BuildListenerPlan(addresses map[RouteGroup]string) ListenerPlan {
	plan := make(ListenerPlan)
	for group, addr := range addresses {
		if addr == "" {
			continue
		}
		plan[addr] = append(plan[addr], group)
	}
	for _, groups := range plan {
		slices.Sort(groups)
	}
	return plan
}
