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

package v0

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"
)

const (
	// DefaultProxyCheckInterval is the default legacy proxy check interval.
	DefaultProxyCheckInterval = 5 * time.Second
	// DefaultRequestTimeout is the default legacy request timeout.
	DefaultRequestTimeout = 10 * time.Second
	// DefaultSleepInterval is the default legacy sleep interval.
	DefaultSleepInterval = 5 * time.Second
	// DefaultKeeperFailInterval is the default legacy keeper failure interval.
	DefaultKeeperFailInterval = 20 * time.Second
	// DefaultMaxStandbysPerSender is the default legacy max standbys per sender.
	DefaultMaxStandbysPerSender = 3
	// DefaultSynchronousReplication is the default legacy synchronous replication setting.
	DefaultSynchronousReplication = false
	// DefaultInitWithMultipleKeepers is the default legacy multiple-keeper init setting.
	DefaultInitWithMultipleKeepers = false
	// DefaultUsePGRewind is the default legacy pg_rewind setting.
	DefaultUsePGRewind = false
)

// NilConfig is the legacy nullable cluster config.
type NilConfig struct {
	// RequestTimeout is request timeout override.
	RequestTimeout *Duration `json:"request_timeout,omitempty"`
	// SleepInterval is main loop interval override.
	SleepInterval *Duration `json:"sleep_interval,omitempty"`
	// KeeperFailInterval is keeper failure threshold override.
	KeeperFailInterval *Duration `json:"keeper_fail_interval,omitempty"`
	// MaxStandbysPerSender is max standbys per sender override.
	MaxStandbysPerSender *uint `json:"max_standbys_per_sender,omitempty"`
	// SynchronousReplication toggles synchronous replication.
	SynchronousReplication *bool `json:"synchronous_replication,omitempty"`
	// InitWithMultipleKeepers enables random initial master with multiple keepers.
	InitWithMultipleKeepers *bool `json:"init_with_multiple_keepers,omitempty"`
	// UsePGRewind toggles pg_rewind usage.
	UsePGRewind *bool `json:"use_pg_rewind,omitempty"`
	// PGParameters overrides postgres parameters.
	PGParameters *map[string]string `json:"pg_parameters,omitempty"`
}

// Config is the legacy cluster config with defaults applied.
type Config struct {
	// Map of postgres parameters
	PGParameters map[string]string
	// Time after which any request (keepers checks from sentinel etc...) will fail.
	RequestTimeout time.Duration
	// Interval to wait before next check (for every component: keeper, sentinel, proxy).
	SleepInterval time.Duration
	// Interval after the first fail to declare a keeper as not healthy.
	KeeperFailInterval time.Duration
	// Max number of standbys for every sender. A sender can be a master or
	// another standby (with cascading replication).
	MaxStandbysPerSender uint
	// Use Synchronous replication between master and its standbys
	SynchronousReplication bool
	// Choose a random initial master when multiple keeper are registered
	InitWithMultipleKeepers bool
	// Whether to use pg_rewind
	UsePGRewind bool
}

// StringP returns a pointer to s.
//
//go:fix inline
func StringP(s string) *string {
	return new(s)
}

// UintP returns a pointer to u.
//
//go:fix inline
func UintP(u uint) *uint {
	return new(u)
}

// BoolP returns a pointer to b.
//
//go:fix inline
func BoolP(b bool) *bool {
	return new(b)
}

// DurationP returns a pointer to d.
//
//go:fix inline
func DurationP(d Duration) *Duration {
	return new(d)
}

// MapStringP returns a pointer to a copy of m.
func MapStringP(m map[string]string) *map[string]string {
	nm := map[string]string{}
	maps.Copy(nm, m)
	return &nm
}

type nilConfig NilConfig

// UnmarshalJSON decodes and validates legacy nullable config.
func (c *NilConfig) UnmarshalJSON(in []byte) error {
	var nc nilConfig
	if err := json.Unmarshal(in, &nc); err != nil {
		return err
	}
	*c = NilConfig(nc)
	if err := c.Validate(); err != nil {
		return fmt.Errorf("config validation failed: %v", err)
	}
	return nil
}

// Copy returns an independent copy of nullable config.
func (c *NilConfig) Copy() *NilConfig {
	if c == nil {
		return c
	}
	var nc NilConfig
	if c.RequestTimeout != nil {
		nc.RequestTimeout = new(*c.RequestTimeout)
	}
	if c.SleepInterval != nil {
		nc.SleepInterval = new(*c.SleepInterval)
	}
	if c.KeeperFailInterval != nil {
		nc.KeeperFailInterval = new(*c.KeeperFailInterval)
	}
	if c.MaxStandbysPerSender != nil {
		nc.MaxStandbysPerSender = new(*c.MaxStandbysPerSender)
	}
	if c.SynchronousReplication != nil {
		nc.SynchronousReplication = new(*c.SynchronousReplication)
	}
	if c.InitWithMultipleKeepers != nil {
		nc.InitWithMultipleKeepers = new(*c.InitWithMultipleKeepers)
	}
	if c.UsePGRewind != nil {
		nc.UsePGRewind = new(*c.UsePGRewind)
	}
	if c.PGParameters != nil {
		nc.PGParameters = MapStringP(*c.PGParameters)
	}
	return &nc
}

// Copy returns an independent copy of config.
func (c *Config) Copy() *Config {
	if c == nil {
		return c
	}
	// Just copy by dereferencing c, the PGParameters map won't be a real deep copy.
	nc := *c
	// Do a real deeep copy of the PGParameters map
	nm := map[string]string{}
	maps.Copy(nm, c.PGParameters)
	nc.PGParameters = nm
	return &nc
}

// Duration is needed to be able to marshal/unmarshal json strings with time
// unit (eg. 3s, 100ms) instead of ugly times in nanoseconds.
type Duration struct {
	time.Duration
}

// MarshalJSON encodes Duration as a Go duration string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// UnmarshalJSON decodes Duration from a Go duration string.
func (d *Duration) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	du, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = du
	return nil
}

// Validate validates nullable config.
func (c *NilConfig) Validate() error {
	if c.RequestTimeout != nil && c.RequestTimeout.Duration < 0 {
		return errors.New("request_timeout must be positive")
	}
	if c.SleepInterval != nil && c.SleepInterval.Duration < 0 {
		return errors.New("sleep_interval must be positive")
	}
	if c.KeeperFailInterval != nil && c.KeeperFailInterval.Duration < 0 {
		return errors.New("keeper_fail_interval must be positive")
	}
	if c.MaxStandbysPerSender != nil && *c.MaxStandbysPerSender < 1 {
		return errors.New("max_standbys_per_sender must be at least 1")
	}
	return nil
}

// MergeDefaults fills absent nullable config fields with defaults.
func (c *NilConfig) MergeDefaults() {
	if c.RequestTimeout == nil {
		c.RequestTimeout = &Duration{DefaultRequestTimeout}
	}
	if c.SleepInterval == nil {
		c.SleepInterval = &Duration{DefaultSleepInterval}
	}
	if c.KeeperFailInterval == nil {
		c.KeeperFailInterval = &Duration{DefaultKeeperFailInterval}
	}
	if c.MaxStandbysPerSender == nil {
		c.MaxStandbysPerSender = new(uint(DefaultMaxStandbysPerSender))
	}
	if c.SynchronousReplication == nil {
		c.SynchronousReplication = new(DefaultSynchronousReplication)
	}
	if c.InitWithMultipleKeepers == nil {
		c.InitWithMultipleKeepers = new(DefaultInitWithMultipleKeepers)
	}
	if c.UsePGRewind == nil {
		c.UsePGRewind = new(DefaultUsePGRewind)
	}
	if c.PGParameters == nil {
		c.PGParameters = &map[string]string{}
	}
}

// ToConfig returns config with defaults applied.
func (c *NilConfig) ToConfig() *Config {
	nc := c.Copy()
	nc.MergeDefaults()
	return &Config{
		RequestTimeout:          nc.RequestTimeout.Duration,
		SleepInterval:           nc.SleepInterval.Duration,
		KeeperFailInterval:      nc.KeeperFailInterval.Duration,
		MaxStandbysPerSender:    *nc.MaxStandbysPerSender,
		SynchronousReplication:  *nc.SynchronousReplication,
		InitWithMultipleKeepers: *nc.InitWithMultipleKeepers,
		UsePGRewind:             *nc.UsePGRewind,
		PGParameters:            *nc.PGParameters,
	}
}

// NewDefaultConfig returns legacy config populated with defaults.
func NewDefaultConfig() *Config {
	nc := &NilConfig{}
	nc.MergeDefaults()
	return nc.ToConfig()
}
