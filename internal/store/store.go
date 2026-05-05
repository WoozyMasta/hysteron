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

package store

import (
	"context"
	"errors"
	"time"

	"github.com/sorintlab/stolon/internal/cluster"
)

//go:generate mockgen -destination=../mock/store/store.go -source=$GOFILE

var (
	// ErrKeyNotFound is thrown when the key is not found in the store during a Get operation
	ErrKeyNotFound = errors.New("key not found in store")
	// ErrKeyModified reports optimistic write conflicts.
	ErrKeyModified = errors.New("unable to complete atomic operation, key modified")
	// ErrElectionNoLeader reports missing election leader.
	ErrElectionNoLeader = errors.New("election: no leader")
	// ErrElectionAlreadyRunning reports a duplicate RunForElection call.
	ErrElectionAlreadyRunning = errors.New("election: already running")
	// ErrElectionNotRunning reports Stop when no election is active.
	ErrElectionNotRunning = errors.New("election: not running")
)

// Store stores and retrieves cluster state and component heartbeats.
type Store interface {
	AtomicPutClusterData(ctx context.Context, cd *cluster.ClusterData, previous *KVPair) (*KVPair, error)
	PutClusterData(ctx context.Context, cd *cluster.ClusterData) error
	GetClusterData(ctx context.Context) (*cluster.ClusterData, *KVPair, error)
	SetKeeperInfo(ctx context.Context, id string, ms *cluster.KeeperInfo, ttl time.Duration) error
	GetKeepersInfo(ctx context.Context) (cluster.KeepersInfo, error)
	SetSentinelInfo(ctx context.Context, si *cluster.SentinelInfo, ttl time.Duration) error
	GetSentinelsInfo(ctx context.Context) (cluster.SentinelsInfo, error)
	SetProxyInfo(ctx context.Context, pi *cluster.ProxyInfo, ttl time.Duration) error
	GetProxiesInfo(ctx context.Context) (cluster.ProxiesInfo, error)
}

// Election coordinates leader election among sentinel instances.
type Election interface {
	// WARNING: If the election error channel receives any error, it is vital that
	// the consuming code calls election.Stop(). Failure to do so can cause
	// subsequent elections to hang indefinitely across all participants of an
	// election.
	RunForElection() (electedCh <-chan bool, errCh <-chan error, err error)
	Leader() (string, error)
	Stop() error
}
