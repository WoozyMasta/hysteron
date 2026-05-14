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
	"time"

	stconfig "github.com/woozymasta/hysteron/internal/config"
	stlog "github.com/woozymasta/hysteron/internal/log"
	"github.com/woozymasta/hysteron/internal/utils/readonly"
)

type runtimeCommonOptions struct {
	Metrics runtimeMetricsOpts `group:"Metrics"`
	clusterSelectionOptions
}

type proxyRuntimeOptions struct {
	ListenAddress string `long:"listen-address" env:"LISTEN_ADDRESS" description:"proxy listening address"`
	Port          string `long:"port"           env:"PORT"           description:"proxy listening port"`

	ReadOnlyListenAddress string                 `long:"read-only-listen-address"  env:"READ_ONLY_LISTEN_ADDRESS"  description:"read-only proxy listening address"`
	ReadOnlyPort          string                 `long:"read-only-port"            env:"READ_ONLY_PORT"            description:"read-only proxy listening port"`
	ReadOnly              readonly.PolicyOptions `group:"Read-Only Routing" namespace:"read-only" env-namespace:"READ_ONLY"`

	DisableWritableListener bool `long:"disable-writable-listener" env:"DISABLE_WRITABLE_LISTENER" description:"disable the writable proxy listener"`
}

type keeperRuntimeOptions struct {
	CanBeMaster             *bool `long:"can-be-master" env:"CAN_BE_MASTER" description:"allow keeper to be elected as master"`
	CanBeSynchronousReplica *bool `long:"can-be-synchronous-replica" env:"CAN_BE_SYNCHRONOUS_REPLICA" description:"allow keeper to be chosen as synchronous replica"`

	PG      keeperPostgresOptions `group:"PostgreSQL" namespace:"pg" env-namespace:"PG"`
	UID     string                `long:"uid" env:"UID" description:"keeper uid (must be unique in the cluster and can contain only lower-case letters, numbers and the underscore character). If not provided a random uid will be generated." short:"i" long-alias:"id"`
	DataDir string                `long:"data-dir" env:"DATA_DIR" description:"data directory" short:"d"`

	DisableDataDirLocking bool `long:"disable-data-dir-locking" env:"DISABLE_DATA_DIR_LOCKING" description:"disable locking on data dir. Warning! It'll cause data corruptions if two keepers are concurrently running with the same data dir."`
	AllowNewerPG          bool `long:"allow-newer-postgres-version" env:"ALLOW_NEWER_POSTGRES_VERSION" description:"allow running with PostgreSQL major versions newer than the highest default-supported major. Older-than-supported versions are always rejected."`
}

type keeperPostgresOptions struct {
	ListenAddress    string `long:"listen-address" env:"LISTEN_ADDRESS" description:"postgresql instance listening address, local address used for the postgres instance. For all network interface, you can set the value to '*'."`
	AdvertiseAddress string `long:"advertise-address" env:"ADVERTISE_ADDRESS" description:"postgresql instance address from outside. Use it to expose ip different than local ip with a NAT networking config"`
	Port             string `long:"port" env:"PORT" description:"postgresql instance listening port" short:"p" default:"5432"`
	AdvertisePort    string `long:"advertise-port" env:"ADVERTISE_PORT" description:"postgresql instance port from outside. Use it to expose port different than local port with a PAT networking config"`
	BinPath          string `long:"bin-path" env:"BIN_PATH" description:"absolute path to postgresql binaries. If empty they will be searched in the current PATH"`

	Repl keeperPostgresReplOptions `group:"PostgreSQL Replication User" namespace:"repl" env-namespace:"REPL"`
	SU   keeperPostgresSUOptions   `group:"PostgreSQL Superuser" namespace:"su" env-namespace:"SU"`
}

type keeperPostgresReplOptions struct {
	AuthMethod   string `long:"auth-method"  env:"AUTH_METHOD"  choices:"md5;scram-sha-256;trust" default:"md5" description:"postgres replication user auth method"`
	Username     string `long:"username"     env:"USERNAME"     description:"postgres replication user name. Required. It'll be created on db initialization. Must be the same for all keepers."`
	Password     string `long:"password"     env:"PASSWORD"     description:"postgres replication user password. Mutually exclusive with --pg-repl-passwordfile. Must be the same for all keepers." xor:"pg-repl-secret"`
	PasswordFile string `long:"passwordfile" env:"PASSWORDFILE" description:"postgres replication user password file. Mutually exclusive with --pg-repl-password. Must be the same for all keepers." xor:"pg-repl-secret"`
}

type keeperPostgresSUOptions struct {
	AuthMethod   string `long:"auth-method"  env:"AUTH_METHOD"  choices:"md5;scram-sha-256;trust" default:"md5" description:"postgres superuser auth method"`
	Username     string `long:"username"     env:"USERNAME"     description:"postgres superuser user name. Defaults to the effective user running keeper. Must be the same for all keepers."`
	Password     string `long:"password"     env:"PASSWORD"     description:"postgres superuser password. Mutually exclusive with --pg-su-passwordfile. Must be the same for all keepers." xor:"pg-su-secret"`
	PasswordFile string `long:"passwordfile" env:"PASSWORDFILE" description:"postgres superuser password file. Mutually exclusive with --pg-su-password. Must be the same for all keepers." xor:"pg-su-secret"`
}

type sentinelWebOptions struct {
	ListenAddress string `long:"listen-address" env:"LISTEN_ADDRESS" description:"web status dashboard listen address, for example 0.0.0.0:8081 (disabled by default)"`
	BasePath      string `long:"base-path" env:"BASE_PATH" default:"/" validate-regex:"^/.*" description:"base path prefix for web UI and API routes"`

	AuthUsername string `long:"auth-username" env:"AUTH_USERNAME" and:"web-auth" description:"optional HTTP Basic auth username for web endpoints"`
	AuthPassword string `long:"auth-password" env:"AUTH_PASSWORD" and:"web-auth" secret:"true" description:"optional HTTP Basic auth password for web endpoints"`

	ReadTimeout  time.Duration `long:"read-timeout" env:"READ_TIMEOUT" default:"5s" validate-min:"0" description:"maximum duration for reading the entire request, including the body"`
	WriteTimeout time.Duration `long:"write-timeout" env:"WRITE_TIMEOUT" default:"10s" validate-min:"0" description:"maximum duration before timing out writes of the response"`

	AllowUnsafeAdminWithoutAuth bool `long:"allow-unsafe-admin-without-auth" env:"ALLOW_UNSAFE_ADMIN_WITHOUT_AUTH" description:"allow admin API endpoints when web auth is disabled (unsafe; intended only for controlled environments)"`
}

type sentinelRuntimeOptions struct {
	InitialClusterSpecFile string   `short:"f" long:"initial-cluster-spec" env:"INITIAL_CLUSTER_SPEC" description:"a file providing the initial cluster specification, used only at cluster initialization, ignored if cluster is already initialized"`
	ClusterSpecFiles       []string `long:"cluster-spec" env:"CLUSTER_SPEC" description:"per-cluster initial cluster specification override as <cluster-name>=<path>; can be repeated"`
}

type rootGlobalOptions struct {
	Log   stlog.FlagGroup `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Debug bool            `long:"debug" env:"DEBUG" hidden:"true" description:"deprecated: forces debug logging"`
}

type managementCommonOptions struct {
	clusterSelectionOptions
	K8s   k8sStoreOptions    `group:"Kubernetes" namespace:"k8s"   env-namespace:"K8S"`
	Store clusterStoreOption `group:"Store"      namespace:"store" env-namespace:"STORE"`
}

type clusterSelectionOptions struct {
	ClusterNames []string `short:"c" long:"cluster-name" env:"CLUSTER_NAME" description:"cluster name"`
}

type clusterStoreOption struct {
	Backend string `long:"backend" env:"BACKEND" choices:"etcd;etcdv3;kubernetes;k8s" validate-non-empty:"true" description:"store backend type"`
	storeConnectionOptions
}

type k8sStoreOptions struct {
	Config       string `long:"config"        env:"CONFIG"        description:"path to kubeconfig file. Overrides $KUBECONFIG"`
	ResourceKind string `long:"resource-kind" env:"RESOURCE_KIND" description:"k8s resource kind used to store cluster data"                            choices:"configmap;secret"`
	ResourceName string `long:"resource-name" env:"RESOURCE_NAME" description:"k8s resource name template; {cluster} is replaced with the cluster name" default:"hysteron-{cluster}"`
	Context      string `long:"context"       env:"CONTEXT"       description:"kubeconfig context name"`
	Namespace    string `long:"namespace"     env:"NAMESPACE"     description:"kubernetes namespace name"`
}

type storeConnectionOptions struct {
	Endpoints     string        `long:"endpoints"       env:"ENDPOINTS"       description:"comma-separated list of store endpoints"`
	Prefix        string        `long:"prefix"          env:"PREFIX"          description:"store base prefix"                                default:"hysteron/cluster"`
	CertFile      string        `long:"cert-file"       env:"CERT_FILE"       description:"certificate file for store client identification"`
	KeyFile       string        `long:"key"             env:"KEY"             description:"private key file for store client identification"`
	CAFile        string        `long:"ca-file"         env:"CA_FILE"         description:"CA bundle for HTTPS-enabled store servers"`
	Timeout       time.Duration `long:"timeout"         env:"TIMEOUT"         description:"store request timeout"                            default:"5s"`
	SkipTLSVerify bool          `long:"skip-tls-verify" env:"SKIP_TLS_VERIFY" description:"skip store certificate verification (insecure)"`
}

type runtimeEtcdOptions struct {
	storeConnectionOptions
}

type runtimeMetricsOpts struct {
	ListenAddress ListenEndpoint `long:"metrics-listen-address" env:"METRICS_LISTEN_ADDRESS" description:"metrics listen address i.e \"0.0.0.0:8080\" (disabled by default)"`
}

func runtimeEtcdConfig(common runtimeCommonOptions, etcd runtimeEtcdOptions) *stconfig.CommonConfig {
	return toRuntimeBackendConfig(common, etcd, k8sStoreOptions{})
}

func runtimeKubernetesConfig(common runtimeCommonOptions, k8s k8sStoreOptions) *stconfig.CommonConfig {
	return toRuntimeBackendConfig(common, runtimeEtcdOptions{}, k8s)
}

func clusterConfig() *stconfig.CommonConfig {
	return toCommonConfig(cfg.Cluster.Common)
}

func failoverConfig() *stconfig.CommonConfig {
	return toCommonConfig(cfg.Failover.Common)
}

func toCommonConfig(opts interface {
	clusterOptions() (clusterStoreOption, k8sStoreOptions, []string)
}) *stconfig.CommonConfig {
	storeOpts, k8sOpts, names := opts.clusterOptions()
	commonConfig := &stconfig.CommonConfig{
		ClusterNames: names,
		Store: stconfig.StoreOptions{
			Backend:       storeOpts.Backend,
			Endpoints:     storeOpts.Endpoints,
			Prefix:        storeOpts.Prefix,
			CertFile:      storeOpts.CertFile,
			KeyFile:       storeOpts.KeyFile,
			CAFile:        storeOpts.CAFile,
			Timeout:       storeOpts.Timeout,
			SkipTLSVerify: storeOpts.SkipTLSVerify,
		},
		K8s: stconfig.K8sOptions{
			Config:       k8sOpts.Config,
			ResourceKind: k8sOpts.ResourceKind,
			ResourceName: k8sOpts.ResourceName,
			Context:      k8sOpts.Context,
			Namespace:    k8sOpts.Namespace,
		},
	}
	applyRootGlobalOptions(commonConfig)
	return commonConfig
}

func toRuntimeBackendConfig(
	common runtimeCommonOptions,
	etcd runtimeEtcdOptions,
	k8s k8sStoreOptions,
) *stconfig.CommonConfig {
	commonConfig := &stconfig.CommonConfig{
		ClusterNames: common.ClusterNames,
		Metrics: stconfig.MetricsOptions{
			ListenAddress: string(common.Metrics.ListenAddress),
		},
		Store: stconfig.StoreOptions{
			Endpoints:     etcd.Endpoints,
			Prefix:        etcd.Prefix,
			CertFile:      etcd.CertFile,
			KeyFile:       etcd.KeyFile,
			CAFile:        etcd.CAFile,
			Timeout:       etcd.Timeout,
			SkipTLSVerify: etcd.SkipTLSVerify,
		},
		K8s: stconfig.K8sOptions{
			Config:       k8s.Config,
			ResourceKind: k8s.ResourceKind,
			ResourceName: k8s.ResourceName,
			Context:      k8s.Context,
			Namespace:    k8s.Namespace,
		},
	}
	applyRootGlobalOptions(commonConfig)
	return commonConfig
}

func (o managementCommonOptions) clusterOptions() (clusterStoreOption, k8sStoreOptions, []string) {
	return o.Store, o.K8s, o.ClusterNames
}

func applyRootGlobalOptions(commonConfig *stconfig.CommonConfig) {
	commonConfig.Log = cfg.Global.Log
	commonConfig.Debug = cfg.Global.Debug
}
