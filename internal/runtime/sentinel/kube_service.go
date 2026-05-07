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

package sentinel

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"net"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
	"github.com/sorintlab/stolon/internal/cluster"
	"github.com/sorintlab/stolon/internal/common"
	runtimecommon "github.com/sorintlab/stolon/internal/runtime/common"
	k8sutil "github.com/sorintlab/stolon/internal/utils/k8s"
	readonly "github.com/sorintlab/stolon/internal/utils/readonly"
	units "github.com/sorintlab/stolon/internal/utils/units"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

const (
	kubeEndpointSliceManagedBy  = "stolon-sentinel"
	kubeServicePortName         = "postgres"
	kubeEndpointSliceSuffix     = "-stolon"
	kubeEndpointSliceHashLength = 8
)

type kubeServicePublishingOptions struct {
	ServiceName             string                   `long:"kube-service-name" env:"KUBE_SERVICE_NAME" default:"{resource}" validate-non-empty:"true" description:"Kubernetes Service name used for writable PostgreSQL traffic; {cluster} and {resource} are replaced with the cluster name and Kubernetes resource name"`
	ReadOnlyServiceName     string                   `long:"kube-read-only-service-name" env:"KUBE_READ_ONLY_SERVICE_NAME" default:"{resource}-ro" validate-non-empty:"true" description:"Kubernetes Service name used for read-only PostgreSQL traffic; {cluster} and {resource} are replaced with the cluster name and Kubernetes resource name"`
	ReadOnlyReplicaPriority readonly.ReplicaPriority `long:"kube-read-only-replica-priority" env:"KUBE_READ_ONLY_REPLICA_PRIORITY" default:"sync" choices:"sync;async;any" description:"read-only replica priority policy"`
	ReadOnlyMaxLag          units.BytesValue         `long:"kube-read-only-max-lag" env:"KUBE_READ_ONLY_MAX_LAG" default:"0" description:"maximum standby WAL lag in bytes for read-only Service publishing"`
	ServicePort             int32                    `long:"kube-service-port" env:"KUBE_SERVICE_PORT" default:"5432" validate-min:"1" validate-max:"65535" description:"Kubernetes Service port exposed for writable PostgreSQL traffic"`
	ReadOnlyServicePort     int32                    `long:"kube-read-only-service-port" env:"KUBE_READ_ONLY_SERVICE_PORT" default:"5432" validate-min:"1" validate-max:"65535" description:"Kubernetes Service port exposed for read-only PostgreSQL traffic"`
	Enabled                 bool                     `long:"kube-service-publishing" env:"KUBE_SERVICE_PUBLISHING" description:"publish the current writable PostgreSQL endpoint through a Kubernetes Service and EndpointSlice"`
	ReadOnlyEnabled         bool                     `long:"kube-read-only-service-publishing" env:"KUBE_READ_ONLY_SERVICE_PUBLISHING" description:"publish read-only PostgreSQL endpoints through a Kubernetes Service and EndpointSlice"`
	ReadOnlyNoFallback      bool                     `long:"kube-read-only-no-fallback" env:"KUBE_READ_ONLY_NO_FALLBACK" xor:"kube-read-only-primary-policy" description:"do not publish primary as read-only endpoint when no eligible standby exists"`
	ReadOnlyIncludePrimary  bool                     `long:"kube-read-only-include-primary" env:"KUBE_READ_ONLY_INCLUDE_PRIMARY" xor:"kube-read-only-primary-policy" description:"include primary in the normal read-only endpoint pool"`
}

type kubeServicePublisher struct {
	log                    zerolog.Logger
	client                 kubernetes.Interface
	namespace              string
	clusterName            string
	writableServiceName    string
	readOnlyServiceName    string
	readOnlyPriority       readonly.ReplicaPriority
	readOnlyMaxLag         units.BytesValue
	writableServicePort    int32
	readOnlyServicePort    int32
	readOnlyEnabled        bool
	readOnlyNoFallback     bool
	readOnlyIncludePrimary bool
}

type kubeEndpoint struct {
	address string
	port    int32
}

func newKubeServicePublisher(cfg *config, clusterName string, logger zerolog.Logger) (*kubeServicePublisher, error) {
	if !cfg.KubeService.Enabled && !cfg.KubeService.ReadOnlyEnabled {
		return nil, nil
	}
	resourceName, err := runtimecommon.KubeResourceNameForCluster(&cfg.CommonConfig, clusterName)
	if err != nil {
		return nil, err
	}
	writableServiceName := strings.ReplaceAll(cfg.KubeService.ServiceName, "{cluster}", clusterName)
	writableServiceName = strings.ReplaceAll(writableServiceName, "{resource}", resourceName)
	if cfg.KubeService.Enabled {
		if errs := k8svalidation.IsDNS1035Label(writableServiceName); len(errs) != 0 {
			return nil, fmt.Errorf("invalid kubernetes writable service name %q: %s", writableServiceName, strings.Join(errs, "; "))
		}
	}
	readOnlyServiceName := strings.ReplaceAll(cfg.KubeService.ReadOnlyServiceName, "{cluster}", clusterName)
	readOnlyServiceName = strings.ReplaceAll(readOnlyServiceName, "{resource}", resourceName)
	if cfg.KubeService.ReadOnlyEnabled {
		if errs := k8svalidation.IsDNS1035Label(readOnlyServiceName); len(errs) != 0 {
			return nil, fmt.Errorf("invalid kubernetes read-only service name %q: %s", readOnlyServiceName, strings.Join(errs, "; "))
		}
	}

	kubeClientConfig := k8sutil.NewKubeClientConfig(cfg.Kube.Config, cfg.Kube.Context, cfg.Kube.Namespace)
	kubecfg, err := kubeClientConfig.ClientConfig()
	if err != nil {
		return nil, err
	}
	namespace, _, err := kubeClientConfig.Namespace()
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(kubecfg)
	if err != nil {
		return nil, fmt.Errorf("cannot create kubernetes client: %w", err)
	}

	return &kubeServicePublisher{
		client:                 client,
		log:                    logger,
		namespace:              namespace,
		clusterName:            clusterName,
		writableServiceName:    writableServiceName,
		writableServicePort:    cfg.KubeService.ServicePort,
		readOnlyEnabled:        cfg.KubeService.ReadOnlyEnabled,
		readOnlyServiceName:    readOnlyServiceName,
		readOnlyServicePort:    cfg.KubeService.ReadOnlyServicePort,
		readOnlyPriority:       cfg.KubeService.ReadOnlyReplicaPriority,
		readOnlyMaxLag:         cfg.KubeService.ReadOnlyMaxLag,
		readOnlyNoFallback:     cfg.KubeService.ReadOnlyNoFallback,
		readOnlyIncludePrimary: cfg.KubeService.ReadOnlyIncludePrimary,
	}, nil
}

func (p *kubeServicePublisher) Publish(ctx context.Context, cd *cluster.ClusterData) error {
	if p == nil {
		return nil
	}
	if err := p.publishWritable(ctx, cd); err != nil {
		return err
	}
	if err := p.publishReadOnly(ctx, cd); err != nil {
		return err
	}
	return nil
}

func (p *kubeServicePublisher) publishWritable(ctx context.Context, cd *cluster.ClusterData) error {
	if p.writableServiceName == "" {
		return nil
	}
	endpoint, err := p.writableEndpoint(cd)
	if err != nil {
		return err
	}
	service, err := p.ensureService(ctx, p.writableServiceName, p.writableServicePort, "writable")
	if err != nil {
		return err
	}
	if err := p.ensureEndpointSlice(ctx, service, []*kubeEndpoint{endpoint}, p.writableServicePort, "writable"); err != nil {
		return err
	}
	if endpoint == nil {
		p.log.Info().
			Str("service_name", p.writableServiceName).
			Msg("published empty writable Kubernetes Service endpoints")
		return nil
	}
	p.log.Info().
		Str("service_name", p.writableServiceName).
		Str("endpoint_address", endpoint.address).
		Int32("endpoint_port", endpoint.port).
		Msg("published writable Kubernetes Service endpoint")
	return nil
}

func (p *kubeServicePublisher) publishReadOnly(ctx context.Context, cd *cluster.ClusterData) error {
	if !p.readOnlyEnabled {
		return nil
	}
	endpoints := p.readOnlyEndpoints(cd)
	service, err := p.ensureService(ctx, p.readOnlyServiceName, p.readOnlyServicePort, "read-only")
	if err != nil {
		return err
	}
	if err := p.ensureEndpointSlice(ctx, service, endpoints, p.readOnlyServicePort, "read-only"); err != nil {
		return err
	}
	p.log.Info().
		Str("service_name", p.readOnlyServiceName).
		Int("endpoint_count", len(endpoints)).
		Msg("published read-only Kubernetes Service endpoints")
	return nil
}

func (p *kubeServicePublisher) writableEndpoint(cd *cluster.ClusterData) (*kubeEndpoint, error) {
	if cd == nil || cd.Proxy == nil || cd.Proxy.Spec.MasterDBUID == "" {
		return nil, nil
	}
	db := cd.DBs[cd.Proxy.Spec.MasterDBUID]
	if db == nil {
		return nil, nil
	}
	if db.Status.ListenAddress == "" || db.Status.Port == "" {
		return nil, nil
	}
	ip := net.ParseIP(db.Status.ListenAddress)
	if ip == nil {
		return nil, fmt.Errorf("writable database listen address %q is not an IP address", db.Status.ListenAddress)
	}
	port, err := strconv.ParseInt(db.Status.Port, 10, 32)
	if err != nil || port <= 0 || port > 65535 {
		return nil, fmt.Errorf("invalid writable database port %q", db.Status.Port)
	}
	return &kubeEndpoint{address: ip.String(), port: int32(port)}, nil
}

func (p *kubeServicePublisher) ensureService(ctx context.Context, serviceName string, servicePort int32, mode string) (*corev1.Service, error) {
	services := p.client.CoreV1().Services(p.namespace)
	service, err := services.Get(ctx, serviceName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		service = &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:   serviceName,
				Labels: p.labels(),
			},
			Spec: corev1.ServiceSpec{
				Ports: []corev1.ServicePort{{
					Name:     kubeServicePortName,
					Protocol: corev1.ProtocolTCP,
					Port:     servicePort,
				}},
				Type: corev1.ServiceTypeClusterIP,
			},
		}
		return services.Create(ctx, service, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, fmt.Errorf("cannot get %s Kubernetes Service: %w", mode, err)
	}
	if len(service.Spec.Selector) != 0 {
		return nil, fmt.Errorf("%s Kubernetes Service %q has a selector and cannot be managed by Stolon", mode, serviceName)
	}

	desiredPorts := []corev1.ServicePort{{
		Name:     kubeServicePortName,
		Protocol: corev1.ProtocolTCP,
		Port:     servicePort,
	}}
	if reflect.DeepEqual(service.Spec.Ports, desiredPorts) && labelsContain(service.Labels, p.labels()) {
		return service, nil
	}
	updated := service.DeepCopy()
	updated.Spec.Ports = desiredPorts
	if updated.Labels == nil {
		updated.Labels = map[string]string{}
	}
	maps.Copy(updated.Labels, p.labels())
	return services.Update(ctx, updated, metav1.UpdateOptions{})
}

func (p *kubeServicePublisher) ensureEndpointSlice(
	ctx context.Context,
	service *corev1.Service,
	endpoints []*kubeEndpoint,
	servicePort int32,
	mode string,
) error {
	slices := p.client.DiscoveryV1().EndpointSlices(p.namespace)
	name := p.endpointSliceName(service.Name)
	desired := p.endpointSlice(service, endpoints, servicePort)
	var err error
	current, err := slices.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = slices.Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return fmt.Errorf("cannot get %s Kubernetes EndpointSlice: %w", mode, err)
	}

	updated := current.DeepCopy()
	updated.Labels = desired.Labels
	updated.OwnerReferences = desired.OwnerReferences
	updated.AddressType = desired.AddressType
	updated.Ports = desired.Ports
	updated.Endpoints = desired.Endpoints
	if reflect.DeepEqual(current.Labels, updated.Labels) &&
		reflect.DeepEqual(current.OwnerReferences, updated.OwnerReferences) &&
		reflect.DeepEqual(current.AddressType, updated.AddressType) &&
		reflect.DeepEqual(current.Ports, updated.Ports) &&
		reflect.DeepEqual(current.Endpoints, updated.Endpoints) {
		return nil
	}
	_, err = slices.Update(ctx, updated, metav1.UpdateOptions{})
	return err
}

func (p *kubeServicePublisher) endpointSlice(service *corev1.Service, endpoints []*kubeEndpoint, servicePort int32) *discoveryv1.EndpointSlice {
	protocol := corev1.ProtocolTCP
	portName := kubeServicePortName
	endpointPort := servicePort
	addressType := discoveryv1.AddressTypeIPv4
	sliceEndpoints := []discoveryv1.Endpoint{}
	filtered := make([]*kubeEndpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint != nil {
			filtered = append(filtered, endpoint)
		}
	}
	if len(filtered) > 0 {
		endpointPort = filtered[0].port
		if strings.Contains(filtered[0].address, ":") {
			addressType = discoveryv1.AddressTypeIPv6
		}
		for _, endpoint := range filtered {
			if endpoint.port != endpointPort {
				p.log.Warn().
					Str("service_name", service.Name).
					Int32("expected_port", endpointPort).
					Int32("endpoint_port", endpoint.port).
					Msg("skipping endpoint with mismatched port for EndpointSlice")
				continue
			}
			endpointType := discoveryv1.AddressTypeIPv4
			if strings.Contains(endpoint.address, ":") {
				endpointType = discoveryv1.AddressTypeIPv6
			}
			if endpointType != addressType {
				p.log.Warn().
					Str("service_name", service.Name).
					Str("endpoint_address", endpoint.address).
					Msg("skipping endpoint with mismatched address family for EndpointSlice")
				continue
			}
			ready := true
			sliceEndpoints = append(sliceEndpoints, discoveryv1.Endpoint{
				Addresses:  []string{endpoint.address},
				Conditions: discoveryv1.EndpointConditions{Ready: &ready},
			})
		}
	}
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:            p.endpointSliceName(service.Name),
			Labels:          p.endpointSliceLabels(service.Name),
			OwnerReferences: p.ownerReferences(service),
		},
		AddressType: addressType,
		Ports: []discoveryv1.EndpointPort{{
			Name:     &portName,
			Protocol: &protocol,
			Port:     &endpointPort,
		}},
		Endpoints: sliceEndpoints,
	}
}

func (p *kubeServicePublisher) endpointSliceName(serviceName string) string {
	name := serviceName + kubeEndpointSliceSuffix
	if errs := k8svalidation.IsDNS1123Label(name); len(errs) == 0 {
		return name
	}
	sum := sha256.Sum256([]byte(serviceName))
	hash := hex.EncodeToString(sum[:])[:kubeEndpointSliceHashLength]
	maxPrefix := 63 - len(hash) - 1
	prefix := strings.TrimRight(serviceName[:min(len(serviceName), maxPrefix)], "-")
	return prefix + "-" + hash
}

func (p *kubeServicePublisher) labels() map[string]string {
	return map[string]string{
		k8sutil.KubeClusterLabel:       p.clusterName,
		"app.kubernetes.io/managed-by": "stolon",
		"app.kubernetes.io/component":  "stolon-service-publishing",
	}
}

func (p *kubeServicePublisher) endpointSliceLabels(serviceName string) map[string]string {
	labels := p.labels()
	labels[discoveryv1.LabelServiceName] = serviceName
	labels[discoveryv1.LabelManagedBy] = kubeEndpointSliceManagedBy
	return labels
}

func (p *kubeServicePublisher) ownerReferences(service *corev1.Service) []metav1.OwnerReference {
	return []metav1.OwnerReference{{
		APIVersion: "v1",
		Kind:       "Service",
		Name:       service.Name,
		UID:        service.UID,
	}}
}

func labelsContain(have map[string]string, want map[string]string) bool {
	for key, value := range want {
		if have[key] != value {
			return false
		}
	}
	return true
}

func (p *kubeServicePublisher) readOnlyEndpoints(cd *cluster.ClusterData) []*kubeEndpoint {
	primary, ok := primaryDB(cd)
	if !ok {
		return nil
	}

	syncStandbys, asyncStandbys := p.readOnlyStandbyCandidates(cd, primary)
	selected := readonly.SelectPriority(p.readOnlyPriority, syncStandbys, asyncStandbys)
	if p.readOnlyIncludePrimary {
		if endpoint, ok := endpointFromDB(primary); ok {
			selected = append(selected, endpoint)
		}
	}
	if len(selected) == 0 && !p.readOnlyNoFallback {
		if endpoint, ok := endpointFromDB(primary); ok {
			p.log.Info().
				Str("service_name", p.readOnlyServiceName).
				Str("db_uid", primary.UID).
				Uint64("max_lag", uint64(p.readOnlyMaxLag)).
				Msg("read-only Kubernetes Service falling back to primary")
			selected = append(selected, endpoint)
		}
	}
	return selected
}

func (p *kubeServicePublisher) readOnlyStandbyCandidates(cd *cluster.ClusterData, primary *cluster.DB) ([]*kubeEndpoint, []*kubeEndpoint) {
	syncStandbySet := map[string]struct{}{}
	for _, dbUID := range primary.Status.SynchronousStandbys {
		syncStandbySet[dbUID] = struct{}{}
	}

	dbUIDs := make([]string, 0, len(cd.DBs))
	for dbUID := range cd.DBs {
		dbUIDs = append(dbUIDs, dbUID)
	}
	sort.Strings(dbUIDs)

	syncStandbys := make([]*kubeEndpoint, 0)
	asyncStandbys := make([]*kubeEndpoint, 0)
	for _, dbUID := range dbUIDs {
		db := cd.DBs[dbUID]
		if db == nil || db.UID == primary.UID || db.Spec == nil {
			continue
		}
		if db.Spec.Role != common.RoleStandby || !readonly.DBStatusEligible(db) {
			continue
		}
		lag := readonly.XLogLag(primary.Status.XLogPos, db.Status.XLogPos)
		if lag > uint64(p.readOnlyMaxLag) {
			continue
		}
		endpoint, ok := endpointFromDB(db)
		if !ok {
			continue
		}
		if _, ok := syncStandbySet[db.UID]; ok {
			syncStandbys = append(syncStandbys, endpoint)
		} else {
			asyncStandbys = append(asyncStandbys, endpoint)
		}
	}
	return syncStandbys, asyncStandbys
}

func primaryDB(cd *cluster.ClusterData) (*cluster.DB, bool) {
	if cd == nil || cd.Proxy == nil || cd.Proxy.Spec.MasterDBUID == "" {
		return nil, false
	}
	db := cd.DBs[cd.Proxy.Spec.MasterDBUID]
	if db == nil {
		return nil, false
	}
	return db, true
}

func endpointFromDB(db *cluster.DB) (*kubeEndpoint, bool) {
	if db == nil || db.Status.ListenAddress == "" || db.Status.Port == "" {
		return nil, false
	}
	ip := net.ParseIP(db.Status.ListenAddress)
	if ip == nil {
		return nil, false
	}
	port, err := strconv.ParseInt(db.Status.Port, 10, 32)
	if err != nil || port <= 0 || port > 65535 {
		return nil, false
	}
	return &kubeEndpoint{address: ip.String(), port: int32(port)}, true
}
