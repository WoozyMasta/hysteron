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

package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"net"
	"reflect"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
	commoncmd "github.com/sorintlab/stolon/cmd"
	"github.com/sorintlab/stolon/internal/cluster"
	"github.com/sorintlab/stolon/internal/util"
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
	ServiceName string `long:"kube-service-name" env:"KUBE_SERVICE_NAME" default:"{resource}-rw" validate-non-empty:"true" description:"Kubernetes Service name used for writable PostgreSQL traffic; {cluster} and {resource} are replaced with the cluster name and Kubernetes resource name"`
	ServicePort int32  `long:"kube-service-port" env:"KUBE_SERVICE_PORT" default:"5432" validate-min:"1" validate-max:"65535" description:"Kubernetes Service port exposed for writable PostgreSQL traffic"`
	Enabled     bool   `long:"kube-service-publishing" env:"KUBE_SERVICE_PUBLISHING" description:"publish the current writable PostgreSQL endpoint through a Kubernetes Service and EndpointSlice"`
}

type kubeServicePublisher struct {
	client      kubernetes.Interface
	log         zerolog.Logger
	namespace   string
	clusterName string
	serviceName string
	servicePort int32
}

type kubeEndpoint struct {
	address string
	port    int32
}

func newKubeServicePublisher(cfg *config, clusterName string, logger zerolog.Logger) (*kubeServicePublisher, error) {
	if !cfg.KubeService.Enabled {
		return nil, nil
	}
	resourceName, err := commoncmd.KubeResourceNameForCluster(&cfg.CommonConfig, clusterName)
	if err != nil {
		return nil, err
	}
	serviceName := strings.ReplaceAll(cfg.KubeService.ServiceName, "{cluster}", clusterName)
	serviceName = strings.ReplaceAll(serviceName, "{resource}", resourceName)
	if errs := k8svalidation.IsDNS1035Label(serviceName); len(errs) != 0 {
		return nil, fmt.Errorf("invalid kubernetes writable service name %q: %s", serviceName, strings.Join(errs, "; "))
	}

	kubeClientConfig := util.NewKubeClientConfig(cfg.Kube.Config, cfg.Kube.Context, cfg.Kube.Namespace)
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
		client:      client,
		log:         logger,
		namespace:   namespace,
		clusterName: clusterName,
		serviceName: serviceName,
		servicePort: cfg.KubeService.ServicePort,
	}, nil
}

func (p *kubeServicePublisher) PublishWritable(ctx context.Context, cd *cluster.ClusterData) error {
	if p == nil {
		return nil
	}
	endpoint, err := p.writableEndpoint(cd)
	if err != nil {
		return err
	}
	service, err := p.ensureWritableService(ctx)
	if err != nil {
		return err
	}
	if err := p.ensureWritableEndpointSlice(ctx, service, endpoint); err != nil {
		return err
	}
	if endpoint == nil {
		p.log.Info().
			Str("service_name", p.serviceName).
			Msg("published empty writable Kubernetes Service endpoints")
		return nil
	}
	p.log.Info().
		Str("service_name", p.serviceName).
		Str("endpoint_address", endpoint.address).
		Int32("endpoint_port", endpoint.port).
		Msg("published writable Kubernetes Service endpoint")
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

func (p *kubeServicePublisher) ensureWritableService(ctx context.Context) (*corev1.Service, error) {
	services := p.client.CoreV1().Services(p.namespace)
	service, err := services.Get(ctx, p.serviceName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		service = &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:   p.serviceName,
				Labels: p.labels(),
			},
			Spec: corev1.ServiceSpec{
				Ports: []corev1.ServicePort{{
					Name:     kubeServicePortName,
					Protocol: corev1.ProtocolTCP,
					Port:     p.servicePort,
				}},
				Type: corev1.ServiceTypeClusterIP,
			},
		}
		return services.Create(ctx, service, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, fmt.Errorf("cannot get writable Kubernetes Service: %w", err)
	}
	if len(service.Spec.Selector) != 0 {
		return nil, fmt.Errorf("writable Kubernetes Service %q has a selector and cannot be managed by Stolon", p.serviceName)
	}

	desiredPorts := []corev1.ServicePort{{
		Name:     kubeServicePortName,
		Protocol: corev1.ProtocolTCP,
		Port:     p.servicePort,
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

func (p *kubeServicePublisher) ensureWritableEndpointSlice(
	ctx context.Context,
	service *corev1.Service,
	endpoint *kubeEndpoint,
) error {
	slices := p.client.DiscoveryV1().EndpointSlices(p.namespace)
	name := p.endpointSliceName()
	desired := p.endpointSlice(service, endpoint)
	current, err := slices.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = slices.Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return fmt.Errorf("cannot get writable Kubernetes EndpointSlice: %w", err)
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

func (p *kubeServicePublisher) endpointSlice(service *corev1.Service, endpoint *kubeEndpoint) *discoveryv1.EndpointSlice {
	protocol := corev1.ProtocolTCP
	portName := kubeServicePortName
	endpointPort := p.servicePort
	addressType := discoveryv1.AddressTypeIPv4
	endpoints := []discoveryv1.Endpoint{}
	if endpoint != nil {
		endpointPort = endpoint.port
		if strings.Contains(endpoint.address, ":") {
			addressType = discoveryv1.AddressTypeIPv6
		}
		ready := true
		endpoints = append(endpoints, discoveryv1.Endpoint{
			Addresses:  []string{endpoint.address},
			Conditions: discoveryv1.EndpointConditions{Ready: &ready},
		})
	}
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:            p.endpointSliceName(),
			Labels:          p.endpointSliceLabels(),
			OwnerReferences: p.ownerReferences(service),
		},
		AddressType: addressType,
		Ports: []discoveryv1.EndpointPort{{
			Name:     &portName,
			Protocol: &protocol,
			Port:     &endpointPort,
		}},
		Endpoints: endpoints,
	}
}

func (p *kubeServicePublisher) endpointSliceName() string {
	name := p.serviceName + kubeEndpointSliceSuffix
	if errs := k8svalidation.IsDNS1123Label(name); len(errs) == 0 {
		return name
	}
	sum := sha256.Sum256([]byte(p.serviceName))
	hash := hex.EncodeToString(sum[:])[:kubeEndpointSliceHashLength]
	maxPrefix := 63 - len(hash) - 1
	prefix := strings.TrimRight(p.serviceName[:min(len(p.serviceName), maxPrefix)], "-")
	return prefix + "-" + hash
}

func (p *kubeServicePublisher) labels() map[string]string {
	return map[string]string{
		util.KubeClusterLabel:          p.clusterName,
		"app.kubernetes.io/managed-by": "stolon",
		"app.kubernetes.io/component":  "stolon-service-publishing",
	}
}

func (p *kubeServicePublisher) endpointSliceLabels() map[string]string {
	labels := p.labels()
	labels[discoveryv1.LabelServiceName] = p.serviceName
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
