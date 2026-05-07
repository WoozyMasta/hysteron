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
	"testing"

	"github.com/rs/zerolog"
	"github.com/sorintlab/stolon/internal/cluster"
	"github.com/sorintlab/stolon/internal/common"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCheckSentinelConfigRequiresKubernetesBackendForServicePublishing(t *testing.T) {
	cfg := &config{}
	cfg.KubeService.Enabled = true
	cfg.Store.Backend = "etcdv3"

	if err := checkSentinelConfig(cfg); err == nil {
		t.Fatal("expected backend validation error")
	}
}

func TestCheckSentinelConfigAllowsKubernetesBackendForServicePublishing(t *testing.T) {
	cfg := &config{}
	cfg.KubeService.Enabled = true
	cfg.Store.Backend = "kubernetes"

	if err := checkSentinelConfig(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCheckSentinelConfigAllowsK8sBackendAliasForServicePublishing(t *testing.T) {
	cfg := &config{}
	cfg.KubeService.Enabled = true
	cfg.Store.Backend = "k8s"

	if err := checkSentinelConfig(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestKubeServicePublisherPublishesWritableEndpointSlice(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	publisher := testKubeServicePublisher(client, "stolon-cluster-test-rw")
	cd := testKubeServiceClusterData("10.1.2.3", "5433")

	if err := publisher.Publish(ctx, cd); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	service, err := client.CoreV1().Services("default").Get(ctx, "stolon-cluster-test-rw", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Services.Get() error = %v", err)
	}
	if len(service.Spec.Selector) != 0 {
		t.Fatalf("Service selector = %v, want empty", service.Spec.Selector)
	}
	if len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Port != 5432 {
		t.Fatalf("Service ports = %#v, want one port 5432", service.Spec.Ports)
	}

	slice, err := client.DiscoveryV1().EndpointSlices("default").Get(ctx, "stolon-cluster-test-rw-stolon", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("EndpointSlices.Get() error = %v", err)
	}
	if slice.Labels[discoveryv1.LabelServiceName] != service.Name {
		t.Fatalf("EndpointSlice service label = %q, want %q", slice.Labels[discoveryv1.LabelServiceName], service.Name)
	}
	if slice.Labels[discoveryv1.LabelManagedBy] != kubeEndpointSliceManagedBy {
		t.Fatalf("EndpointSlice managed-by label = %q", slice.Labels[discoveryv1.LabelManagedBy])
	}
	if len(slice.Endpoints) != 1 || slice.Endpoints[0].Addresses[0] != "10.1.2.3" {
		t.Fatalf("EndpointSlice endpoints = %#v", slice.Endpoints)
	}
	if len(slice.Ports) != 1 || slice.Ports[0].Port == nil || *slice.Ports[0].Port != 5433 {
		t.Fatalf("EndpointSlice ports = %#v, want target port 5433", slice.Ports)
	}
}

func TestKubeServicePublisherClearsWritableEndpoints(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	publisher := testKubeServicePublisher(client, "stolon-cluster-test-rw")
	cd := testKubeServiceClusterData("10.1.2.3", "5432")

	if err := publisher.Publish(ctx, cd); err != nil {
		t.Fatalf("Publish() create error = %v", err)
	}
	cd.Proxy.Spec.MasterDBUID = ""
	if err := publisher.Publish(ctx, cd); err != nil {
		t.Fatalf("Publish() clear error = %v", err)
	}

	slice, err := client.DiscoveryV1().EndpointSlices("default").Get(ctx, "stolon-cluster-test-rw-stolon", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("EndpointSlices.Get() error = %v", err)
	}
	if len(slice.Endpoints) != 0 {
		t.Fatalf("EndpointSlice endpoints = %#v, want empty", slice.Endpoints)
	}
}

func TestKubeServicePublisherRejectsSelectorService(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "stolon-cluster-test-rw", Namespace: "default"},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "postgres"},
			Ports:    []corev1.ServicePort{{Name: "postgres", Port: 5432}},
		},
	})
	publisher := testKubeServicePublisher(client, "stolon-cluster-test-rw")

	if err := publisher.Publish(ctx, testKubeServiceClusterData("10.1.2.3", "5432")); err == nil {
		t.Fatal("expected selector service error")
	}
}

func TestKubeServicePublisherPublishesReadOnlyEndpoints(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	publisher := testKubeServicePublisher(client, "stolon-cluster-test-rw")
	publisher.readOnlyEnabled = true
	publisher.readOnlyServiceName = "stolon-cluster-test-ro"
	publisher.readOnlyServicePort = 5432
	publisher.readOnlyMaxLag = 10
	cd := testKubeServiceClusterData("10.1.2.3", "5432")
	cd.DBs["db-02"] = &cluster.DB{
		UID:        "db-02",
		Generation: 1,
		Spec: &cluster.DBSpec{
			KeeperUID: "keeper-02",
			Role:      common.RoleStandby,
		},
		Status: cluster.DBStatus{
			Healthy:           true,
			CurrentGeneration: 1,
			ListenAddress:     "10.1.2.4",
			Port:              "5432",
			XLogPos:           95,
		},
	}
	cd.DBs["db-01"].Generation = 1
	cd.DBs["db-01"].Status.CurrentGeneration = 1
	cd.DBs["db-01"].Status.XLogPos = 100
	cd.DBs["db-01"].Status.SynchronousStandbys = []string{"db-02"}

	if err := publisher.Publish(ctx, cd); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	slice, err := client.DiscoveryV1().EndpointSlices("default").Get(ctx, "stolon-cluster-test-ro-stolon", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("EndpointSlices.Get() error = %v", err)
	}
	if len(slice.Endpoints) != 1 || slice.Endpoints[0].Addresses[0] != "10.1.2.4" {
		t.Fatalf("read-only EndpointSlice endpoints = %#v", slice.Endpoints)
	}
}

func TestKubeServicePublisherReadOnlyFallbackToPrimary(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	publisher := testKubeServicePublisher(client, "stolon-cluster-test-rw")
	publisher.readOnlyEnabled = true
	publisher.readOnlyServiceName = "stolon-cluster-test-ro"
	publisher.readOnlyServicePort = 5432
	publisher.readOnlyNoFallback = false
	publisher.readOnlyMaxLag = 0
	cd := testKubeServiceClusterData("10.1.2.3", "5432")
	cd.DBs["db-01"].Generation = 1
	cd.DBs["db-01"].Status.CurrentGeneration = 1
	cd.DBs["db-01"].Status.XLogPos = 100
	cd.DBs["db-02"] = &cluster.DB{
		UID:        "db-02",
		Generation: 1,
		Spec: &cluster.DBSpec{
			KeeperUID: "keeper-02",
			Role:      common.RoleStandby,
		},
		Status: cluster.DBStatus{
			Healthy:           true,
			CurrentGeneration: 1,
			ListenAddress:     "10.1.2.4",
			Port:              "5432",
			XLogPos:           50,
		},
	}

	if err := publisher.Publish(ctx, cd); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	slice, err := client.DiscoveryV1().EndpointSlices("default").Get(ctx, "stolon-cluster-test-ro-stolon", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("EndpointSlices.Get() error = %v", err)
	}
	if len(slice.Endpoints) != 1 || slice.Endpoints[0].Addresses[0] != "10.1.2.3" {
		t.Fatalf("read-only fallback EndpointSlice endpoints = %#v", slice.Endpoints)
	}
}

func testKubeServicePublisher(client *fake.Clientset, serviceName string) *kubeServicePublisher {
	return &kubeServicePublisher{
		client:              client,
		log:                 zerolog.Nop(),
		namespace:           "default",
		clusterName:         "test",
		writableServiceName: serviceName,
		writableServicePort: 5432,
	}
}

func testKubeServiceClusterData(address, port string) *cluster.ClusterData {
	cd := cluster.NewClusterData(&cluster.Cluster{UID: "cluster-01"})
	cd.Proxy = &cluster.Proxy{
		Spec: cluster.ProxySpec{MasterDBUID: "db-01"},
	}
	cd.DBs["db-01"] = &cluster.DB{
		UID: "db-01",
		Spec: &cluster.DBSpec{
			KeeperUID: "keeper-01",
			Role:      common.RoleMaster,
		},
		Status: cluster.DBStatus{
			ListenAddress: address,
			Port:          port,
			Healthy:       true,
		},
	}
	return cd
}
