// Copyright 2018 Sorint.lab
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
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/sorintlab/stolon/internal/cluster"
	mock_store "github.com/sorintlab/stolon/internal/mock/store"
)

func TestWriteClusterdata(t *testing.T) {
	t.Run("should handle error returned by stdin", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		c := &ClusterDataWriteCommand{}
		store := mock_store.NewMockStore(ctrl)
		reader := strings.Reader{}
		err := c.writeFrom(&reader, store)

		if err == nil {
			t.Error("expected to have an error")
		}

		expected := "invalid cluster data: unexpected end of JSON input"
		if err.Error() != expected {
			t.Errorf("expected %s, got %s", expected, err.Error())
		}
	})

	t.Run("should handle parse error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		c := &ClusterDataWriteCommand{File: "cluster_data.json"}
		store := mock_store.NewMockStore(ctrl)
		reader := strings.NewReader("[")
		err := c.writeFrom(reader, store)

		if err == nil {
			t.Error("expected to have an error")
		}

		expected := "invalid cluster data: yaml: line 1: did not find expected node content"
		if err.Error() != expected {
			t.Errorf("expected %s, got %s", expected, err.Error())
		}
	})

	t.Run("should fail when GetClusterData errors", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		c := &ClusterDataWriteCommand{File: "-"}
		reader := strings.NewReader("{}")
		store := mock_store.NewMockStore(ctrl)
		store.EXPECT().GetClusterData(gomock.Any()).Return(nil, nil, fmt.Errorf("Error in getting cluster data"))

		err := c.writeFrom(reader, store)
		if err == nil {
			t.Error("expected to have an error")
		}
		if err.Error() != "Error in getting cluster data" {
			t.Errorf("got %s", err.Error())
		}
	})

	t.Run("should require --yes when cluster data already available", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		c := &ClusterDataWriteCommand{File: "-"}
		reader := strings.NewReader("{}")
		store := mock_store.NewMockStore(ctrl)
		store.EXPECT().GetClusterData(gomock.Any()).Return(&cluster.ClusterData{}, nil, nil)

		err := c.writeFrom(reader, store)
		if err == nil {
			t.Error("expected to have an error")
		}
		if err.Error() != "WARNING: cluster data already available use --yes to override" {
			t.Errorf("got %s", err.Error())
		}
	})

	t.Run("should propagate Put errors", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		c := &ClusterDataWriteCommand{File: "-", ForceYes: true}
		reader := strings.NewReader("{}")
		store := mock_store.NewMockStore(ctrl)
		cd := &cluster.ClusterData{}
		store.EXPECT().GetClusterData(gomock.Any()).Return(cd, nil, nil)
		store.EXPECT().PutClusterData(gomock.Any(), cd).Return(fmt.Errorf("error while uploading the cluster data"))

		err := c.writeFrom(reader, store)
		if err == nil {
			t.Error("expected to have an error")
		}
		if err.Error() != "failed to write cluster data into new store error while uploading the cluster data" {
			t.Errorf("got %s", err.Error())
		}
	})

	t.Run("should successfully upload the cluster data", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		c := &ClusterDataWriteCommand{File: "-", ForceYes: true}
		reader := strings.NewReader("{}")
		store := mock_store.NewMockStore(ctrl)
		cd := &cluster.ClusterData{}
		store.EXPECT().GetClusterData(gomock.Any()).Return(cd, nil, nil)
		store.EXPECT().PutClusterData(gomock.Any(), cd).Return(nil)

		err := c.writeFrom(reader, store)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})
}
