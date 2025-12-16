/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PipelinePlacementRequestSpec defines the desired state of PipelinePlacementRequest
type PipelinePlacementRequestSpec struct {
	// PipelineName is the name for the pipeline
	// +kubebuilder:validation:Required
	PipelineName string `json:"pipelineName"`

	// PlacementStrategy specifies which strategy to use for placement
	// +kubebuilder:validation:Enum=cloud-only-pipeline;data-locality-pipeline;simple-heuristic-pipeline;rl-based-pipeline
	// +kubebuilder:validation:Required
	PlacementStrategy string `json:"placementStrategy"`

	// DataLocation specifies where the majority of data resides
	// +kubebuilder:validation:Enum=edge_cluster_1;edge_cluster_2;edge_cluster_3;cloud_cluster;none
	// +optional
	DataLocation string `json:"dataLocation,omitempty"`

	// ExperimentId specifies the Kubeflow experiment to use for pipeline runs
	// If empty, a default experiment will be created/used
	// +optional
	ExperimentId string `json:"experimentId,omitempty"`

	// ExperimentName specifies the name for the default experiment
	// Only used if ExperimentId is empty
	// +kubebuilder:default="cloudcontinuum-default"
	// +optional
	ExperimentName string `json:"experimentName,omitempty"`

	// PipelineSource specifies where to get the pipeline YAML
	// +kubebuilder:validation:Required
	PipelineSource PipelineSource `json:"pipelineSource"`

	// Parameters for the pipeline run
	// +optional
	Parameters map[string]string `json:"parameters,omitempty"`
}

// PipelineSource defines where to fetch the pipeline YAML from
type PipelineSource struct {
	// Type of source: "url", "configmap", or "inline"
	// +kubebuilder:validation:Enum=url;configmap;inline
	// +kubebuilder:validation:Required
	Type string `json:"type"`

	// URL to fetch pipeline YAML (if type=url)
	// +optional
	URL string `json:"url,omitempty"`

	// ConfigMap reference (if type=configmap)
	// +optional
	ConfigMapRef *ConfigMapReference `json:"configMapRef,omitempty"`

	// Inline YAML content (if type=inline)
	// +optional
	Inline string `json:"inline,omitempty"`
}

// Aggiungi questa struct dopo PipelineSource
type PipelineResourcesSummary struct {
	// ExecutorCount is the total number of executors in the pipeline
	ExecutorCount int `json:"executorCount"`

	// TotalCPU is the sum of all CPU requests (in millicores)
	TotalCPU int64 `json:"totalCPU"`

	// TotalMemory is the sum of all memory requests (in bytes)
	TotalMemory int64 `json:"totalMemory"`

	// TotalGPU is the sum of all GPU requests
	TotalGPU int `json:"totalGPU,omitempty"`
}

// ConfigMapReference points to a ConfigMap containing pipeline YAML
type ConfigMapReference struct {
	// Name of the ConfigMap
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Key within the ConfigMap
	// +kubebuilder:validation:Required
	Key string `json:"key"`
}

// PipelinePlacementRequestStatus defines the observed state of PipelinePlacementRequest
type PipelinePlacementRequestStatus struct {
	// TargetCluster where the pipeline was placed
	// +optional
	TargetCluster string `json:"targetCluster,omitempty"`

	// PlacementTime when the placement decision was made
	// +optional
	PlacementTime *metav1.Time `json:"placementTime,omitempty"`

	// PlacementDecision explains why this cluster was chosen
	// +optional
	PlacementDecision string `json:"placementDecision,omitempty"`

	// PipelineRunID from Kubeflow
	// +optional
	PipelineRunID string `json:"pipelineRunID,omitempty"`

	// PipelineRunURL link to Kubeflow UI
	// +optional
	PipelineRunURL string `json:"pipelineRunURL,omitempty"`

	// TotalResources summary of pipeline resource requirements
	// +optional
	TotalResources *PipelineResourcesSummary `json:"totalResources,omitempty"`

	// ExperimentId used for the run
	// +optional
	ExperimentId string `json:"experimentId,omitempty"`

	// ExperimentName used for the run
	// +optional
	ExperimentName string `json:"experimentName,omitempty"`

	// Parameters used for the pipeline run
	// +optional
	Parameters map[string]string `json:"parameters,omitempty"`
	// Conditions represent the current state
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Strategy",type=string,JSONPath=`.spec.placementStrategy`
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.status.targetCluster`
// +kubebuilder:printcolumn:name="Pipeline",type=string,JSONPath=`.spec.pipelineName`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PipelinePlacementRequest is the Schema for the pipelineplacementrequests API
type PipelinePlacementRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PipelinePlacementRequestSpec   `json:"spec"`
	Status PipelinePlacementRequestStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PipelinePlacementRequestList contains a list of PipelinePlacementRequest
type PipelinePlacementRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PipelinePlacementRequest `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PipelinePlacementRequest{}, &PipelinePlacementRequestList{})
}
