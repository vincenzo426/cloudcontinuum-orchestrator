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

// PlacementRequestSpec defines the desired state of PlacementRequest
type PlacementRequestSpec struct {
	// WorkloadType specifies the type of ML workload
	// +kubebuilder:validation:Enum=training;inference;preprocessing
	// +kubebuilder:validation:Required
	WorkloadType string `json:"workloadType"`

	// ResourceRequirements specifies the compute resources needed
	// +kubebuilder:validation:Required
	ResourceRequirements ResourceRequirements `json:"resourceRequirements"`

	// DataLocation specifies where the data resides
	// +kubebuilder:validation:Enum=edge_cluster_1;edge_cluster_2;cloud_cluster;none
	// +optional
	DataLocation string `json:"dataLocation,omitempty"`

	// PlacementStrategy specifies which strategy to use for placement
	// +kubebuilder:validation:Enum=cloud-only;data-locality;simple-heuristic;rl-based
	// +kubebuilder:validation:Required
	PlacementStrategy string `json:"placementStrategy"`

	// PodSpec contains the actual pod specification to deploy
	// +optional
	PodSpec *PodTemplateSpec `json:"podSpec,omitempty"`
}

// ResourceRequirements defines compute resource requirements
type ResourceRequirements struct {
	// CPU in cores (e.g., "2" or "500m")
	// +kubebuilder:validation:Required
	CPU string `json:"cpu"`

	// Memory in bytes (e.g., "4Gi", "2048Mi")
	// +kubebuilder:validation:Required
	Memory string `json:"memory"`

	// GPU count (0 if not needed)
	// +kubebuilder:validation:Minimum=0
	// +optional
	GPU int `json:"gpu,omitempty"`
}

// PodTemplateSpec is a simplified pod template
type PodTemplateSpec struct {
	// Image is the container image to run
	// +kubebuilder:validation:Required
	Image string `json:"image"`

	// Command to run in the container
	// +optional
	Command []string `json:"command,omitempty"`

	// Args for the command
	// +optional
	Args []string `json:"args,omitempty"`
}

// PlacementRequestStatus defines the observed state of PlacementRequest
type PlacementRequestStatus struct {
	// TargetCluster where the workload was placed
	// +optional
	TargetCluster string `json:"targetCluster,omitempty"`

	// PlacementTime when the placement decision was made
	// +optional
	PlacementTime *metav1.Time `json:"placementTime,omitempty"`

	// PlacementDecision explains why this cluster was chosen
	// +optional
	PlacementDecision string `json:"placementDecision,omitempty"`

	// Conditions represent the current state of the PlacementRequest
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Strategy",type=string,JSONPath=`.spec.placementStrategy`
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.status.targetCluster`
// +kubebuilder:printcolumn:name="Workload",type=string,JSONPath=`.spec.workloadType`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PlacementRequest is the Schema for the placementrequests API
type PlacementRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitzero"`

	Spec   PlacementRequestSpec   `json:"spec"`
	Status PlacementRequestStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PlacementRequestList contains a list of PlacementRequest
type PlacementRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PlacementRequest `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PlacementRequest{}, &PlacementRequestList{})
}
