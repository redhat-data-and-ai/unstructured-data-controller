/*
Copyright 2026.

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

package controllerutils

import (
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
)

func NewFilesDetectedPredicate() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return false
			}
			if e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() {
				return true
			}

			oldUDP, oldOk := e.ObjectOld.(*operatorv1alpha1.UnstructuredDataProduct)
			newUDP, newOk := e.ObjectNew.(*operatorv1alpha1.UnstructuredDataProduct)

			if oldOk && newOk {
				if oldUDP.Status.IsNewFilesDetected != newUDP.Status.IsNewFilesDetected {
					return true
				}
			}
			return false
		},
		CreateFunc: func(_ event.CreateEvent) bool {
			return true
		},
		DeleteFunc: func(_ event.DeleteEvent) bool {
			return true
		},
	}
}

func NewFilesProcessedPredicate() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return false
			}

			if e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() {
				return true
			}

			oldDP, oldOk := e.ObjectOld.(*operatorv1alpha1.DocumentProcessor)
			newDP, newOk := e.ObjectNew.(*operatorv1alpha1.DocumentProcessor)

			if oldOk && newOk {
				if oldDP.Status.IsNewFilesProcessed != newDP.Status.IsNewFilesProcessed {
					return true
				}
			}

			return false
		},
		CreateFunc: func(_ event.CreateEvent) bool {
			return true
		},
		DeleteFunc: func(_ event.DeleteEvent) bool {
			return true
		},
	}
}

func NewFilesChunkedPredicate() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return false
			}

			if e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() {
				return true
			}

			oldCG, oldOk := e.ObjectOld.(*operatorv1alpha1.ChunksGenerator)
			newCG, newOk := e.ObjectNew.(*operatorv1alpha1.ChunksGenerator)

			if oldOk && newOk {
				if oldCG.Status.IsNewFilesChunked != newCG.Status.IsNewFilesChunked {
					return true
				}
			}

			return false
		},
		CreateFunc: func(_ event.CreateEvent) bool {
			return true
		},
		DeleteFunc: func(_ event.DeleteEvent) bool {
			return true
		},
	}
}

func NewFilesEmbeddedPredicate() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return false
			}

			if e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() {
				return true
			}

			oldVEG, oldOk := e.ObjectOld.(*operatorv1alpha1.VectorEmbeddingsGenerator)
			newVEG, newOk := e.ObjectNew.(*operatorv1alpha1.VectorEmbeddingsGenerator)

			if oldOk && newOk {
				if oldVEG.Status.IsNewFilesEmbedded != newVEG.Status.IsNewFilesEmbedded {
					return true
				}
			}

			return false
		},
		CreateFunc: func(_ event.CreateEvent) bool {
			return true
		},
		DeleteFunc: func(_ event.DeleteEvent) bool {
			return true
		},
	}
}
