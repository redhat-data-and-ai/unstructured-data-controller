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
)

// FilesProcessedGetter is implemented by all stage CRs to expose the filesProcessed counter.
type FilesProcessedGetter interface {
	GetFilesProcessed() int64
}

// FilesProcessedChangedPredicate filters Update events so that only changes
// to the filesProcessed status field trigger a reconcile.
type FilesProcessedChangedPredicate struct{}

func (FilesProcessedChangedPredicate) Create(_ event.CreateEvent) bool   { return true }
func (FilesProcessedChangedPredicate) Delete(_ event.DeleteEvent) bool   { return false }
func (FilesProcessedChangedPredicate) Generic(_ event.GenericEvent) bool { return false }
func (FilesProcessedChangedPredicate) Update(e event.UpdateEvent) bool {
	oldObj, ok1 := e.ObjectOld.(FilesProcessedGetter)
	newObj, ok2 := e.ObjectNew.(FilesProcessedGetter)
	if !ok1 || !ok2 {
		return true // can't compare, allow through
	}
	return oldObj.GetFilesProcessed() != newObj.GetFilesProcessed()
}
