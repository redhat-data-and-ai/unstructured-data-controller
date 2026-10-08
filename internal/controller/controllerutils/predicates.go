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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

// ReconcilableObject is implemented by CRs that track their reconciliation
// state via LastAppliedGeneration and Conditions.
type ReconcilableObject interface {
	client.Object
	GetLastAppliedGeneration() int64
	GetStatusConditions() []metav1.Condition
}

// IsAlreadyReconciled returns true when the CR's spec has already been
// successfully reconciled. This allows controllers to skip redundant work
// during pod restarts when the informer re-lists all existing objects.
func IsAlreadyReconciled(generation, lastAppliedGeneration int64, conditions []metav1.Condition, conditionType string) bool {
	if lastAppliedGeneration != generation {
		return false
	}
	condition := meta.FindStatusCondition(conditions, conditionType)
	return condition != nil && condition.Status == metav1.ConditionTrue
}

// ReconcileNeededPredicate filters out Create events for objects that have
// already been successfully reconciled. This prevents redundant reconciliation
// during pod restarts when the informer re-lists all existing objects as Create
// events. RequeueAfter items bypass predicates entirely, so controllers that
// rely on periodic re-reconciliation (polling S3, checking task status) are
// unaffected.
type ReconcileNeededPredicate struct {
	ConditionType string
}

func (p ReconcileNeededPredicate) Create(e event.CreateEvent) bool {
	obj, ok := e.Object.(ReconcilableObject)
	if !ok {
		return true
	}
	return !IsAlreadyReconciled(
		obj.GetGeneration(),
		obj.GetLastAppliedGeneration(),
		obj.GetStatusConditions(),
		p.ConditionType,
	)
}

//nolint:revive // receiver unused but required by the predicate.Predicate interface
func (p ReconcileNeededPredicate) Update(_ event.UpdateEvent) bool { return true }

//nolint:revive // receiver unused but required by the predicate.Predicate interface
func (p ReconcileNeededPredicate) Delete(_ event.DeleteEvent) bool { return true }

//nolint:revive // receiver unused but required by the predicate.Predicate interface
func (p ReconcileNeededPredicate) Generic(_ event.GenericEvent) bool { return true }
