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
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	// ForceReconcileLabel triggers a reconcile (used by SQS on new file, and by ChunksGenerator when chunks are ready).
	ForceReconcileLabel string = "operator.dataverse.redhat.com/force-reconcile"
	// SyncDestinationLabel when set together with ForceReconcileLabel means "only sync to destination" (set by ChunksGenerator when chunks are ready).
	// If only ForceReconcileLabel is set (e.g. by SQS on new file), UnstructuredDataProduct runs full pipeline instead.
	SyncDestinationLabel string = "operator.dataverse.redhat.com/sync-destination"
)

func ForceReconcilePredicate() predicate.Predicate {
	return CustomLabelKeyChangedPredicate{LabelKey: ForceReconcileLabel}
}

// Custom Predicate to filter by a specific label key
type CustomLabelKeyChangedPredicate struct {
	LabelKey string
	predicate.Funcs
}

// Custom Predicate label to force reconciliation on label addition
func (p CustomLabelKeyChangedPredicate) Update(e event.UpdateEvent) bool {
	if e.ObjectOld == nil || e.ObjectNew == nil {
		return false
	}

	oldLabels := e.ObjectOld.GetLabels()
	newLabels := e.ObjectNew.GetLabels()

	_, oldExists := oldLabels[p.LabelKey]
	_, newExists := newLabels[p.LabelKey]

	// Trigger reconciliation only if the label is added
	if !oldExists && newExists {
		return true
	}

	return false
}

func RemoveForceReconcileLabel(ctx context.Context, c client.Client, obj client.Object) error {
	labels := obj.GetLabels()
	// if there are no labels, which is weird, return nil
	if labels == nil {
		return nil
	}

	// if the force reconcile label is not present, return nil, nothing to do here
	_, ok := labels[ForceReconcileLabel]
	if !ok {
		return nil
	}

	// if the force reconcile label is present, remove it and update the object
	delete(labels, ForceReconcileLabel)
	obj.SetLabels(labels)
	return c.Update(ctx, obj)
}

func AddForceReconcileLabel(ctx context.Context, c client.Client, obj client.Object) error {
	labels := obj.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[ForceReconcileLabel] = "true"
	obj.SetLabels(labels)
	return c.Update(ctx, obj)
}

// AddForceReconcileAndSyncDestinationLabel adds both labels so UnstructuredDataProduct runs destination-only (used by ChunksGenerator when chunks are ready).
func AddForceReconcileAndSyncDestinationLabel(ctx context.Context, c client.Client, obj client.Object) error {
	labels := obj.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[ForceReconcileLabel] = "true"
	labels[SyncDestinationLabel] = "true"
	obj.SetLabels(labels)
	return c.Update(ctx, obj)
}

// RemoveSyncDestinationLabel removes the sync-destination label (call after destination-only reconcile).
func RemoveSyncDestinationLabel(ctx context.Context, c client.Client, obj client.Object) error {
	labels := obj.GetLabels()
	if labels == nil {
		return nil
	}
	if _, ok := labels[SyncDestinationLabel]; !ok {
		return nil
	}
	delete(labels, SyncDestinationLabel)
	obj.SetLabels(labels)
	return c.Update(ctx, obj)
}

// RemoveForceReconcileAndSyncDestinationLabels removes both labels (call after destination-only reconcile).
func RemoveForceReconcileAndSyncDestinationLabels(ctx context.Context, c client.Client, obj client.Object) error {
	labels := obj.GetLabels()
	if labels == nil {
		return nil
	}
	changed := false
	if _, ok := labels[ForceReconcileLabel]; ok {
		delete(labels, ForceReconcileLabel)
		changed = true
	}
	if _, ok := labels[SyncDestinationLabel]; ok {
		delete(labels, SyncDestinationLabel)
		changed = true
	}
	if !changed {
		return nil
	}
	obj.SetLabels(labels)
	return c.Update(ctx, obj)
}
