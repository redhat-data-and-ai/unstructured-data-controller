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

	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// StatusUpdateWithRetry runs Get -> mutate -> Status().Update with RetryOnConflict.
// newEmpty must return a new zero value of the object type (e.g. func() client.Object { return &ChunksGenerator{} }).
// mutate is called with the fetched object so the caller can set status (e.g. SetWaiting(), UpdateStatus(msg, err)).
func StatusUpdateWithRetry(ctx context.Context, c client.Client, key client.ObjectKey, newEmpty func() client.Object, mutate func(client.Object)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		obj := newEmpty()
		if err := c.Get(ctx, key, obj); err != nil {
			return err
		}
		mutate(obj)
		return c.Status().Update(ctx, obj)
	})
}

// RemoveForceReconcileLabelWithRetry gets the latest object, removes the force-reconcile label, and updates with retry.
func RemoveForceReconcileLabelWithRetry(ctx context.Context, c client.Client, key client.ObjectKey, newEmpty func() client.Object) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		obj := newEmpty()
		if err := c.Get(ctx, key, obj); err != nil {
			return err
		}
		return RemoveForceReconcileLabel(ctx, c, obj)
	})
}

// AddForceReconcileLabelWithRetry gets the latest object, adds the force-reconcile label, and updates with retry.
func AddForceReconcileLabelWithRetry(ctx context.Context, c client.Client, key client.ObjectKey, newEmpty func() client.Object) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		obj := newEmpty()
		if err := c.Get(ctx, key, obj); err != nil {
			return err
		}
		return AddForceReconcileLabel(ctx, c, obj)
	})
}
