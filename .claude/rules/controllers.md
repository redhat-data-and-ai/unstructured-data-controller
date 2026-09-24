---
globs: internal/controller/**/*.go
---

# Controller Conventions

Best practices from controller-runtime, cluster-api, and cert-manager take precedence over existing code in this repo. If existing code contradicts Go or Kubernetes controller best practices, do not copy it — challenge it and write it correctly.

## Reconciler Structure
- Every reconciler struct embeds `client.Client` and has `Scheme *runtime.Scheme`
- `SetupWithManager` uses `GenerationChangedPredicate{}` on the primary resource and watches dependent resources with appropriate predicates
- RBAC markers use namespace-scoped format: `+kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=...`
- Break reconciliation into focused helper methods — never dump all logic into `Reconcile()`

## Reconcile Flow
1. `log.FromContext(ctx)` — never create standalone loggers
2. Check `IsConfigCRHealthy()` — requeue if not ready
3. `Get` the CR — return nil on NotFound (resource deleted, not an error)
4. Reconcile logic in focused helper methods
5. On error: update CR status with the error, return the error. Do NOT log the error AND return it — that double-logs.

## Error Handling
- Always check `if err != nil` — never use `if err == nil` as the happy-path guard
- Handle errors once: update status OR log, then return. Never log AND return the same error.
- Status updates: always use `controllerutils.StatusPatch` — re-fetches object before mutating
- Never return both error AND `Requeue: true` — error already implies requeue with backoff
- Wrap errors with context: `fmt.Errorf("creating deployment for %s: %w", name, err)`
- The existing `handleError` pattern double-logs (logs + returns). New code should not copy this — update status and return, let the caller log.

## Logging
- Use structured key-value pairs: `logger.Info("Reconciling", "controller", ControllerName)`
- Log at reconcile entry and meaningful state transitions
- `log.Error(err, ...)` for real errors. `log.Info(...)` for expected conditions like NotFound
- Import alias `operatorv1alpha1` for API types package
