---
globs: internal/controller/**/*.go
---

# Controller Conventions

Best practices from controller-runtime, cluster-api, and cert-manager take precedence over existing code in this repo. If existing code contradicts Go or Kubernetes controller best practices, do not copy it - challenge it and write it correctly.

## Reconciler Structure
- Every reconciler struct embeds `client.Client` and has `Scheme *runtime.Scheme`
- `SetupWithManager` uses `GenerationChangedPredicate{}` on the primary resource and watches dependent resources with appropriate predicates
- RBAC markers use namespace-scoped format: `+kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=...`
- Break reconciliation into focused helper methods - never dump all logic into `Reconcile()`

## Reconcile Flow
1. `log.FromContext(ctx)` - never create standalone loggers
2. Check `IsConfigCRHealthy()` - requeue if not ready
3. `Get` the CR - return nil on NotFound (resource deleted, not an error)
4. Reconcile logic in focused helper methods
5. On error: update CR status with the error, return the error. Do NOT log the error AND return it - that double-logs.

## Do NOT skip reconciliation based on generation or status checks

Never add logic inside `Reconcile()` to skip work based on `observedGeneration`, `LastAppliedGeneration`, or condition status. This is an anti-pattern for this project because:

1. **It defeats predicates.** Controllers use watches and predicates (e.g., `FilesProcessedChangedPredicate`, Secret watches) to trigger reconciliation for specific events. A generation check inside `Reconcile()` can't distinguish why the reconcile was triggered - it blindly skips based on the CR's own state, silently dropping legitimate events from upstream stages, Secrets, or other watches.

2. **It breaks polling loops.** Controllers that use `RequeueAfter` for periodic polling (e.g., SourceCrawler polling S3 for new files) get stuck because the generation doesn't change between polls. After the first successful reconcile, every subsequent timer-fired reconcile matches the check and skips.

3. **It's fragile.** Every new watch or trigger added in the future silently breaks unless someone remembers to also update the skip check. We hit this repeatedly: it blocked RequeueAfter, then upstream events, then Secret changes.

The idiomatic controller-runtime approach is:
- **Make Reconcile idempotent** - always read current state, always converge to desired state
- **Use predicates in SetupWithManager** to control which events trigger reconciliation (this is the right layer for filtering)
- **Use concurrency** (5 workers per controller) to handle restart backlog quickly
- **Rely on the priority queue** to prioritize new CRs (priority 0) over restart re-list items (priority -100)

This is what the Kubebuilder documentation recommends, and what controller-runtime is designed for. `GenerationChangedPredicate` on the `For()` resource is the correct and sufficient restart optimization - it filters status-only Update events without blocking Create events or RequeueAfter items.

## Restart Behavior

When a pod restarts or a new version is deployed:
1. The work queue is empty (all RequeueAfter timers are lost)
2. The informer re-lists every existing CR as a Create event
3. controller-runtime's priority queue assigns priority -100 to these initial-list items
4. Any new CR created by a user after startup gets priority 0 (processed first)
5. With concurrency=5, multiple workers process the backlog in parallel
6. `GenerationChangedPredicate` on `For()` only filters Update events - it lets all Create events through, which is correct

## Priority Queue

controller-runtime v0.23.3 uses a priority queue by default. Understanding how priorities work prevents unnecessary workarounds:

**How priorities are assigned:**
- Initial list items (pod restart re-list): priority -100 (set by the framework via `handler.LowPriority`)
- Resync events (same ResourceVersion): priority -100
- Real Create events (new CR from user): priority 0 (default, nil priority)
- Real Update events (spec changed): priority 0 (default)

**How priorities are preserved:**
- When `Reconcile()` returns `RequeueAfter` or an error WITHOUT setting `result.Priority`, controller-runtime preserves the original dequeue priority. A restart item dequeued at -100 stays at -100 on requeue.
- When `Reconcile()` sets `result.Priority = &value`, that overrides the priority for the requeue.
- This means new CRs (priority 0) always jump ahead of restart backlog (priority -100) without any manual priority setting.

**When to set `result.Priority`:**
- For top-level orchestrating controllers that don't watch other stages or Secrets, setting a low priority on skip requeues can be useful (e.g., data-platform-operator's `ShouldSkipReconciliation` uses -200). This works because those controllers are only triggered by their own spec changes, so the skip check doesn't defeat any predicates.
- For stage controllers in this project, do NOT set priority manually. The skip logic itself is wrong for our use case (see "Do NOT skip reconciliation" above), so the priority is irrelevant.

## Concurrency
Per-controller concurrency is configured via environment variables (e.g., `CONCURRENCY_DOCUMENT_PROCESSOR=10`) with a default of 5 workers. This is set at startup via `config.Controller.GroupKindConcurrency` in the manager options. The work queue guarantees the same CR is never reconciled by two workers simultaneously, so increasing concurrency is safe for idempotent reconcilers.

## Error Handling
- Always check `if err != nil` - never use `if err == nil` as the happy-path guard
- Handle errors once: update status OR log, then return. Never log AND return the same error.
- Status updates: always use `controllerutils.StatusPatch` - re-fetches object before mutating
- Never return both error AND `Requeue: true` - error already implies requeue with backoff
- Wrap errors with context: `fmt.Errorf("creating deployment for %s: %w", name, err)`
- The existing `handleError` pattern double-logs (logs + returns). New code should not copy this - update status and return, let the caller log.

## Logging
- Use structured key-value pairs: `logger.Info("Reconciling", "controller", ControllerName)`
- Log at reconcile entry and meaningful state transitions
- `log.Error(err, ...)` for real errors. `log.Info(...)` for expected conditions like NotFound
- Import alias `operatorv1alpha1` for API types package
