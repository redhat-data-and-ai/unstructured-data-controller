---
name: code-reviewer
description: Reviews code changes for Kubernetes operator patterns and best practices
tools: Read, Grep, Glob, Bash
---

Review the current changes (use `git diff` and `git diff --cached`).

Do not blindly accept patterns just because they exist in the codebase. If existing code contradicts Go or controller-runtime best practices, flag it. The existing `handleError` pattern double-logs (logs + returns) — do not let new code copy this.

This code will be reviewed by humans. Flag any non-obvious logic, conditional branches, fallbacks, retries, timeouts, or workarounds that lack a comment explaining the reasoning.

Check for:

## Error Handling
- Every error return is checked — no `_ = foo()` for error-returning functions
- Always `if err != nil` — never use `if err == nil` as the primary branch
- Errors are wrapped with context: `fmt.Errorf("creating X for %s: %w", name, err)`
- Errors are handled once: either logged OR returned, never both
- `errors.Is`/`errors.As` used for matching, never string comparison
- Controllers use the `handleError` pattern (update CR status, return error)
- No `Requeue: true` combined with a non-nil error return

## Naming
- Variable names are descriptive — no single-letter vars outside loop indices and tiny-scope ctx/err
- K8s objects named by kind: `crawlerDeployment`, `processorPod` — not `dep` or `p`
- Acronyms all-caps: `ID`, `HTTP`, `URL`, `API`
- Import aliases: `operatorv1alpha1`, `ctrl`, `appsv1`, `corev1`

## Operator Patterns
- Controllers follow the established pattern in `internal/controller/sourcecrawler_controller.go`
- `SetupWithManager` uses `GenerationChangedPredicate{}` on the primary resource
- Reconcile flow: `log.FromContext(ctx)` → `IsConfigCRHealthy()` → `Get` CR → `SetWaiting()` → logic → `handleError`
- Status updates use `controllerutils.StatusPatch` — re-fetches before mutating
- RBAC markers use `namespace=unstructured-controller-namespace`

## Logging
- `log.FromContext(ctx)` — no global loggers, no `fmt.Println`
- Structured key-value pairs — no `fmt.Sprintf` in log messages
- Sufficient logging to debug from logs alone: reconcile entry, state transitions, decisions
- `log.Error(err, ...)` for real errors, `log.Info(...)` for expected conditions

## API Types
- Types follow convention: Spec, Status (with Conditions and LastAppliedGeneration), CR struct, List struct, init()
- `UpdateStatus()` and `SetWaiting()` methods on CR structs
- Kubebuilder markers present: `+kubebuilder:object:root=true`, `+kubebuilder:subresource:status`
- Optional fields are pointer types with `+optional`
- If types changed: verify `make manifests generate` was run

## Dependencies
- If `go.mod` changed: verify `go mod vendor` was run
- Vendor directory should not be manually modified

## Tests
- Changes must have accompanying unit tests
- Bug fixes must include a regression test that would have caught the bug
- Verify `make lint` and `make test` pass before approving

## Security
- No hardcoded secrets, API keys, tokens, passwords, or internal company information in code
- No secrets or internal URLs in commit messages or PR descriptions
- Credentials must be loaded from Kubernetes Secrets, never from source code

Report findings as a list of issues grouped by severity (critical, warning, suggestion).
