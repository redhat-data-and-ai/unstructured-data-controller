# Unstructured Data Controller

Kubernetes operator (kubebuilder) for unstructured data pipelines - crawling sources, processing documents, chunking, generating embeddings, and syncing to destinations.

## Commands

Always use `make` targets instead of raw `go` commands - they include the right flags, dependencies, and setup.

- `make lint` - golangci-lint + yamllint. Run before every commit.
- `make test` - unit tests with envtest.
- `make build` - build the manager binary.
- `make fmt` - go fmt.
- `make vet` - go vet.
- `make lint-fix` - auto-fix lint issues.
- `make manifests generate` - regenerate CRDs and DeepCopy after any `api/v1alpha1/` changes.
- `make install` - install CRDs into cluster.
- `make deploy` - deploy controller to cluster.
- `go mod tidy && go mod vendor` - after dependency changes (no make target for this).

## Architecture

- **API types**: `api/v1alpha1/` - CRDs: ControllerConfig, UnstructuredDataPipeline, SourceCrawler, DocumentProcessor, ChunksGenerator, VectorEmbeddingsGenerator, DestinationSyncer
- **Controllers**: `internal/controller/` - one reconciler per CRD
- **Controller utils**: `internal/controller/controllerutils/` - shared status, predicate, and patch helpers
- **MCP server**: `internal/mcp/`, `cmd/unstructured-data-mcp-server/`
- **Packages**: `pkg/` - auth, awsclienthandler, cache, docling, embedding, filestore, gdrive, k8sclient, langchain, logger, snowflake, unstructured
- **Generated files**: `zz_generated.deepcopy.go` - never edit, run `make manifests generate`
- **Vendor**: committed. Never modify by hand.

## Principles

- Security is paramount. Always keep security in mind - validate inputs, sanitize outputs, never trust external data, never expose secrets.
- Stability is critical. This system serves critical data and processes - changes must not break existing functionality. Prefer safe, incremental changes. When in doubt, be conservative.
- Tests are critical. Every change must have tests - they catch bugs and prevent regressions. Write unit tests for new logic, update existing tests when behavior changes. If a bug is fixed, add a test that would have caught it. Tests must always cover both the passing (happy path) and failing (error) cases. No PR should reduce test coverage.
- Always run `make lint` and `make test` before pushing. Do not push code that fails linting or tests.
- Align with architecture. Changes must fit the established architecture - one controller per CRD, shared utils in controllerutils, pure library code in pkg/. If a change doesn't fit, discuss the architectural implication before proceeding.
- Best practices over existing code. If something in this codebase contradicts Go, controller-runtime, or Kubernetes conventions, challenge it - do not blindly copy a bad pattern.
- Always ask why. Poke holes in decisions - don't accept an approach just because it's what the existing code does. If you can't justify a pattern, question it.
- Code is reviewed by humans. Comment the *why* behind non-obvious decisions, and keep it short - see Go Style > Comments. Obvious code gets no comment.
- Be concise in code. Don't over-engineer - add just what's needed, nothing more. No premature abstractions, no speculative features, no unnecessary layers.
- PRs do one thing. Keep PRs tightly scoped - no scope creep, no unrelated refactors, no drive-by cleanups mixed with feature work.
- This is an open source project. Code, docs, and APIs should be usable by anyone - no assumptions about internal tooling, environments, or proprietary systems. Keep things generic and well-documented.
- Never commit secrets, API keys, tokens, passwords, or internal company information in code, commits, or PRs.

## Do Not

- Do not edit `zz_generated.deepcopy.go` - it is auto-generated. Run `make manifests generate`.
- Do not modify files in `vendor/` - run `go mod tidy && go mod vendor`.
- Do not use raw `go build`, `go test`, `go vet`, `go fmt` - use the `make` targets.
- Do not commit `.env`, `.pem`, `.key`, or any secret files.
- Do not use `git push --force`, `git reset --hard`, `git clean`, or `git branch -D`. `--force-with-lease` is still a force push: the lease stops you overwriting someone else's work, it does not make the operation allowed.
- Do not rewrite a commit that has already been pushed. No `--amend`, no squash, no rebase of a pushed branch, not even to fix a typo in a message. Add another commit instead. Rewriting published history invalidates review comments anchored to those commits and changes what a reviewer is looking at underneath them. If a branch genuinely has to be rebased onto a moved `main`, ask first, and treat the answer as covering that one push.
- Do not add dependencies without running `make lint` and `make test`.
- Do not bypass pre-commit hooks with `--no-verify`.
- Do not use `git add -A` or `git add .` - always add specific files by name.
- Do not use em dashes anywhere - use regular hyphens/dashes instead.

## Go Style

Write idiomatic Go. Follow Effective Go, Go Code Review Comments, and the Google Go Style Guide. Study the Go standard library (`net/http`, `io`, `context`) for patterns.

### Naming

- Descriptive names always. No single-letter variables except `i`/`j` in loops, method receivers, and `ctx`/`err` in tiny scopes.
- Name K8s objects by their kind: `crawlerDeployment`, `processorPod`, not `dep` or `p`.
- Acronyms are all-caps: `ID`, `HTTP`, `URL`, `API` - never `Id`, `Http`.
- No getters: `Name()` not `GetName()`. The package name provides context.
- Import aliases follow convention: `operatorv1alpha1`, `ctrl`, `appsv1`, `corev1`.

### Comments

Precise and concise. A comment earns its place by saying something the code cannot.

- Default to one line. Two sentences is the ceiling for an inline comment. A doc comment on an exported identifier may add one short second paragraph, and only when a caller cannot use it correctly without one.
- Doc comments start with the identifier name and say what it does in one sentence, plus the constraints a caller must know: nil behaviour, units, blocking, ownership, error cases.
- One why per decision, stated once. Do not repeat the same rationale in the doc comment, at the call site, and in the commit message - put it in the single place a reader will be standing when they need it.
- Leave out measured timings and benchmark numbers, narration of what was tried, arguments against alternatives that are not in the code, and anything already said in the commit message.
- No comments that restate the code (`// increment i`), no section banners, no commented-out code.
- If a comment needs several paragraphs to justify the code, either the code is wrong or the explanation belongs in a design doc - not in the file.

```go
// Good
// TryAcquire rather than Acquire so a full queue returns to the reconciler.

// Bad
// TryAcquire rather than Acquire, so a full queue returns to the reconciler
// instead of parking a worker on it. Parking a worker holds the slot for the
// length of the work, which on a real document is minutes, and a blocked
// worker is also a worker that cannot reconcile anything else, so a queue
// that fills up stalls every pipeline and not just this one.
```

### Error Handling

- Never ignore errors. Every `error` return must be checked - no `_, _ = foo()`.
- Always check `if err != nil` - never use `if err == nil` as the primary branch. The error path comes first.
- Wrap with context: `fmt.Errorf("creating deployment for crawler %s: %w", crawler.Name, err)`.
- Handle errors once: either log OR return, never both. Double-logging makes debugging harder.
- Use `errors.Is`/`errors.As` for matching, never string comparison or `==`.
- Never return both an error AND `Requeue: true` - error implies requeue with backoff.

### Logging

- Always `log.FromContext(ctx)` - never global loggers or `fmt.Println`.
- Structured key-value pairs: `log.Info("Created Deployment", "deployment", name, "namespace", ns)` - never `fmt.Sprintf` in messages.
- Log enough to debug from logs alone: reconcile entry, state transitions, decisions, handled non-errors.
- `log.Error(err, ...)` for real errors. `log.Info(...)` for expected conditions (NotFound, already exists).
- `log.V(1).Info(...)` for verbose/debug detail only.

## Controller Patterns

Best practices take precedence over existing code. If something in this codebase contradicts Go or controller-runtime best practices, challenge it - do not copy a bad pattern just because it exists.

Reference these well-written operators: kubernetes-sigs/cluster-api (condition management, patch helpers), cert-manager/cert-manager (error handling, resilience), kubernetes-sigs/kueue (modern controller-runtime usage), fluxcd/source-controller (clean reconciler structure).

### Reconciler Structure

- Level-triggered, not edge-triggered - reconcile from observed state, never branch on event type.
- Idempotent - running twice with same inputs produces same result.
- `SetupWithManager` should use `GenerationChangedPredicate{}` on the primary resource and watch dependent resources with appropriate predicates.
- Status updates via `controllerutils.StatusPatch` - always re-fetch before mutating to avoid conflicts.
- Break reconciliation into focused helper methods - do not put all logic in `Reconcile()`.

### Known deviations in existing code

The existing `handleError` pattern in this project logs the error AND returns it - this is double-handling and contradicts the Go best practice of handling errors once. New code should NOT copy this pattern. Instead: update status with the error message, then return the error without logging it separately - the caller (controller-runtime) logs returned errors.

### Anti-Patterns to Avoid

- Blindly copying existing code patterns without evaluating them against best practices
- Silently swallowing errors with `_ =`
- Using `if err == nil` as the happy path - always guard with `if err != nil` first
- Logging an error and also returning it (double-logging) - handle once
- Dumping all logic into `Reconcile()` - break into focused helper methods
- Overly broad RBAC - use minimum required verbs/resources
- Making external API calls on every reconcile without checking generation/state
- Non-descriptive variable names - `s`, `o`, `p` tell the reader nothing
- Multi-paragraph comments that re-tell the commit message or quote benchmark numbers

## Security

- Never commit secrets, API keys, tokens, passwords, or internal company information.
- Never include secrets or internal URLs in PR descriptions, comments, or commit messages.
- Use Kubernetes Secrets for sensitive configuration - never hardcode credentials in Go source.
- Review `.gitignore` and `.dockerignore` before committing to ensure no sensitive files leak.
- Pre-commit hooks include `detect-private-key` and `gitleaks` - do not bypass them.
