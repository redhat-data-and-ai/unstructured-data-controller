---
name: verify
description: Run linting and unit tests to verify changes are correct before committing
---

Run the following checks in sequence, stopping on first failure:

1. `make lint` - Run golangci-lint
2. `make test` - Run unit tests with envtest

If any API types in `api/v1alpha1/` were modified, first run `make manifests generate` before the checks above.

If any dependencies changed, first run `go mod tidy && go mod vendor`.

Report a summary of results: what passed, what failed, and any errors to fix.
