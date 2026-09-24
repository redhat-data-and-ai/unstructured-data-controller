---
globs: test/**/*.go, **/*_test.go
---

# Testing Conventions

- E2E tests use `//go:build e2e` tag and live in `test/e2e/`
- E2E framework: `sigs.k8s.io/e2e-framework` (not Ginkgo for e2e)
- Dot imports of `github.com/onsi/ginkgo/v2` and `github.com/onsi/gomega` are allowed
- Test helpers go in `test/utils/`
- Unit tests should use envtest where Kubernetes interaction is needed
- Run `make test` for unit tests, `make test-e2e` for e2e tests
- Use table-driven tests with descriptive test case names
- Tests must cover both passing (happy path) and failing (error) cases — never just the happy path
- Test names should describe the behavior being tested, not the implementation
- Assert specific error messages/types, not just `err != nil`
