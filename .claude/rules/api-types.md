---
globs: api/v1alpha1/**/*.go
---

# API Type Conventions

- Follow the established type structure: Spec struct, Status struct with `Conditions []metav1.Condition` and `LastAppliedGeneration int64`, CR struct with kubebuilder markers, List struct, `init()` with `SchemeBuilder.Register`
- Add `UpdateStatus()` and `SetWaiting()` methods on the CR struct
- Define a condition type constant (e.g., `MyResourceCondition = "MyResourceReady"`)
- Use kubebuilder markers: `+kubebuilder:object:root=true`, `+kubebuilder:subresource:status`
- Add `+kubebuilder:printcolumn` for Status and Message using the condition type
- JSON tags must use camelCase
- Optional fields must be pointer types with `+optional` marker
- Do not edit `zz_generated.deepcopy.go` — run `make manifests generate`
- After any changes: run `make manifests generate`
