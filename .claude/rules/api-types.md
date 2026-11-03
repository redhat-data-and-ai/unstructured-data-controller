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
- Do not edit `zz_generated.deepcopy.go` - run `make manifests generate`
- After any changes: run `make manifests generate`

## K8s API Conventions

Follow the [Kubernetes API conventions](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md). Key rules for this project:

### Status fields
- Status fields must be observations of actual state, not desired state
- Use `observed` prefix for fields that record what the controller last saw (matches `observedGeneration` convention)
- Status information that may be large should go in separate objects
- Historical information (last transition time, failure counts) is best-effort

### Lists of named subobjects preferred over maps
There are no maps of subobjects in any API objects. Use a list of subobjects containing name fields instead:

```yaml
# Correct: list of typed structs
observedUpstreamState:
  - name: crawl
    filesProcessed: 15

# Wrong: map
observedUpstreamState:
  crawl: 15
```

The only exceptions are pure maps (labels, selectors, annotations, data), not sets of subobjects. This maintains the invariant that all JSON/YAML keys are fields in API objects, and is required for proper OpenAPI validation.

Naming the elements is not enough on its own - a named-subobject list must also declare its merge key, or server-side apply treats the whole list as atomic and one applier replaces every entry instead of merging its own:

```go
// +listType=map
// +listMapKey=name
// +optional
ObservedUpstreamState []ObservedUpstreamState `json:"observedUpstreamState,omitempty"`
```

Use `+listType=atomic` instead only when the list really is a single indivisible value, and say why in a comment. Conditions use the same markers keyed on `type` (see below).

### Conditions
- Use `[]metav1.Condition` with `+listType=map` and `+listMapKey=type`
- Conditions complement detailed status, they don't replace it
- Controllers should set conditions on first visit, even if Unknown
- Names should describe current observed state (adjective or past-tense verb): `Ready`, `Succeeded`, `Failed`
- The pattern of using `phase` is deprecated - use conditions instead
- Conditions are observations, not state machines

### Recording the observed generation
Upstream calls this field `observedGeneration`. This project spells it
`lastAppliedGeneration` (`json:"lastAppliedGeneration,omitempty"`) and every
existing CRD already uses that name, so new CRDs must use it too - a second
spelling would mean clients had to check both.

- Record the generation most recently acted upon, so clients can tell whether the controller has seen the latest spec
- Do not rename the existing field: it is part of the published CRD schema and renaming it is a breaking API change for anything reading status
- Do not add an `observedGeneration` alongside it
