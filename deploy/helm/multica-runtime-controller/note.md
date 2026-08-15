# notes for `deploy/helm/multica-runtime-controller`
> Last updated: 2026-08-13 - made Pi configuration paths relative to the native Pi home.

## File Index
### `Chart.yaml`
- **Purpose**: Declares the Helm chart identity and release version used to package the runtime controller.
- **Consumers**: Helm packaging, linting, and installation commands

### `values.yaml`
- **Purpose**: Provides supported defaults for the controller, task runtime, and shared workspace claim.
- **Consumers**: Templates in `templates/` and Helm operators overriding deployment values
- **Notes**: `workspace.claimName` identifies the single Helm-owned claim shared by controller and task Pods.

### `values.schema.json`
- **Purpose**: Rejects unsupported Helm values before rendering or installation.
- **Dependencies**: The public values contract in `values.yaml`
- **Consumers**: Helm linting, rendering, and installation commands

## Subdirectory `templates/`
### `_helpers.tpl`
- **Purpose**: Centralizes stable Kubernetes resource names and common labels for chart templates.
- **Key exports**: `multica-runtime-controller.name`, `multica-runtime-controller.fullname`, `multica-runtime-controller.labels`, `multica-runtime-controller.workspaceClaimName`, `multica-runtime-controller.identitySecretName`, `multica-runtime-controller.daemonProxyServiceName`
- **Consumers**: Kubernetes manifests in `templates/`
- **Notes**: `workspaceClaimName` maps `workspace.claimName` to the single shared workspace claim.

### `deployment.yaml`
- **Purpose**: Runs the controller and prepares Pi's standard home directories and declared packages on the shared workspace claim.
- **Dependencies**: Controller and runtime values, chart naming helpers, and the workspace claim
- **Consumers**: Helm rendering and Kubernetes Deployment controllers
- **Notes**: Controller mounts use `$HOME/.pi` and `$HOME/.multica/pi-sessions`; ConfigMap paths are copied relative to `$HOME/.pi` without adding an `agent` directory; provider-specific home environment variables are not set by the chart.

### `workspace-pvc.yaml`
- **Purpose**: Provisions the shared persistent workspace required by separate controller and task Pods.
- **Dependencies**: Workspace values and naming helpers
- **Consumers**: `deployment.yaml` and task Pods launched by the runtime controller

### `networkpolicy.yaml`
- **Purpose**: Restricts controller and dynamically launched worker Pod ingress when network policy is enabled.
- **Dependencies**: Network-policy values and chart naming helpers
- **Consumers**: Kubernetes network-policy controllers
