package kubernetes

import (
	"context"
	"errors"
	"reflect"
	"unicode/utf8"

	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

const configurationAnnotation = "multica.ai/configuration-sha256"
const configurationGroupAnnotation = "multica.ai/configuration-group"

func snapshotName(owner Owner, bundleDigest, group string) string {
	return "runtime-config-" + core.Digest([]byte(owner.UID + "\x00" + bundleDigest + "\x00" + group))[:40]
}

func snapshotObject(namespace string, owner Owner, bundleDigest string, group configuration.Group) (*corev1.ConfigMap, error) {
	if namespace == "" || owner.Name == "" || owner.UID == "" || !core.ValidSHA(bundleDigest) {
		return nil, errors.New("configuration snapshot requires controller authority")
	}
	if err := group.Validate(); err != nil {
		return nil, err
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: snapshotName(owner, bundleDigest, group.Name), Namespace: namespace, Labels: map[string]string{managedLabel: managedValue}, Annotations: map[string]string{configurationAnnotation: configuration.GroupDigest(group), configurationGroupAnnotation: group.Name}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: owner.Name, UID: types.UID(owner.UID), Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true)}}}, Immutable: ptr.To(true), Data: map[string]string{}, BinaryData: map[string][]byte{}}
	for _, file := range group.Files {
		key := configuration.Key(file.Target)
		if utf8.Valid(file.Content) {
			cm.Data[key] = string(file.Content)
		} else {
			cm.BinaryData[key] = file.Content
		}
	}
	return cm, nil
}

func snapshotOwner(cm *corev1.ConfigMap, namespace, name, uid string, owner Owner) bool {
	if cm == nil || cm.Name != name || cm.Namespace != namespace || cm.UID == "" || uid != "" && string(cm.UID) != uid || cm.DeletionTimestamp != nil || cm.Immutable == nil || !*cm.Immutable || cm.Labels[managedLabel] != managedValue || len(cm.OwnerReferences) != 1 {
		return false
	}
	o := cm.OwnerReferences[0]
	return o.APIVersion == "v1" && o.Kind == "Pod" && o.Name == owner.Name && string(o.UID) == owner.UID && o.Controller != nil && *o.Controller && o.BlockOwnerDeletion != nil && *o.BlockOwnerDeletion
}

func snapshotGroup(cm *corev1.ConfigMap, ref configuration.SnapshotRef) (configuration.Group, error) {
	g := configuration.Group{Name: ref.SourceGroup, Directories: append([]string{}, ref.Directories...), Files: []configuration.File{}}
	if cm.Annotations[configurationAnnotation] != ref.Digest || cm.Annotations[configurationGroupAnnotation] != ref.SourceGroup || len(cm.Data)+len(cm.BinaryData) != len(ref.Mappings) {
		return g, errors.New("configuration snapshot metadata or payload mismatch")
	}
	for _, mapping := range ref.Mappings {
		value, text := cm.Data[mapping.Key]
		raw, binary := cm.BinaryData[mapping.Key]
		if text == binary {
			return g, errors.New("configuration snapshot has a missing or ambiguous key")
		}
		if text {
			raw = []byte(value)
		}
		if utf8.Valid(raw) != text {
			return g, errors.New("configuration snapshot data encoding differs from committed capture")
		}
		g.Files = append(g.Files, configuration.File{Target: mapping.Target, Mode: mapping.Mode, SHA256: core.Digest(raw), Content: raw})
	}
	if err := g.Validate(); err != nil {
		return g, err
	}
	if configuration.GroupDigest(g) != ref.Digest {
		return g, errors.New("configuration snapshot content or mapping differs")
	}
	return g, nil
}

// EnsureSnapshots reconciles a deterministic create even when its API response
// was lost. Existing objects are never patched, replaced or adopted by name.
func (c *Client) EnsureSnapshots(ctx context.Context, owner Owner, bundle configuration.Bundle) ([]configuration.SnapshotRef, error) {
	if err := bundle.Validate(); err != nil {
		return nil, err
	}
	refs := make([]configuration.SnapshotRef, 0, len(bundle.Groups))
	for _, group := range bundle.Groups {
		want, err := snapshotObject(c.Namespace, owner, bundle.Digest, group)
		if err != nil {
			return nil, err
		}
		actual, err := c.API.CoreV1().ConfigMaps(c.Namespace).Create(ctx, want, metav1.CreateOptions{})
		if err != nil {
			actual, err = c.API.CoreV1().ConfigMaps(c.Namespace).Get(ctx, want.Name, metav1.GetOptions{})
			if err != nil {
				return nil, diagnostics.ForGroup("configuration_snapshot_unavailable", group.Name, err)
			}
		}
		if !snapshotOwner(actual, c.Namespace, want.Name, "", owner) {
			return nil, diagnostics.ForGroup("configuration_snapshot_owner_mismatch", group.Name, errors.New("configuration snapshot name belongs to different authority"))
		}
		ref := configuration.SnapshotRef{Namespace: c.Namespace, Name: want.Name, UID: string(actual.UID), SourceGroup: group.Name, Digest: configuration.GroupDigest(group), Directories: append([]string{}, group.Directories...), Mappings: group.Mappings()}
		observed, err := snapshotGroup(actual, ref)
		if err != nil {
			return nil, diagnostics.ForGroup("configuration_snapshot_payload_mismatch", group.Name, err)
		}
		if !reflect.DeepEqual(group, observed) {
			return nil, diagnostics.ForGroup("configuration_snapshot_payload_mismatch", group.Name, errors.New("configuration snapshot does not preserve committed capture"))
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

// ValidateSnapshots is required both before resource creation and immediately
// before provider execution. Worker files prove contents; this proves API identity.
func (c *Client) ValidateSnapshots(ctx context.Context, owner Owner, refs []configuration.SnapshotRef, digest string) error {
	if err := configuration.ValidateRefs(refs); err != nil {
		return diagnostics.Wrap("configuration_snapshot_reference_invalid", err)
	}
	if configuration.DigestRefs(refs) != digest {
		return diagnostics.Wrap("configuration_snapshot_selection_mismatch", errors.New("snapshot selection differs from runtime configuration"))
	}
	for _, ref := range refs {
		if ref.Namespace != c.Namespace || ref.Name != snapshotName(owner, digest, ref.SourceGroup) {
			return diagnostics.ForGroup("configuration_snapshot_owner_mismatch", ref.SourceGroup, errors.New("configuration snapshot reference belongs to a different controller"))
		}
		cm, err := c.API.CoreV1().ConfigMaps(c.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return diagnostics.ForGroup("configuration_snapshot_unavailable", ref.SourceGroup, err)
		}
		if !snapshotOwner(cm, c.Namespace, ref.Name, ref.UID, owner) {
			return diagnostics.ForGroup("configuration_snapshot_identity_changed", ref.SourceGroup, errors.New("configuration snapshot object was replaced or changed"))
		}
		if _, err = snapshotGroup(cm, ref); err != nil {
			return diagnostics.ForGroup("configuration_snapshot_payload_mismatch", ref.SourceGroup, err)
		}
	}
	return nil
}
