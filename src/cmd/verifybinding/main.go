// verifybinding exercises production startup in an explicitly disposable image.
// The only API is an in-process TLS fixture reporting the prior Pod image.
package main

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func main() {
	priorPath := flag.String("prior-descriptor", "", "actual prior image descriptor")
	priorImage := flag.String("prior-image", "", "actual published prior image repository digest")
	evidence := flag.String("evidence", "", "JSON evidence file")
	flag.Parse()
	if err := verify(*priorPath, *priorImage, *evidence); err != nil {
		fmt.Fprintln(os.Stderr, "binding fixture:", err)
		os.Exit(1)
	}
}

func verify(priorPath, priorImage, evidence string) error {
	if runtime.GOOS != "linux" || os.Geteuid() != 65532 || os.Getenv("LOCALVERIFY_DISPOSABLE_CONTAINER") != "true" {
		return errors.New("disposable Linux UID65532 container required")
	}
	if priorPath != "/evidence/prior-image.json" || evidence != "/evidence/binding.json" || flag.NArg() != 0 {
		return errors.New("fixed disposable evidence paths required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	current, currentDigest, err := runtimeimage.Check(ctx, runtimeimage.Root, core.Root, core.HostPlatform())
	if err != nil {
		return err
	}
	var prior runtimeimage.Descriptor
	priorRaw, err := runtimeimage.ReadJSON(priorPath, &prior)
	if err != nil {
		return err
	}
	if err = prior.Validate(current.Platform); err != nil {
		return err
	}
	if prior.ImageBuildID == current.ImageBuildID || !prior.Controller.Equal(current.Controller) {
		return errors.New("fixture requires different image builds with the same controller")
	}
	priorImage, err = runtimeimage.NormalizeImageID(priorImage)
	if err != nil {
		return err
	}
	for _, root := range []string{wire.Home, wire.ControlRoot, wire.WorkspaceRoot} {
		entries, err := os.ReadDir(root)
		if err != nil {
			return err
		}
		if len(entries) != 0 {
			return errors.New("binding fixture requires fresh private volumes")
		}
	}
	canary := filepath.Join(wire.WorkspaceRoot, "unfinished-user-work")
	if err = os.WriteFile(canary, []byte("preserve private workspace"), 0600); err != nil {
		return err
	}
	if err = runtimeimage.PublishReceipt(wire.ControlRoot, prior, core.Digest(priorRaw)); err != nil {
		return err
	}
	b := configuration.Bundle{SchemaVersion: 1, Groups: []configuration.Group{}}
	b.Digest = configuration.Digest(b.Groups)
	if _, err = configuration.Commit(wire.ControlRoot, b); err != nil {
		return err
	}
	uid := uuid.NewString()
	pod := corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: "fixture", UID: types.UID(uid)}, Spec: corev1.PodSpec{NodeName: "fixture-node", NodeSelector: map[string]string{"kubernetes.io/os": "linux", "kubernetes.io/arch": strings.TrimPrefix(current.Platform, "linux/")}, Containers: []corev1.Container{{Name: "controller", Image: priorImage}}, InitContainers: []corev1.Container{{Name: "home-layout", Image: priorImage}}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "controller", ImageID: priorImage}}, InitContainerStatuses: []corev1.ContainerStatus{{Name: "home-layout", ImageID: priorImage}}}}
	nonRoot, readOnly, escalation := true, true, false
	uid65532 := int64(65532)
	pod.Spec.SecurityContext = &corev1.PodSecurityContext{RunAsNonRoot: &nonRoot, RunAsUser: &uid65532}
	security := &corev1.SecurityContext{ReadOnlyRootFilesystem: &readOnly, AllowPrivilegeEscalation: &escalation, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}
	pod.Spec.Containers[0].SecurityContext = security
	pod.Spec.InitContainers[0].SecurityContext = security
	var reads, mutations, backendCalls atomic.Int64
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/fixture/pods/controller" {
			reads.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(pod)
			return
		}
		mutations.Add(1)
		http.Error(w, "no mutation in binding fixture", http.StatusForbidden)
	}))
	defer api.Close()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls.Add(1)
		http.Error(w, "daemon must not start", http.StatusForbidden)
	}))
	defer backend.Close()
	endpoint, _ := url.Parse(api.URL)
	host, port, err := net.SplitHostPort(endpoint.Host)
	if err != nil {
		return err
	}
	serviceAccount := "/var/run/secrets/kubernetes.io/serviceaccount"
	for name, raw := range map[string][]byte{"token": []byte("disposable-kubernetes-fixture"), "ca.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: api.Certificate().Raw})} {
		if err = os.WriteFile(filepath.Join(serviceAccount, name), raw, 0600); err != nil {
			return err
		}
	}
	if err = os.Setenv("KUBERNETES_SERVICE_HOST", host); err != nil {
		return err
	}
	if err = os.Setenv("KUBERNETES_SERVICE_PORT", port); err != nil {
		return err
	}
	client, err := kubernetes.InCluster("fixture")
	if err != nil {
		return err
	}
	// A live positive control proves the stale fixture serves a valid pullable
	// index/manifest and platform through the same production binding client.
	bound, err := client.BindImage(ctx, "controller", uid, "controller", "fixture-node", current.Platform, current)
	if err != nil {
		return err
	}
	if bound.Image != priorImage {
		return errors.New("binding changed the reported repository digest")
	}
	if err = verifyImmutableAdmission(ctx, client, &pod, current); err != nil {
		return err
	}
	beforeReads := reads.Load()
	owner := uuid.NewString()
	worker := kubernetes.Config{Platform: current.Platform, ImagePullPolicy: corev1.PullAlways, WorkspaceClaim: "workspace", WorkspaceAccessMode: corev1.ReadWriteOnce, SingleNodeName: "fixture-node", ServiceAccount: "worker", TaskDeadlineSeconds: 60, TerminationGraceSeconds: 10}
	if err = worker.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(worker)
	if err != nil {
		return err
	}
	if err = os.WriteFile("/tmp/binding-worker.json", raw, 0600); err != nil {
		return err
	}
	if err = os.WriteFile("/tmp/binding-token", []byte("mul_disposable_binding"), 0600); err != nil {
		return err
	}
	command := exec.CommandContext(ctx, core.Root+"/runtime", "controller")
	command.Env = append(os.Environ(), "POD_NAMESPACE=fixture", "POD_NAME=controller", "POD_UID="+uid, "POD_NODE_NAME=fixture-node", "POD_CONTAINER_NAME=controller", "MULTICA_OWNER_ID="+owner, "MULTICA_DAEMON_ID="+owner, "MULTICA_WORKER_CONFIG_FILE=/tmp/binding-worker.json", "MULTICA_CONTROLLER_TOKEN_FILE=/tmp/binding-token", "MULTICA_BASE_URL="+backend.URL, "MULTICA_DAEMON_PROXY_URL=http://controller:8080", "MULTICA_STARTUP_TIMEOUT=5s")
	out, runErr := command.CombinedOutput()
	if err = os.WriteFile("/evidence/binding-controller.log", out, 0600); err != nil {
		return err
	}
	if runErr == nil || ctx.Err() != nil {
		return errors.New("new rootfs did not reject the old init receipt promptly")
	}
	if reads.Load() != beforeReads || mutations.Load() != 0 || backendCalls.Load() != 0 {
		return errors.New("stale-status admission reached API or daemon activity")
	}
	data, err := os.ReadFile(canary)
	if err != nil || string(data) != "preserve private workspace" {
		return errors.New("rejected startup modified existing work")
	}
	entries, err := os.ReadDir(wire.WorkspaceRoot)
	if err != nil || len(entries) != 1 {
		return errors.New("rejected startup initialized application workspace")
	}
	if err = runtimeimage.CheckReceipt(wire.ControlRoot, prior, core.Digest(priorRaw)); err != nil {
		return errors.New("rejected startup overwrote the prior receipt")
	}
	if err = runtimeimage.CheckReceipt(wire.ControlRoot, current, currentDigest); err == nil {
		return errors.New("fixture failed to distinguish the old receipt")
	}
	result := map[string]any{"passed": true, "platform": current.Platform, "priorImage": priorImage, "priorBuildID": prior.ImageBuildID, "currentBuildID": current.ImageBuildID, "liveAPIBinding": true, "immutableAdmission": true, "staleStatusRejectedBeforeAPI": true, "daemonCalls": backendCalls.Load(), "workspacePreserved": true, "receiptPreserved": true}
	raw, err = json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(evidence, append(raw, '\n'), 0600)
}
