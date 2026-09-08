package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	runtimekube "github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (f *fixture) indexWorker() error {
	image, err := runtimeimage.NormalizeImageID(f.indexImage)
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(f.indexManifest)
	if err != nil {
		return err
	}
	if !strings.HasSuffix(image, "@sha256:"+core.Digest(raw)) {
		return errors.New("raw registry index differs from its digest")
	}
	var index struct {
		SchemaVersion int    `json:"schemaVersion"`
		MediaType     string `json:"mediaType"`
		Manifests     []struct {
			Digest   string `json:"digest"`
			Platform struct {
				OS           string `json:"os"`
				Architecture string `json:"architecture"`
			} `json:"platform"`
			Annotations map[string]string `json:"annotations"`
		} `json:"manifests"`
	}
	if err = json.Unmarshal(raw, &index); err != nil {
		return err
	}
	if index.SchemaVersion != 2 || index.MediaType != "application/vnd.oci.image.index.v1+json" && index.MediaType != "application/vnd.docker.distribution.manifest.list.v2+json" {
		return errors.New("registry object is not an executable OCI index")
	}
	native := 0
	for _, entry := range index.Manifests {
		if entry.Platform.OS == "unknown" && entry.Platform.Architecture == "unknown" && entry.Annotations["vnd.docker.reference.type"] == "attestation-manifest" {
			continue
		}
		if entry.Platform.OS+"/"+entry.Platform.Architecture != f.selection.RuntimeRef.Platform || !strings.HasPrefix(entry.Digest, "sha256:") || !core.ValidSHA(strings.TrimPrefix(entry.Digest, "sha256:")) {
			return errors.New("index has an unexpected executable platform")
		}
		native++
	}
	if native != 1 {
		return errors.New("index must select exactly one native executable")
	}
	ref, request, err := f.attempt()
	if err != nil {
		return err
	}
	session, err := wire.PiSession(request)
	if err != nil {
		return err
	}
	request.Args = []string{"--version", "--session", session}
	request.RuntimeRef.Image = image
	ref.RuntimeRef = request.RuntimeRef
	raw, err = json.Marshal(request)
	if err != nil {
		return err
	}
	if _, err = wire.Decode(raw); err != nil {
		return err
	}
	ref.RequestDigest = wire.Digest(raw)
	ref.PodDigest, err = runtimekube.PodFingerprint(f.selection.Worker, ref, request, f.selection.Gateway)
	if err != nil {
		return err
	}
	if err = f.createSecret(&ref, request); err != nil {
		return err
	}
	ref.PodUID, err = f.client.CreatePod(f.ctx, f.selection.Worker, ref, request, f.selection.Gateway)
	if err != nil {
		return err
	}
	pod, err := f.api.CoreV1().Pods(ref.Namespace).Get(f.ctx, ref.PodName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	f.keepPod(pod)
	for _, container := range append(pod.Spec.Containers, pod.Spec.InitContainers...) {
		if container.Image != image {
			return errors.New("worker did not retain the exact index reference")
		}
	}
	var output bytes.Buffer
	if err = f.client.Execute(f.ctx, ref, 3*time.Minute, runtimekube.Streams{Stdout: &output, Stderr: &output}); err != nil {
		return err
	}
	if !strings.Contains(output.String(), request.RuntimeRef.Providers[request.Provider].Version) {
		return errors.New("indexed worker did not run its installed provider")
	}
	if err = f.client.Cleanup(f.ctx, ref); err != nil {
		return err
	}
	return nil
}
