package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"

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
	// HOME rebinding also proves that this index selects the sample's exact
	// native manifest; other runtime/configuration contents remain unchanged.
	ref, request, err := f.attemptWithImage(image, raw)
	if err != nil {
		return err
	}
	session, err := wire.PiSession(request)
	if err != nil {
		return err
	}
	request.Args = []string{"--version", "--session", session}
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
