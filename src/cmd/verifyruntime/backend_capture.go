package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/korioinc/multica-runtime-controller/internal/fixturehome"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"golang.org/x/sys/unix"
)

// captureHome runs outside the backend state mutex while streaming large npm
// trees. Only an immutable, verified capture may become the latest request.
func (f *runtimeBackend) captureHome(w http.ResponseWriter, r *http.Request) {
	if err := f.receiveHome(r); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]bool{"captured": true})
}

func (f *runtimeBackend) receiveHome(r *http.Request) error {
	form, err := r.MultipartReader()
	if err != nil {
		return err
	}
	metadata, err := form.NextRawPart()
	if err != nil || metadata.FormName() != "request" {
		return errors.New("HOME capture must begin with its mounted request")
	}
	raw, err := io.ReadAll(io.LimitReader(metadata, wire.MaxRequestBytes+1))
	if err != nil {
		return err
	}
	request, err := wire.Decode(raw)
	if err != nil {
		return err
	}
	f.mutex.Lock()
	epoch, err := f.homeCaptureEpoch(request)
	f.mutex.Unlock()
	if err != nil {
		return err
	}
	payload, err := form.NextRawPart()
	if err != nil || payload.FormName() != "archive" {
		return errors.New("HOME capture requires the producer archive")
	}
	directory := filepath.Join(f.evidence, fixturehome.CaptureDirectory)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	if err := syncCaptureDirectory(f.evidence); err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, ".home-capture-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if err := fixturehome.CopyCapturedArchive(file, payload, request); err != nil {
		return err
	}
	if _, err := form.NextRawPart(); err != io.EOF {
		return errors.New("HOME capture has unexpected trailing parts")
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	if err := publishCaptureFile(file.Name(), filepath.Join(directory, request.AttemptID+".tar"), request.HomeDigest); err != nil {
		return err
	}
	metadataFile, err := os.CreateTemp(directory, ".request-capture-")
	if err != nil {
		return err
	}
	defer os.Remove(metadataFile.Name())
	if _, err := metadataFile.Write(raw); err != nil {
		metadataFile.Close()
		return err
	}
	if err := errors.Join(metadataFile.Sync(), metadataFile.Close()); err != nil {
		return err
	}
	if err := publishCaptureFile(metadataFile.Name(), filepath.Join(directory, request.AttemptID+".json"), wire.Digest(raw)); err != nil {
		return err
	}
	f.mutex.Lock()
	defer f.mutex.Unlock()
	current, err := f.homeCaptureEpoch(request)
	if err != nil || current != epoch {
		return errors.New("fixture claim changed while its HOME was being captured")
	}
	if wire.Value(request.Env, fixturehome.PreserveSampleEnv) == "true" {
		// Resource/transport verifiers retain their source sample across retries,
		// including execution through an equivalent OCI-index image reference.
		return nil
	}
	latest, err := os.CreateTemp(f.evidence, ".latest-request-")
	if err != nil {
		return err
	}
	defer os.Remove(latest.Name())
	if _, err := latest.Write(raw); err != nil {
		latest.Close()
		return err
	}
	if err := errors.Join(latest.Sync(), latest.Close()); err != nil {
		return err
	}
	if err := os.Rename(latest.Name(), filepath.Join(f.evidence, "request.json")); err != nil {
		return err
	}
	return syncCaptureDirectory(f.evidence)
}

// The caller holds the backend state mutex. Capturing --version must not replace
// the task's independently collected provider or repository evidence.
func (f *runtimeBackend) homeCaptureEpoch(request wire.Request) (string, error) {
	record := f.state.Tasks[request.TaskID]
	if record == nil || record.Transport == "" || record.Input.Case != wire.Value(request.Env, "VERIFYRUNTIME_CASE") || record.Claim["id"] != request.TaskID || record.Claim["auth_token"] != wire.Value(request.Env, "MULTICA_TOKEN") || record.Claim["workspace_id"] != wire.Value(request.Env, "MULTICA_WORKSPACE_ID") || record.Claim["agent_id"] != wire.Value(request.Env, "MULTICA_AGENT_ID") {
		return "", errors.New("HOME capture does not match an actually claimed fixture task")
	}
	return record.Epoch, nil
}

func publishCaptureFile(temporary, destination, digest string) error {
	if err := os.Link(temporary, destination); errors.Is(err, os.ErrExist) {
		file, err := os.OpenFile(destination, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
		if err != nil {
			return err
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil || !info.Mode().IsRegular() {
			return errors.New("existing HOME capture is not a regular file")
		}
		hash := sha256.New()
		if _, err := io.Copy(hash, file); err != nil {
			return err
		}
		if hex.EncodeToString(hash.Sum(nil)) != digest {
			return errors.New("attempt capture already exists with different bytes")
		}
	} else if err != nil {
		return err
	}
	return syncCaptureDirectory(filepath.Dir(destination))
}

func syncCaptureDirectory(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}
