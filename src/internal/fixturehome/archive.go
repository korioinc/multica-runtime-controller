package fixturehome

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

const (
	CaptureDirectory  = "task-homes"
	PreserveSampleEnv = "VERIFYRUNTIME_PRESERVE_SAMPLE"
)

type identity struct {
	SchemaVersion int              `json:"schemaVersion"`
	TaskID        string           `json:"taskID"`
	AttemptID     string           `json:"attemptID"`
	OwnerID       string           `json:"ownerID"`
	WorkerSubPath string           `json:"workerSubPath"`
	Provider      string           `json:"provider"`
	RuntimeRef    runtimeimage.Ref `json:"runtimeRef"`
}

func identityFor(request wire.Request) identity {
	return identity{1, request.TaskID, request.AttemptID, request.OwnerID, request.WorkerSubPath, request.Provider, request.RuntimeRef}
}

func (i identity) matches(request wire.Request) bool {
	return i.SchemaVersion == 1 && i.TaskID == request.TaskID && i.AttemptID == request.AttemptID && i.OwnerID == request.OwnerID && i.WorkerSubPath == request.WorkerSubPath && i.Provider == request.Provider && i.RuntimeRef.Equal(request.RuntimeRef)
}

// Publish copies a captured, digest-verified producer artifact. Only attempt
// identity changes. An OCI index may name the same image payload when its actual
// registry bytes prove that its native child is the captured manifest digest.
func (f *Fixture) Publish(ctx context.Context, next wire.Request, index []byte) (string, error) {
	if err := f.available(ctx); err != nil {
		return "", err
	}
	if next.TaskID != f.sample.TaskID || next.OwnerID != f.sample.OwnerID || next.WorkerSubPath != f.sample.WorkerSubPath || next.Provider != f.sample.Provider || !wire.UUID(next.AttemptID) || next.AttemptID == f.sample.AttemptID {
		return "", errors.New("fixture HOME must remain bound to the captured task and storage")
	}
	if err := next.RuntimeRef.Validate(); err != nil {
		return "", err
	}
	if err := sameImageContents(f.sample.RuntimeRef, next.RuntimeRef, index); err != nil {
		return "", err
	}
	return rebindArchive(f.root, filepath.Join("/verification-evidence", CaptureDirectory, f.sample.AttemptID+".tar"), f.sample, next)
}

func sameImageContents(source, next runtimeimage.Ref, raw []byte) error {
	sameReference := source.Equal(next)
	if sameReference && raw == nil {
		return nil
	}
	comparable := next
	comparable.Image = source.Image
	if !source.Equal(comparable) {
		return errors.New("fixture HOME cannot be rebound to different runtime contents")
	}
	sum := sha256.Sum256(raw)
	if !strings.HasSuffix(next.Image, "@sha256:"+hex.EncodeToString(sum[:])) {
		return errors.New("fixture OCI index bytes do not match the requested image")
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
	if json.Unmarshal(raw, &index) != nil || index.SchemaVersion != 2 || index.MediaType != "application/vnd.oci.image.index.v1+json" && index.MediaType != "application/vnd.docker.distribution.manifest.list.v2+json" {
		return errors.New("fixture image is not a supported OCI index")
	}
	_, digest, ok := strings.Cut(source.Image, "@")
	if !ok {
		return errors.New("captured image has no manifest digest")
	}
	native := 0
	for _, entry := range index.Manifests {
		if entry.Platform.OS == "unknown" && entry.Platform.Architecture == "unknown" && entry.Annotations["vnd.docker.reference.type"] == "attestation-manifest" {
			continue
		}
		if entry.Platform.OS+"/"+entry.Platform.Architecture != source.Platform || !strings.HasPrefix(entry.Digest, "sha256:") || !core.ValidSHA(strings.TrimPrefix(entry.Digest, "sha256:")) || !sameReference && entry.Digest != digest {
			return errors.New("fixture index selects different HOME image contents")
		}
		native++
	}
	if native != 1 {
		return errors.New("fixture index must select exactly one captured native image")
	}
	return nil
}

func rebindArchive(root, sourcePath string, sample, next wire.Request) (string, error) {
	directory := filepath.Join(root, ".runtime-home")
	if err := realDirectory(root); err != nil {
		return "", err
	}
	rootInfo, err := os.Stat(root)
	if err != nil {
		return "", err
	}
	owner, ok := rootInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return "", errors.New("worker storage ownership is unavailable")
	}
	if err := os.Mkdir(directory, 0700); err == nil {
		if err := os.Chown(directory, int(owner.Uid), int(owner.Gid)); err != nil {
			return "", err
		}
		parent, err := os.Open(root)
		if err != nil {
			return "", err
		}
		if err := errors.Join(parent.Sync(), parent.Close()); err != nil {
			return "", err
		}
	} else if !errors.Is(err, os.ErrExist) {
		return "", err
	}
	if err := realDirectory(directory); err != nil {
		return "", err
	}
	source, err := openRegular(sourcePath, os.O_RDONLY)
	if err != nil {
		return "", err
	}
	defer source.Close()
	info, err := source.Stat()
	if err != nil {
		return "", err
	}
	file, err := os.CreateTemp(directory, ".fixture-home-")
	if err != nil {
		return "", err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	// The capture belongs to the backend's evidence volume. The new artifact
	// must be readable by the owner of the actual unprivileged worker storage.
	if err := file.Chown(int(owner.Uid), int(owner.Gid)); err != nil {
		return "", err
	}
	sourceHash, nextHash := sha256.New(), sha256.New()
	input := &countedReader{Reader: io.TeeReader(source, sourceHash)}
	if err := consumeIdentity(input, sample); err != nil {
		return "", err
	}
	output := io.MultiWriter(file, nextHash)
	raw, err := json.Marshal(identityFor(next))
	if err != nil {
		return "", err
	}
	writer := tar.NewWriter(output)
	if err := writer.WriteHeader(&tar.Header{Name: "identity.json", Typeflag: tar.TypeReg, Mode: 0600, Size: int64(len(raw))}); err != nil {
		return "", err
	}
	if _, err := writer.Write(raw); err != nil {
		return "", err
	}
	if err := writer.Flush(); err != nil {
		return "", err
	}
	// Preserve the producer's encoded HOME body, including headers, links and
	// end padding. Parse only tar framing; no HOME composition or install policy
	// is shared with production or reimplemented by the verifier.
	if err := copyArchiveBody(output, input); err != nil {
		return "", err
	}
	if input.count != info.Size() || hex.EncodeToString(sourceHash.Sum(nil)) != sample.HomeDigest {
		return "", errors.New("captured HOME artifact bytes differ from the selected digest")
	}
	if err := file.Sync(); err != nil {
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	if err := os.Link(file.Name(), filepath.Join(directory, next.AttemptID+".tar")); err != nil {
		return "", err
	}
	dir, err := os.Open(directory)
	if err != nil {
		return "", err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return "", err
	}
	return hex.EncodeToString(nextHash.Sum(nil)), nil
}

// CopyCapturedArchive validates a producer's complete streaming upload while
// preserving its original bytes. Callers publish only after this succeeds.
func CopyCapturedArchive(destination io.Writer, source io.Reader, request wire.Request) error {
	hash := sha256.New()
	input := io.TeeReader(source, io.MultiWriter(destination, hash))
	if err := consumeIdentity(input, request); err != nil {
		return err
	}
	if err := copyArchiveBody(io.Discard, input); err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != request.HomeDigest {
		return errors.New("captured HOME archive differs from the mounted request digest")
	}
	return nil
}

func consumeIdentity(input io.Reader, request wire.Request) error {
	reader := tar.NewReader(input)
	header, err := reader.Next()
	if err != nil || header.Name != "identity.json" || header.Typeflag != tar.TypeReg || header.Size <= 0 || header.Size > wire.MaxRequestBytes {
		return errors.New("captured HOME archive has no bounded task identity")
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	var captured identity
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&captured) != nil || decoder.Decode(new(any)) != io.EOF || !captured.matches(request) {
		return errors.New("captured HOME artifact identity differs from its request")
	}
	_, err = io.CopyN(io.Discard, input, (512-header.Size%512)%512)
	return err
}

type countedReader struct {
	io.Reader
	count int64
}

func (r *countedReader) Read(raw []byte) (int, error) {
	n, err := r.Reader.Read(raw)
	r.count += int64(n)
	return n, err
}

func copyArchiveBody(destination io.Writer, source io.Reader) error {
	input := &countedReader{Reader: io.TeeReader(source, destination)}
	reader := tar.NewReader(input)
	first := true
	for {
		before := input.count
		header, err := reader.Next()
		if err == io.EOF {
			if first || input.count-before < 1024 {
				return errors.New("captured HOME archive is incomplete")
			}
			break
		}
		if err != nil {
			return err
		}
		if first && (header.Typeflag != tar.TypeDir || strings.TrimSuffix(header.Name, "/") != "home") {
			return errors.New("captured HOME archive has no HOME root")
		}
		first = false
		if _, err := io.Copy(io.Discard, reader); err != nil {
			return err
		}
	}
	var padding [32 << 10]byte
	for {
		n, err := input.Read(padding[:])
		if len(bytes.Trim(padding[:n], "\x00")) != 0 {
			return errors.New("captured HOME archive has trailing payload")
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
