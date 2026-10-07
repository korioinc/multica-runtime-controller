package checkout

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

// Copy publishes independent files from a controller-owned immutable checkout.
// Callers retain the snapshot and the attempt's writer authority until return.
// Failed stages remain available for recovery; existing task data is untouched.
func Copy(ctx context.Context, taskRoot, workdir, remote string, source *os.Root) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := validateURL(remote); err != nil {
		return "", err
	}
	if source == nil {
		return "", errors.New("repository source is unavailable")
	}
	if err := verifyCheckout(ctx, source, "."); err != nil {
		return "", err
	}
	config, err := source.OpenFile(".git/config", os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return "", err
	}
	origin, readErr := readOrigin(ctx, config)
	if err := errors.Join(readErr, config.Close()); err != nil || strings.TrimSpace(string(origin)) != remote {
		return "", errors.Join(errors.New("repository source belongs to another remote"), err)
	}
	name, err := DirectoryName(remote)
	if err != nil {
		return "", err
	}
	root, err := openCheckoutRoot(taskRoot, workdir)
	if err != nil {
		return "", err
	}
	defer root.Close()
	if _, err := root.Lstat(name); !errors.Is(err, os.ErrNotExist) {
		return "", errors.New("repository destination is occupied")
	}
	stage := ".checkout-" + uuid.NewString()
	if err := root.Mkdir(stage, 0700); err != nil {
		return "", err
	}
	repository, err := root.OpenRoot(stage)
	if err != nil {
		return "", err
	}
	defer repository.Close()
	if err := copyCheckoutTree(ctx, source, repository, nil); err != nil {
		return "", err
	}
	if err := verifyCheckout(ctx, repository, "."); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := publishCheckout(ctx, root, repository, stage, name); err != nil {
		return "", err
	}
	return filepath.Join(workdir, name), nil
}

type checkoutReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *checkoutReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
