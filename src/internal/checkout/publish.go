package checkout

import (
	"context"
	"errors"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

const pendingCheckout = "multica-checkout-pending"

func publishCheckout(ctx context.Context, root, repository *os.Root, stage, name string) error {
	parent, err := root.Open(".")
	if err != nil {
		return err
	}
	defer parent.Close()
	if err := checkoutDirectoryMatches(root, repository, stage); err != nil {
		return err
	}
	err = renameCheckout(int(parent.Fd()), stage, name)
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS) {
		return copyCheckout(ctx, root, repository, stage, name)
	}
	if err != nil {
		return err
	}
	return checkoutDirectoryMatches(root, repository, name)
}

func checkoutDirectoryMatches(root, directory *os.Root, name string) error {
	expected, err := directory.Stat(".")
	if err != nil {
		return err
	}
	current, err := root.Lstat(name)
	if err != nil || !os.SameFile(expected, current) {
		return errors.New("repository directory identity changed")
	}
	return nil
}

// NFS does not implement rename-without-replacement. Reserve a real directory
// exclusively and keep it unreadable to Retain until copying has completed.
// The directory may be visible to the provider before the CLI returns.
func copyCheckout(ctx context.Context, root, source *os.Root, stage, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := root.Mkdir(name, 0700); err != nil {
		return err
	}
	destination, err := root.OpenRoot(name)
	if err != nil {
		return err
	}
	defer destination.Close()
	if err := destination.Mkdir(".git", 0700); err != nil {
		return err
	}
	git, err := destination.OpenRoot(".git")
	if err != nil {
		return err
	}
	defer git.Close()
	pending, err := git.OpenFile(pendingCheckout, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	marker, statErr := pending.Stat()
	if err := errors.Join(statErr, pending.Close()); err != nil {
		return err
	}
	if err := copyCheckoutTree(ctx, source, destination, git); err != nil {
		return err
	}
	if err := verifyCheckout(ctx, destination, "."); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkoutDirectoryMatches(root, destination, name); err != nil {
		return err
	}
	current, err := git.Lstat(pendingCheckout)
	if err != nil || !os.SameFile(marker, current) {
		return errors.New("repository publication marker changed")
	}
	if err := git.Remove(pendingCheckout); err != nil {
		return err
	}
	if err := checkoutDirectoryMatches(root, destination, name); err != nil {
		return err
	}
	// Only the opened stage is ours. Never recursively remove its pathname,
	// which a provider can replace while the copy is in progress.
	if removeCheckoutTree(ctx, source) == nil {
		original, err := source.Stat(".")
		current, currentErr := root.Lstat(stage)
		if err == nil && currentErr == nil && os.SameFile(original, current) {
			if parent, err := root.Open("."); err == nil {
				_ = unix.Unlinkat(int(parent.Fd()), stage, unix.AT_REMOVEDIR)
				_ = parent.Close()
			}
		}
	}
	return checkoutDirectoryMatches(root, destination, name)
}

func copyCheckoutTree(ctx context.Context, source, destination, reservedGit *os.Root) error {
	directory, err := source.Open(".")
	if err != nil {
		return err
	}
	entries, readErr := directory.ReadDir(-1)
	if err := errors.Join(readErr, directory.Close()); err != nil {
		return err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := entry.Name()
		info, err := source.Lstat(name)
		if err != nil {
			return err
		}
		switch {
		case info.IsDir():
			from, err := source.OpenRoot(name)
			if err != nil {
				return err
			}
			opened, err := from.Stat(".")
			if err != nil || !os.SameFile(info, opened) {
				from.Close()
				return errors.New("repository source directory changed")
			}
			var to *os.Root
			if reservedGit != nil && name == ".git" {
				to = reservedGit
			} else {
				if err = destination.Mkdir(name, 0700); err == nil {
					to, err = destination.OpenRoot(name)
				}
			}
			if err == nil {
				err = copyCheckoutTree(ctx, from, to, nil)
			}
			from.Close()
			if to != nil && to != reservedGit {
				to.Close()
			}
			if err != nil {
				return err
			}
		case info.Mode().IsRegular():
			from, err := source.OpenFile(name, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
			if err != nil {
				return err
			}
			opened, err := from.Stat()
			if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
				from.Close()
				return errors.New("repository source file changed")
			}
			to, err := destination.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm())
			if err != nil {
				from.Close()
				return err
			}
			_, copyErr := io.Copy(to, &checkoutReader{ctx: ctx, reader: from})
			if err := errors.Join(copyErr, from.Close(), to.Close()); err != nil {
				return err
			}
		case info.Mode()&os.ModeSymlink != 0:
			target, err := source.Readlink(name)
			if err != nil {
				return err
			}
			if err := destination.Symlink(target, name); err != nil {
				return err
			}
		default:
			return errors.New("repository stage contains a special file")
		}
	}
	return nil
}

func removeCheckoutTree(ctx context.Context, root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	entries, readErr := directory.ReadDir(-1)
	if err := errors.Join(readErr, directory.Close()); err != nil {
		return err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			child, err := root.OpenRoot(entry.Name())
			if err != nil {
				return err
			}
			err = removeCheckoutTree(ctx, child)
			child.Close()
			if err != nil {
				return err
			}
		}
		if err := root.Remove(entry.Name()); err != nil {
			return err
		}
	}
	return nil
}
