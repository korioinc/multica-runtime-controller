package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"

	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
)

type configCopies []configuration.Copy

func (c *configCopies) String() string { return "sourceGroup/source/target JSON" }
func (c *configCopies) Set(raw string) error {
	var copy configuration.Copy
	if err := runtimeimage.Decode([]byte(raw), &copy); err != nil {
		return err
	}
	*c = append(*c, copy)
	return nil
}

func layoutHome(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("home layout", flag.ContinueOnError)
	root := flags.String("private-root", "", "")
	storageRoot := flags.String("storage-root", "", "")
	var copies configCopies
	flags.Var(&copies, "config-copy", "")
	if err := flags.Parse(args); err != nil {
		return err
	}
	resolved, err := filepath.EvalSymlinks(*root)
	if err != nil || resolved != *root || !filepath.IsAbs(*root) || *root == "/" || flags.NArg() != 0 {
		return errors.New("real private volume root required")
	}
	d, digest, err := runtimeimage.Check(ctx, runtimeimage.Root, core.Root, core.HostPlatform())
	if err != nil {
		return err
	}
	for _, name := range []string{"run", "tmp"} {
		path := filepath.Join(*root, name)
		if err := os.MkdirAll(path, 0700); err != nil {
			return err
		}
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() {
			return errors.New("private path is not a directory")
		}
		if err := os.Chmod(path, 0700); err != nil {
			return err
		}
	}
	if *storageRoot != "" {
		resolved, err := filepath.EvalSymlinks(*storageRoot)
		if err != nil || resolved != *storageRoot || !filepath.IsAbs(*storageRoot) || *storageRoot == "/" {
			return errors.New("real storage volume required")
		}
		storage, err := os.OpenRoot(*storageRoot)
		if err != nil {
			return err
		}
		defer storage.Close()
		directory, err := storage.Open(".")
		if err != nil {
			return err
		}
		defer directory.Close()
		entries, err := directory.ReadDir(-1)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if (entry.Name() != "controller" && entry.Name() != "nfs-recovery" && entry.Name() != "workspace" && entry.Name() != "lost+found") || !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
				return errors.New("storage volume contains unknown data; prepared layout required")
			}
		}
		for _, name := range []string{"controller", "nfs-recovery", "workspace"} {
			if err := storage.Mkdir(name, 0700); err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}
			info, err := storage.Lstat(name)
			if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				return errors.New("storage directory is not real")
			}
		}
		if err := directory.Sync(); err != nil {
			return err
		}
	}
	run := filepath.Join(*root, "run")
	if _, err := configuration.CaptureOrRead(run, copies, configuration.InputRoot); err != nil {
		return err
	}
	return runtimeimage.PublishReceipt(run, d, digest)
}
