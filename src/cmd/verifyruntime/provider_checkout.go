package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

func verifyProviderCheckout(ctx context.Context, request wire.Request, backend string, report *providerResult) error {
	if len(request.RepositoryURLs) != 1 {
		return errors.New("fixture expected its single assigned local repository")
	}
	assigned := request.RepositoryURLs[0]
	if _, err := command(ctx, report.WorkDir, request.RuntimeRef.Daemon.Path, "repo", "checkout", assigned); err != nil {
		return err
	}
	repository, err := findRepository(report.WorkDir)
	if err != nil {
		return err
	}
	report.Repository = repository
	data, err := mutateProviderCheckout(ctx, request, report)
	if err != nil {
		return err
	}
	if err := verifyRepeatedCheckout(ctx, request, report, data); err != nil {
		return err
	}
	if _, err := command(ctx, report.WorkDir, request.RuntimeRef.Daemon.Path, "repo", "checkout", backend+"/git/forbidden.git"); err == nil {
		return errors.New("worker checked out a repository outside its observed scope")
	}
	report.ScopeChecked = true
	return nil
}

func mutateProviderCheckout(ctx context.Context, request wire.Request, report *providerResult) ([]byte, error) {
	data, err := os.ReadFile(filepath.Join(report.Repository, "tracked.txt"))
	if err != nil {
		return nil, err
	}
	report.PriorWork = bytes.Contains(data, []byte("runtime fixture mutation"))
	report.PriorWorkDigest = core.Digest(data)
	if !report.PriorWork {
		if _, err := command(ctx, report.Repository, "git", "-c", "core.hooksPath=/dev/null", "switch", "-c", "fixture-user-work"); err != nil {
			return nil, err
		}
	}
	branch, err := command(ctx, report.Repository, "git", "-c", "core.hooksPath=/dev/null", "branch", "--show-current")
	if err != nil {
		return nil, err
	}
	report.Branch = strings.TrimSpace(string(branch))
	data = append(data, []byte("runtime fixture mutation "+request.TaskID+" "+request.AttemptID+"\n")...)
	if err := os.WriteFile(filepath.Join(report.Repository, "tracked.txt"), data, 0600); err != nil {
		return nil, err
	}
	report.ModifiedDigest = core.Digest(data)
	return data, nil
}

func verifyRepeatedCheckout(ctx context.Context, request wire.Request, report *providerResult, data []byte) error {
	userHook := []byte("#!/bin/sh\n# runtime fixture user's hook\nexit 42\n")
	hook := filepath.Join(report.Repository, ".git/hooks/pre-commit")
	if err := os.WriteFile(hook, userHook, 0700); err != nil {
		return err
	}
	if _, err := command(ctx, report.WorkDir, request.RuntimeRef.Daemon.Path, "repo", "checkout", request.RepositoryURLs[0]); err != nil {
		return err
	}
	after, err := os.ReadFile(filepath.Join(report.Repository, "tracked.txt"))
	if err != nil || !bytes.Equal(after, data) {
		return errors.New("repeated official checkout replaced worker edits")
	}
	afterBranch, err := command(ctx, report.Repository, "git", "-c", "core.hooksPath=/dev/null", "branch", "--show-current")
	if err != nil || strings.TrimSpace(string(afterBranch)) != report.Branch {
		return errors.New("repeated checkout replaced the worker's branch")
	}
	afterHook, err := os.ReadFile(hook)
	if err != nil || !bytes.Equal(afterHook, userHook) {
		return errors.New("repeated checkout replaced a user Git hook")
	}
	report.RepeatCheckoutChecked = true
	return nil
}

func command(ctx context.Context, dir, executable string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, executable, args...)
	cmd.Dir = dir
	raw, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%s: %w: %s", filepath.Base(executable), err, strings.TrimSpace(string(raw)))
	}
	return raw, nil
}

func findRepository(root string) (string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return "", err
	}
	found := ""
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(root, entry.Name())
		info, err := os.Lstat(filepath.Join(path, ".git"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", err
		}
		if !info.IsDir() || found != "" {
			return "", errors.New("checkout did not provide one standalone Git directory")
		}
		for _, name := range []string{"objects/info/alternates", "commondir"} {
			if _, err := os.Stat(filepath.Join(path, ".git", name)); !errors.Is(err, os.ErrNotExist) {
				return "", errors.New("checkout shares external Git state")
			}
		}
		found = path
	}
	if found == "" {
		return "", errors.New("checkout repository was not published")
	}
	return found, nil
}
