package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/intercept"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func collectRetiredStorage(ctx context.Context, cfg intercept.Config) {
	config, err := rest.InClusterConfig()
	if err != nil {
		slog.Warn("worker storage collection unavailable", "error", err)
		return
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		slog.Warn("worker storage collection unavailable", "error", err)
		return
	}
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		retired, err := intercept.CollectTaskStorage(callCtx, cfg.Namespace, client, cfg.Deadline)
		cancel()
		if err != nil {
			slog.Warn("worker storage collection deferred", "error", err)
		} else if retired > 0 {
			slog.Info("retired worker storage collected", "directories", retired)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
