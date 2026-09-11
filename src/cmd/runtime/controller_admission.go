package main

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/execution"
	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/official"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

type installedController struct {
	descriptor       runtimeimage.Descriptor
	descriptorDigest string
	bundle           configuration.Bundle
	worker           kubernetes.Config
}

type admittedController struct {
	descriptor  runtimeimage.Descriptor
	selection   execution.Selection
	resources   *kubernetes.Client
	environment []string
}

func inspectControllerInstallation(ctx context.Context, options controllerOptions) (installedController, error) {
	finishValidation := diagnostics.StartPhase("controller_image_validation")
	descriptor, descriptorDigest, err := runtimeimage.Check(ctx, runtimeimage.Root, core.Root, core.HostPlatform())
	finishValidation(err)
	if err != nil {
		return installedController{}, err
	}
	// Receipt validation precedes the API read: a restarted B rootfs must never
	// accept the old A status left briefly visible by the kubelet.
	if err = runtimeimage.CheckReceipt(wire.ControlRoot, descriptor, descriptorDigest); err != nil {
		return installedController{}, err
	}
	bundle, err := configuration.Read(wire.ControlRoot)
	if err != nil {
		return installedController{}, err
	}
	if err = execution.CheckPrivate(); err != nil {
		return installedController{}, err
	}
	cfg, err := kubernetes.LoadConfig(options.workerConfigFile)
	if err != nil {
		return installedController{}, err
	}
	if cfg.Platform != descriptor.Platform {
		return installedController{}, errors.New("worker policy differs from installed runtime platform")
	}
	if cfg.SingleNodeName != "" && options.nodeName != cfg.SingleNodeName {
		return installedController{}, errors.New("controller fixed Node mismatch")
	}
	return installedController{descriptor: descriptor, descriptorDigest: descriptorDigest, bundle: bundle, worker: cfg}, nil
}

func admitController(ctx context.Context, options controllerOptions) (admittedController, error) {
	installed, err := inspectControllerInstallation(ctx, options)
	if err != nil {
		return admittedController{}, err
	}
	resources, err := kubernetes.InCluster(options.namespace)
	if err != nil {
		return admittedController{}, err
	}
	startup, stopStartup := context.WithTimeout(ctx, options.startupTimeout)
	defer stopStartup()
	finishBinding := diagnostics.StartPhase("controller_image_binding")
	binding, err := resources.BindImage(startup, options.podName, options.podUID, options.containerName, options.nodeName, installed.descriptor.Platform, installed.descriptor)
	finishBinding(err)
	if err != nil {
		return admittedController{}, err
	}
	ref, err := installed.descriptor.Reference(binding.Image, installed.descriptorDigest, installed.bundle.Digest)
	if err != nil {
		return admittedController{}, err
	}
	vars, err := execution.SelectedEnvironment(installed.descriptor, options.environment, runtimeimage.Locations{Home: wire.Home, TmpDir: "/tmp", Workspace: wire.WorkspaceRoot})
	if err != nil {
		return admittedController{}, err
	}
	backend, err := official.NormalizeBackendURL(options.backendURL)
	if err != nil {
		return admittedController{}, err
	}
	selection := execution.Selection{SchemaVersion: execution.SelectionSchemaVersion, OwnerID: options.ownerID, Controller: binding.Owner, Namespace: resources.Namespace, Gateway: options.gatewayURL, Backend: backend, RuntimeRef: ref, Worker: installed.worker, OperatorKeys: execution.OperatorNames(options.environment)}
	selection.GitHubApp = wire.Value(vars, "GITHUB_APP_ID") != "" || wire.Value(vars, "GITHUB_APP_PRIVATE_KEY") != ""
	if err := validateControllerRestart(selection); err != nil {
		return admittedController{}, err
	}
	if err := execution.SaveSelection(selection); err != nil {
		return admittedController{}, err
	}
	slog.Info("runtime image bound", "phase", "controller", "image", ref.Image, "platform", ref.Platform, "controllerBuild", ref.Controller.BuildID, "imageBuildID", ref.ImageBuildID)
	return admittedController{descriptor: installed.descriptor, selection: selection, resources: resources, environment: vars}, nil
}

// A same-Pod process restart may reuse its private configuration only for the
// exact controller and runtime selection already admitted in that Pod.
func validateControllerRestart(selection execution.Selection) error {
	if _, err := os.Lstat(wire.SelectionPath); err == nil {
		previous, err := execution.LoadSelection()
		if err != nil {
			return err
		}
		if previous.Controller != selection.Controller || previous.Namespace != selection.Namespace || previous.OwnerID != selection.OwnerID || !previous.RuntimeRef.Equal(selection.RuntimeRef) {
			return errors.New("restarted controller differs from its persisted execution selection")
		}
		return nil
	} else if os.IsNotExist(err) {
		return nil
	} else {
		return err
	}
}
