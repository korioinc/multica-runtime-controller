package main

import (
	"errors"
	"os"
	"strconv"
	"time"

	control "github.com/korioinc/multica-runtime-controller/internal/controller"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

type controllerOptions struct {
	workerConfigFile        string
	ownerID                 string
	namespace               string
	podName                 string
	podUID                  string
	containerName           string
	nodeName                string
	startupTimeout          time.Duration
	backendURL              string
	gatewayURL              string
	tokenFile               string
	name                    string
	capacity                int
	conversationIdleTimeout time.Duration
	maxResidentPods         int
	pollInterval            time.Duration
	heartbeatInterval       time.Duration
	environment             []string
}

func loadControllerOptions() (controllerOptions, error) {
	options := controllerOptions{
		workerConfigFile: value("MULTICA_WORKER_CONFIG_FILE", wire.WorkerConfigPath),
		ownerID:          value("MULTICA_OWNER_ID", os.Getenv("MULTICA_DAEMON_ID")),
		namespace:        os.Getenv("POD_NAMESPACE"),
		podName:          os.Getenv("POD_NAME"),
		podUID:           os.Getenv("POD_UID"),
		containerName:    value("POD_CONTAINER_NAME", "controller"),
		nodeName:         os.Getenv("POD_NODE_NAME"),
		backendURL:       os.Getenv("MULTICA_BASE_URL"),
		gatewayURL:       os.Getenv("MULTICA_GATEWAY_URL"),
		tokenFile:        os.Getenv("MULTICA_CONTROLLER_TOKEN_FILE"),
		name:             value("MULTICA_RUNTIME_NAME", "runtime-controller"),
		environment:      os.Environ(),
	}
	if options.ownerID != os.Getenv("MULTICA_DAEMON_ID") || !wire.UUID(options.ownerID) {
		return controllerOptions{}, errors.New("installation owner differs from daemon identity")
	}
	var err error
	options.startupTimeout, err = time.ParseDuration(value("MULTICA_STARTUP_TIMEOUT", "120s"))
	if err != nil || options.startupTimeout <= 0 {
		return controllerOptions{}, errors.New("startup timeout must be a positive duration")
	}
	options.capacity, err = strconv.Atoi(value("MULTICA_RUNTIME_CAPACITY", "20"))
	if err != nil || options.capacity < 1 {
		return controllerOptions{}, errors.New("invalid runtime capacity")
	}
	options.conversationIdleTimeout, err = time.ParseDuration(value("MULTICA_CONVERSATION_IDLE_TIMEOUT", control.DefaultConversationIdleTimeout.String()))
	if err != nil || options.conversationIdleTimeout < 0 {
		return controllerOptions{}, errors.New("conversation idle timeout must be a nonnegative duration")
	}
	options.maxResidentPods, err = strconv.Atoi(value("MULTICA_MAX_RESIDENT_PODS", "0"))
	if err != nil || options.maxResidentPods < 0 {
		return controllerOptions{}, errors.New("resident Pod limit must be nonnegative")
	}
	options.pollInterval, err = time.ParseDuration(value("MULTICA_POLL_INTERVAL", "10s"))
	if err != nil || options.pollInterval <= 0 {
		return controllerOptions{}, errors.New("invalid poll interval")
	}
	options.heartbeatInterval, err = time.ParseDuration(value("MULTICA_HEARTBEAT_INTERVAL", "15s"))
	if err != nil || options.heartbeatInterval <= 0 {
		return controllerOptions{}, errors.New("invalid heartbeat interval")
	}
	return options, nil
}

func value(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
