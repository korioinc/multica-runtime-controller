package main

import (
	"testing"
	"time"

	"github.com/google/uuid"
	control "github.com/korioinc/multica-runtime-controller/internal/controller"
)

func TestConversationPolicyOptions(t *testing.T) {
	owner := uuid.NewString()
	t.Setenv("MULTICA_OWNER_ID", owner)
	t.Setenv("MULTICA_DAEMON_ID", owner)
	for _, name := range []string{"MULTICA_STARTUP_TIMEOUT", "MULTICA_RUNTIME_CAPACITY", "MULTICA_POLL_INTERVAL", "MULTICA_HEARTBEAT_INTERVAL"} {
		t.Setenv(name, "")
	}
	for _, test := range []struct {
		name, timeout, resident string
		wantTimeout             time.Duration
		wantResident            int
		invalid                 bool
	}{
		{name: "default", wantTimeout: control.DefaultConversationIdleTimeout},
		{name: "disabled", timeout: "0", resident: "0"},
		{name: "explicit", timeout: "2m30s", resident: "3", wantTimeout: 150 * time.Second, wantResident: 3},
		{name: "negative timeout", timeout: "-1ns", invalid: true},
		{name: "malformed timeout", timeout: "ten minutes", invalid: true},
		{name: "overflow timeout", timeout: "1000000000h", invalid: true},
		{name: "negative ceiling", resident: "-1", invalid: true},
		{name: "fractional ceiling", resident: "1.5", invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("MULTICA_CONVERSATION_IDLE_TIMEOUT", test.timeout)
			t.Setenv("MULTICA_MAX_RESIDENT_PODS", test.resident)
			got, err := loadControllerOptions()
			if test.invalid {
				if err == nil {
					t.Fatal("invalid conversation policy was accepted")
				}
				return
			}
			if err != nil || got.conversationIdleTimeout != test.wantTimeout || got.maxResidentPods != test.wantResident {
				t.Fatalf("policy = (%s, %d), error = %v", got.conversationIdleTimeout, got.maxResidentPods, err)
			}
		})
	}
}
