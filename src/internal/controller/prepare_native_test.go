package controller

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

func TestTurnPreparationBudgetIncludesMandatoryPromptAndPrivateMetadata(t *testing.T) {
	g := workspace.TaskGrant{WorkerSessionID: uuid.NewString(), TurnSequence: 1, CreatedAt: time.Now().UTC()}
	p := workspace.Prepared{NativeMetadata: &workspace.NativeMetadata{WorkerSessionID: g.WorkerSessionID, TurnSequence: g.TurnSequence}}
	run := wire.Run{Prompt: "current brief and task", NativeMetadata: p.NativeMetadata}
	if !turnPreparationFits(wire.Bootstrap{}, g, p, run) {
		t.Fatal("bounded current execution was refused")
	}
	run.Prompt = strings.Repeat("x", wire.MaxAssignmentBytes)
	if turnPreparationFits(wire.Bootstrap{}, g, p, run) {
		t.Fatal("mandatory prompt escaped the aggregate assignment bound")
	}
	run.Prompt = "current task"
	run.NativeMetadata.CodexConfig = make([]byte, wire.MaxAssignmentBytes)
	if turnPreparationFits(wire.Bootstrap{}, g, p, run) {
		t.Fatal("mandatory private native configuration escaped the aggregate assignment bound")
	}
}
