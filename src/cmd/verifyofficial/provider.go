package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
)

const eventFile = "/tmp/verifyofficial-provider-events.jsonl"
const failVersionFile = "/tmp/verifyofficial-version-fails"

type providerEvent struct {
	Mode          string   `json:"mode"`
	Args          []string `json:"args"`
	Home          string   `json:"home"`
	TaskID        string   `json:"taskID"`
	VersionFailed bool     `json:"versionFailed,omitempty"`
}

func providerHelper(mode string, args []string) int {
	event := providerEvent{Mode: mode, Args: args, Home: os.Getenv("HOME"), TaskID: os.Getenv("MULTICA_TASK_ID")}
	if slices.Contains(args, "--version") {
		_, err := os.Stat(failVersionFile)
		event.VersionFailed = err == nil
	}
	data, _ := json.Marshal(event)
	file, err := os.OpenFile(eventFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return 91
	}
	_, err = file.Write(append(data, '\n'))
	file.Close()
	if err != nil {
		return 92
	}
	if mode == "forbidden" {
		return 93
	}
	if slices.Contains(args, "--duplex-stream") {
		return streamProvider()
	}
	if slices.Contains(args, "--version") {
		if event.VersionFailed {
			return 23
		}
		fmt.Println("0.85.0")
		return 0
	}
	if slices.Contains(args, "--list-models") {
		fmt.Print("provider  model  context  max-out  reasoning  images\nfixture  fixture-model  200000  8192  yes  yes\n")
		return 0
	}
	if len(args) > 0 && args[0] == "--echo-protocol" {
		body, _ := io.ReadAll(io.LimitReader(os.Stdin, 4<<20))
		out, _ := json.Marshal(struct {
			Args  []string
			Input []byte
			Home  string
		}{args[1:], body, os.Getenv("HOME")})
		os.Stdout.Write(out)
		fmt.Fprint(os.Stderr, "fixture stderr bytes\n")
		return 17
	}
	// No real model/provider task is allowed in this discovery-only fixture.
	return 94
}
