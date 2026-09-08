package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

func streamBytes(label string) []byte {
	return bytes.Repeat(append([]byte(label+" fixture bytes\n"), 0, 255), 80000)
}
func streamDigest(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func streamFrame(reader io.Reader) ([]byte, error) {
	var size uint32
	if err := binary.Read(reader, binary.BigEndian, &size); err != nil {
		return nil, err
	}
	if size > 4<<20 {
		return nil, errors.New("oversized fixture frame")
	}
	raw := make([]byte, size)
	_, err := io.ReadFull(reader, raw)
	return raw, err
}
func writeFrame(writer io.Writer, raw []byte) error {
	if err := binary.Write(writer, binary.BigEndian, uint32(len(raw))); err != nil {
		return err
	}
	_, err := writer.Write(raw)
	return err
}

func streamProvider() int {
	first, err := streamFrame(os.Stdin)
	if err != nil {
		return 95
	}
	if _, err := fmt.Fprintf(os.Stdout, "READY %s\n", streamDigest(first)); err != nil {
		return 96
	}
	if _, err := os.Stdout.Write(streamBytes("stdout")); err != nil {
		return 97
	}
	if _, err := os.Stderr.Write(streamBytes("stderr")); err != nil {
		return 98
	}
	second, err := streamFrame(os.Stdin)
	if err != nil {
		return 99
	}
	var extra [1]byte
	n, err := os.Stdin.Read(extra[:])
	if n != 0 || err != io.EOF {
		return 100
	}
	if _, err := fmt.Fprintf(os.Stdout, "DONE %s\n", streamDigest(second)); err != nil {
		return 101
	}
	return 0
}

type streamEvidence struct {
	StdoutBytes          int    `json:"stdoutBytes"`
	StderrBytes          int    `json:"stderrBytes"`
	StdoutSHA256         string `json:"stdoutSHA256"`
	StderrSHA256         string `json:"stderrSHA256"`
	FirstReplyBeforeEOF  bool   `json:"firstReplyBeforeEOF"`
	ExitCode             int    `json:"exitCode"`
	ClosedOutputExitCode int    `json:"closedOutputExitCode"`
}

func (v *verifier) streams(parent context.Context, env []string) error {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, fixtureShim, "--duplex-stream")
	command.Env = env
	command.Dir = "/tmp"
	stdin, err := command.StdinPipe()
	if err != nil {
		return err
	}
	defer stdin.Close()
	stdout, err := command.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := command.StderrPipe()
	if err != nil {
		return err
	}
	if err := command.Start(); err != nil {
		return err
	}
	first := bytes.Repeat([]byte("first binary input\x00\xff"), 1024)
	second := bytes.Repeat([]byte("second binary input\x00\xfe"), 2048)
	firstReply := "READY " + streamDigest(first) + "\n"
	lastReply := "DONE " + streamDigest(second) + "\n"
	acknowledged := make(chan error, 1)
	outDone := make(chan error, 1)
	errDone := make(chan error, 1)
	var out, diagnostics bytes.Buffer
	go func() {
		reader := bufio.NewReader(stdout)
		line, err := reader.ReadString('\n')
		out.WriteString(line)
		if err == nil && line != firstReply {
			err = errors.New("first duplex response did not acknowledge the first input frame")
		}
		acknowledged <- err
		if err != nil {
			outDone <- err
			return
		}
		_, err = io.Copy(&out, reader)
		outDone <- err
	}()
	go func() { _, err := io.Copy(&diagnostics, stderr); errDone <- err }()
	if err := writeFrame(stdin, first); err != nil {
		cancel()
		_ = command.Wait()
		return err
	}
	select {
	case err := <-acknowledged:
		if err != nil {
			cancel()
			_ = command.Wait()
			return err
		}
	case <-ctx.Done():
		_ = command.Wait()
		return errors.New("provider required stdin EOF before replying to the first frame")
	}
	// The first reply has been consumed while the input pipe is still open.
	if err := writeFrame(stdin, second); err != nil {
		cancel()
		_ = command.Wait()
		return err
	}
	if err := stdin.Close(); err != nil {
		cancel()
		_ = command.Wait()
		return err
	}
	outErr, stderrErr := <-outDone, <-errDone
	waitErr := command.Wait()
	if ctx.Err() != nil {
		return errors.New("duplex streams or stdin EOF did not terminate the provider")
	}
	if outErr != nil || stderrErr != nil || waitErr != nil {
		return errors.Join(outErr, stderrErr, waitErr)
	}
	expectedOut := append(append([]byte(firstReply), streamBytes("stdout")...), []byte(lastReply)...)
	expectedErr := streamBytes("stderr")
	if out.Len() <= 1<<20 || diagnostics.Len() <= 1<<20 || streamDigest(out.Bytes()) != streamDigest(expectedOut) || streamDigest(diagnostics.Bytes()) != streamDigest(expectedErr) {
		return errors.New("large stdout/stderr bytes changed in the adapter process relay")
	}
	closedCode, err := closedOutput(parent, env, first, firstReply)
	if err != nil {
		return err
	}
	evidence := streamEvidence{StdoutBytes: out.Len(), StderrBytes: diagnostics.Len(), StdoutSHA256: streamDigest(out.Bytes()), StderrSHA256: streamDigest(diagnostics.Bytes()), FirstReplyBeforeEOF: true, ExitCode: 0, ClosedOutputExitCode: closedCode}
	raw, _ := json.MarshalIndent(evidence, "", "  ")
	if err := os.WriteFile(filepath.Join(v.evidence, "streams.json"), append(raw, '\n'), 0600); err != nil {
		return err
	}
	v.results = append(v.results, "large-duplex-streams-eof", "closed-output-not-success")
	return nil
}

func closedOutput(parent context.Context, env []string, first []byte, expected string) (int, error) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, fixtureShim, "--duplex-stream")
	command.Env = env
	command.Dir = "/tmp"
	command.Stderr = io.Discard
	stdin, err := command.StdinPipe()
	if err != nil {
		return 0, err
	}
	defer stdin.Close()
	stdout, err := command.StdoutPipe()
	if err != nil {
		return 0, err
	}
	if err := command.Start(); err != nil {
		return 0, err
	}
	if err := writeFrame(stdin, first); err != nil {
		cancel()
		_ = command.Wait()
		return 0, err
	}
	acknowledged := make(chan error, 1)
	go func() {
		line, err := bufio.NewReader(stdout).ReadString('\n')
		if err == nil && line != expected {
			err = errors.New("closed-output scenario did not enter the real streaming provider")
		}
		acknowledged <- err
	}()
	select {
	case err := <-acknowledged:
		if err != nil {
			cancel()
			_ = command.Wait()
			return 0, err
		}
	case <-ctx.Done():
		_ = command.Wait()
		return 0, ctx.Err()
	}
	if err := stdout.Close(); err != nil {
		cancel()
		_ = command.Wait()
		return 0, err
	}
	// Deliberately keep upstream stdin open. Provider/output failure must finish
	// without waiting for an input EOF that the upstream sender has not supplied.
	err = command.Wait()
	if ctx.Err() != nil {
		return 0, errors.New("output interruption left the runtime waiting for upstream stdin EOF")
	}
	if err == nil {
		return 0, errors.New("closed output transport was converted to a successful provider exit")
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		return 0, err
	}
	return exit.ExitCode(), nil
}
