package githubauth

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Credential implements Git's credential helper protocol. Credentials are
// issued only for one HTTPS github.com repository, never for an entire host.
func Credential(ctx context.Context, operation string, input io.Reader, output io.Writer) error {
	switch operation {
	case "store", "erase":
		return nil
	case "get":
	default:
		return errors.New("unsupported Git credential operation")
	}
	repository, err := credentialRepository(input)
	if err != nil {
		return err
	}
	token, err := RequestToken(ctx, repository)
	if err != nil {
		return err
	}
	// This stdout is consumed by Git. No credential is written to an error or
	// persisted by the helper; each invocation obtains its current scoped token.
	if _, err := fmt.Fprintf(output, "username=x-access-token\npassword=%s\n\n", token.Value); err != nil {
		return errors.New("Git credential output could not be written")
	}
	return nil
}

func credentialRepository(input io.Reader) (string, error) {
	const maxInput = 64 << 10
	reader := io.LimitReader(input, maxInput+1)
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1024), maxInput+1)
	// ScanLines silently removes a trailing carriage return. Preserve it so
	// every control character is checked before interpreting credential fields.
	scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		if end := bytes.IndexByte(data, '\n'); end >= 0 {
			return end + 1, data[:end], nil
		}
		if atEOF && len(data) > 0 {
			return len(data), data, nil
		}
		return 0, nil, nil
	})
	fields := make(map[string]string)
	consumed := 0
	for scanner.Scan() {
		line := scanner.Text()
		consumed += len(line) + 1
		if consumed > maxInput || strings.IndexFunc(line, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 {
			return "", errors.New("invalid Git credential input")
		}
		if line == "" {
			break
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || key == "" {
			return "", errors.New("invalid Git credential input")
		}
		if key != "protocol" && key != "host" && key != "path" {
			continue
		}
		if _, duplicate := fields[key]; duplicate {
			return "", errors.New("ambiguous Git credential target")
		}
		fields[key] = value
	}
	if scanner.Err() != nil {
		return "", errors.New("Git credential input could not be read")
	}
	if fields["protocol"] != "https" || !strings.EqualFold(fields["host"], "github.com") || fields["path"] == "" || strings.HasPrefix(fields["path"], "/") {
		return "", errors.New("Git credential target must be one HTTPS github.com repository")
	}
	repository, err := parseRepositoryHint("https://github.com/" + fields["path"])
	if err != nil {
		return "", errors.New("invalid Git credential repository")
	}
	return repository, nil
}
