package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"golang.org/x/sys/unix"
)

func captureProviderHome(ctx context.Context, backend string, request wire.Request, raw []byte) error {
	root, err := wire.StorageRoot(request)
	if err != nil {
		return err
	}
	path := filepath.Join(root, ".runtime-home", request.AttemptID+".tar")
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || canonical != path {
		return errors.New("provider HOME capture source is not canonical")
	}
	file, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("provider HOME capture requires its regular producer archive")
	}
	var envelope bytes.Buffer
	form := multipart.NewWriter(&envelope)
	metadata, err := form.CreateFormField("request")
	if err != nil {
		return err
	}
	if _, err := metadata.Write(raw); err != nil {
		return err
	}
	if _, err := form.CreateFormFile("archive", "task-home.tar"); err != nil {
		return err
	}
	prefix := append([]byte{}, envelope.Bytes()...)
	envelope.Reset()
	if err := form.Close(); err != nil {
		return err
	}
	suffix := envelope.Bytes()
	body := io.MultiReader(bytes.NewReader(prefix), file, bytes.NewReader(suffix))
	upload, err := http.NewRequestWithContext(ctx, http.MethodPost, backend+"/fixture/home", body)
	if err != nil {
		return err
	}
	upload.Header.Set("Content-Type", form.FormDataContentType())
	upload.ContentLength = int64(len(prefix)+len(suffix)) + info.Size()
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(upload)
	if err != nil {
		return err
	}
	var result struct {
		Captured bool `json:"captured"`
	}
	if err := readBody(response, &result); err != nil {
		return err
	}
	if !result.Captured {
		return errors.New("fixture backend did not commit the HOME capture")
	}
	return nil
}
