package runtimeimage

import (
	"encoding/json"
	"errors"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"os"
	"path/filepath"
)

const ReceiptName = "image-receipt.json"

type Receipt struct {
	SchemaVersion    int    `json:"schemaVersion"`
	ImageBuildID     string `json:"imageBuildID"`
	DescriptorDigest string `json:"descriptorDigest"`
	Platform         string `json:"platform"`
}

func (d Descriptor) Receipt(digest string) Receipt {
	return Receipt{SchemaVersion: 1, ImageBuildID: d.ImageBuildID, DescriptorDigest: digest, Platform: d.Platform}
}

func CheckReceipt(run string, d Descriptor, digest string) error {
	var got Receipt
	if _, err := ReadJSON(filepath.Join(run, ReceiptName), &got); err != nil {
		return err
	}
	if got != d.Receipt(digest) {
		return diagnostics.Wrap("runtime_image_receipt_mismatch", errors.New("private volume image receipt differs from current rootfs"))
	}
	return nil
}

// PublishReceipt never replaces an existing receipt, including across a
// container restart. The link commits the complete file without an overwrite.
// This receipt shares the Pod-private lifetime of the HOME it admits.
func PublishReceipt(run string, d Descriptor, digest string) error {
	path := filepath.Join(run, ReceiptName)
	if _, err := os.Lstat(path); err == nil {
		return CheckReceipt(run, d, digest)
	} else if !os.IsNotExist(err) {
		return err
	}
	raw, err := json.Marshal(d.Receipt(digest))
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(run, ".receipt-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(raw)
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Link(f.Name(), path); err != nil && !os.IsExist(err) {
		return err
	}
	return CheckReceipt(run, d, digest)
}
