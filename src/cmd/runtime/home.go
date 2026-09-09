package main

import (
	"context"
	"errors"
	"flag"
	"os"

	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/execution"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
)

type configCopies []configuration.Copy

func (c *configCopies) String() string { return "sourceGroup/source/target JSON" }
func (c *configCopies) Set(raw string) error {
	var copy configuration.Copy
	if err := runtimeimage.Decode([]byte(raw), &copy); err != nil {
		return errors.New("home configuration copy requires sourceGroup/source/target JSON")
	}
	*c = append(*c, copy)
	return nil
}

func layoutHome(ctx context.Context, arguments []string) error {
	flags := flag.NewFlagSet("home layout", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var options execution.HomeOptions
	flags.StringVar(&options.PrivateRoot, "private-root", "", "whole Pod-private volume preparation path")
	flags.StringVar(&options.RequestPath, "request", "", "worker's read-only request input")
	var copies configCopies
	flags.Var(&copies, "config-copy", "sourceGroup/source/target JSON")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("home layout requires --private-root canonical directory")
	}
	options.Copies = copies
	return execution.LayoutHome(ctx, options)
}
