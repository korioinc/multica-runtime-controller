package processgroup

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

func stopped(pgid int) (bool, error) {
	if err := syscall.Kill(-pgid, 0); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return true, nil
		}
		return false, err
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return false, err
	}
	found := false
	for _, entry := range entries {
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		raw, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "stat"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return false, err
		}
		// comm can contain spaces and parentheses. Fields after its last ')' are
		// state (3), ppid (4), pgrp (5), ... num_threads (20).
		end := strings.LastIndexByte(string(raw), ')')
		if end < 0 {
			return false, errors.New("invalid process identity")
		}
		fields := strings.Fields(string(raw[end+1:]))
		if len(fields) < 18 {
			return false, errors.New("incomplete process identity")
		}
		group, err := strconv.Atoi(fields[2])
		if err != nil {
			return false, err
		}
		if group != pgid {
			continue
		}
		found = true
		threads, err := strconv.Atoi(fields[17])
		if err != nil {
			return false, err
		}
		// Linux closes file descriptors before a process becomes a zombie.
		// A zombie leader can still have live sibling threads: only a final,
		// single-thread zombie proves that this member cannot write again.
		if fields[0] != "Z" || threads != 1 {
			return false, nil
		}
	}
	if found {
		return true, nil
	}
	// A group hidden from this proc mount is not evidence of termination.
	err = syscall.Kill(-pgid, 0)
	if errors.Is(err, syscall.ESRCH) {
		return true, nil
	}
	return false, err
}
