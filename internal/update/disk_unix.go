// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

//go:build !windows

package update

import (
	"os"

	"golang.org/x/sys/unix"
)

// diskFreeBytes returns the bytes available to unprivileged users on the
// filesystem containing path.
func diskFreeBytes(path string) (int64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, err
	}
	//nolint:unconvert // the int64 casts are required on macOS (uint32 fields).
	return int64(stat.Bavail) * int64(stat.Bsize), nil
}

// removeOrHide removes the old binary. Unix allows removing the swapped-out
// file immediately.
func removeOrHide(path string) error {
	return os.Remove(path)
}
