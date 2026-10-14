// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

//go:build windows

package update

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

// diskFreeBytes returns the bytes available to the current user on the
// filesystem containing path. GetDiskFreeSpaceEx requires a directory.
func diskFreeBytes(path string) (int64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if !fi.IsDir() {
		path = filepath.Dir(path)
	}

	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, fmt.Errorf("invalid path %s: %w", path, err)
	}

	var freeBytesAvailable, totalBytes, totalFreeBytes uint64
	if err := windows.GetDiskFreeSpaceEx(pathPtr, &freeBytesAvailable, &totalBytes, &totalFreeBytes); err != nil {
		return 0, fmt.Errorf("GetDiskFreeSpaceEx %s: %w", path, err)
	}
	//nolint:gosec // uint64 to int64: disk sizes do not exceed int64 max.
	return int64(freeBytesAvailable), nil
}

// removeOrHide removes the swapped-out binary. Windows cannot delete the file
// while the old process is still running, so hide it instead; it gets removed
// on the next update.
func removeOrHide(path string) error {
	if err := os.Remove(path); err == nil {
		return nil
	}

	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	// FILE_ATTRIBUTE_HIDDEN | FILE_ATTRIBUTE_SYSTEM keeps the leftover out
	// of directory listings.
	return windows.SetFileAttributes(pathPtr, windows.FILE_ATTRIBUTE_HIDDEN|windows.FILE_ATTRIBUTE_SYSTEM)
}
