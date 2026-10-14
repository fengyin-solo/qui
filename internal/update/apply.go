// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package update

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/Masterminds/semver/v3"
)

// RollbackError reports both the failure that triggered a rollback and the
// error that prevented the original binary from being restored. When this is
// returned, the executable is missing or damaged and must be restored
// manually from the persistent backup.
type RollbackError struct {
	Err         error
	RollbackErr error
}

func (e *RollbackError) Error() string {
	return fmt.Sprintf("%v (rollback also failed: %v)", e.Err, e.RollbackErr)
}

func (e *RollbackError) Unwrap() error { return e.Err }

// apply stages the verified candidate next to the target, swaps the files
// atomically, and verifies the replaced file's hash and runtime version. Any
// failure after the swap restores the original binary, first via rename and
// then via the persistent backup copy.
func (u *Updater) apply(ctx context.Context, target, candidatePath, backupPath string, report PreflightReport) error {
	dir := filepath.Dir(target)
	base := filepath.Base(target)
	newPath := filepath.Join(dir, "."+base+".new")
	oldPath := filepath.Join(dir, "."+base+".old")

	mode := os.FileMode(0o755)
	if stat, err := os.Stat(target); err == nil {
		mode = stat.Mode()
	}

	if err := copyBinary(candidatePath, newPath, mode); err != nil {
		_ = os.Remove(newPath)
		return fmt.Errorf("could not stage new binary in %s: %w", dir, err)
	}

	// Re-hash the file copied next to the target before anything is renamed.
	stagedHash, _, err := hashFile(newPath)
	if err != nil || stagedHash != report.CandidateSHA256 {
		_ = os.Remove(newPath)
		if err != nil {
			return fmt.Errorf("could not verify staged binary: %w", err)
		}
		return fmt.Errorf("staged binary hash %s does not match verified candidate %s", stagedHash, report.CandidateSHA256)
	}

	_ = os.Remove(oldPath)
	if err := u.renameFile(target, oldPath); err != nil {
		_ = os.Remove(newPath)
		return fmt.Errorf("could not move current binary aside: %w", err)
	}

	if err := u.renameFile(newPath, target); err != nil {
		if rerr := u.renameFile(oldPath, target); rerr != nil {
			if cerr := u.copyFile(backupPath, target, mode); cerr != nil {
				return &RollbackError{Err: err, RollbackErr: cerr}
			}
		}
		_ = os.Remove(newPath)
		_ = u.removeOrHideOld(oldPath)
		return fmt.Errorf("could not replace binary, original restored: %w", err)
	}

	if err := u.verifyReplacedBinary(ctx, target, oldPath, backupPath, mode, report); err != nil {
		return err
	}

	_ = u.removeOrHideOld(oldPath)
	return nil
}

// verifyReplacedBinary checks the replaced file in place and restores the
// previous binary when the hash or the reported runtime version is wrong.
func (u *Updater) verifyReplacedBinary(ctx context.Context, target, oldPath, backupPath string, mode os.FileMode, report PreflightReport) error {
	postHash, _, err := hashFile(target)
	if err != nil || postHash != report.CandidateSHA256 {
		if rerr := u.restorePrevious(target, oldPath, backupPath, mode); rerr != nil {
			return &RollbackError{Err: errors.New("replaced binary failed hash verification"), RollbackErr: rerr}
		}
		if err != nil {
			return fmt.Errorf("could not read replaced binary, original restored: %w", err)
		}
		return fmt.Errorf("replaced binary hash %s does not match %s, original restored", postHash, report.CandidateSHA256)
	}

	runningVersion, err := u.runBinary(ctx, target)
	if err != nil {
		if rerr := u.restorePrevious(target, oldPath, backupPath, mode); rerr != nil {
			return &RollbackError{Err: fmt.Errorf("replaced binary failed to run: %w", err), RollbackErr: rerr}
		}
		return fmt.Errorf("replaced binary failed to run, original restored: %w", err)
	}
	if !versionsEqual(runningVersion, report.CandidateVersion) {
		if rerr := u.restorePrevious(target, oldPath, backupPath, mode); rerr != nil {
			return &RollbackError{Err: fmt.Errorf("replaced binary reported version %q, want %q", runningVersion, report.CandidateVersion), RollbackErr: rerr}
		}
		return fmt.Errorf("replaced binary reported version %q, want %q; original restored", runningVersion, report.CandidateVersion)
	}
	return nil
}

// restorePrevious puts the saved original binary back when the target already
// holds the new (bad) binary.
func (u *Updater) restorePrevious(target, oldPath, backupPath string, mode os.FileMode) error {
	_ = os.Remove(target)
	if _, err := os.Stat(oldPath); err == nil {
		if err := u.renameFile(oldPath, target); err == nil {
			return nil
		}
	}
	return u.copyFile(backupPath, target, mode)
}

// recoverInterrupted repairs a swap that was interrupted by a process kill:
// the target is gone but its renamed-aside copy remains. With a healthy
// target, stale swap artifacts are removed.
func (u *Updater) recoverInterrupted(target string) error {
	dir := filepath.Dir(target)
	base := filepath.Base(target)
	newPath := filepath.Join(dir, "."+base+".new")
	oldPath := filepath.Join(dir, "."+base+".old")

	if _, err := os.Stat(target); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if _, statErr := os.Stat(oldPath); statErr == nil {
			if renameErr := u.renameFile(oldPath, target); renameErr != nil {
				return renameErr
			}
			_ = os.Remove(newPath)
		}
		return nil
	}

	_ = os.Remove(newPath)
	_ = u.removeOrHideOld(oldPath)
	return nil
}

// versionsEqual compares two version strings semantically, tolerating a
// leading "v" and surrounding whitespace.
func versionsEqual(running, candidate string) bool {
	running = strings.TrimSpace(running)
	candidate = strings.TrimSpace(candidate)
	if running == candidate {
		return true
	}
	r, err1 := semver.NewVersion(running)
	c, err2 := semver.NewVersion(candidate)
	if err1 != nil || err2 != nil {
		return false
	}
	return r.String() == c.String()
}
