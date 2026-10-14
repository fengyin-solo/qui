// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package update

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Masterminds/semver/v3"
	"github.com/creativeprojects/go-selfupdate"
)

const releaseChecksumsAsset = "checksums.txt"

const (
	defaultMaxBackups = 3
	// progressInterval keeps CLI output quiet on fast connections while
	// still giving responsive feedback on slow ones.
	progressInterval = 100_000_000 // 100ms in nanoseconds
)

// Config configures the staged updater.
type Config struct {
	// Repository is the GitHub "owner/repo" slug to fetch releases from.
	Repository string
	// Version is the version of the currently running binary.
	Version string

	// BackupDir holds persistent copies of the previous binary. Empty
	// defaults to a ".qui-backups" directory next to the executable.
	BackupDir string
	// MaxBackups retains at most this many old binaries. Zero uses the default.
	MaxBackups int

	// StageDir is where a candidate is downloaded and verified before it
	// touches the running binary. Empty uses the OS temp directory.
	StageDir string

	// DryRun downloads, verifies and runs all preflight checks without
	// backing up or replacing the binary.
	DryRun bool
}

// Progress reports download progress for a release asset.
type Progress struct {
	Asset     string
	BytesRead int64
	TotalSize int64 // zero when the source does not report a size
}

// ProgressFunc is called while the candidate archive streams to disk. It may
// be nil. Callbacks must not block on the download goroutine.
type ProgressFunc func(Progress)

// Result summarizes an update run.
type Result struct {
	// UpToDate is true when the running version already matches the latest release.
	UpToDate bool
	// DryRun is true when no replacement was attempted.
	DryRun bool
	// Updated is true after the binary was replaced and verified.
	Updated bool
	// Report is the preflight decision record for the candidate.
	Report PreflightReport
}

// Updater performs a staged self-update: download and verify in an isolated
// temp directory, back up the running binary, atomically swap in the
// candidate, and restore the original binary on any failure.
type Updater struct {
	config Config

	// Hooks default to real implementations and are overridden in tests.
	newProvider     func() (releaseProvider, error)
	signingCert     []byte
	executablePath  func() (string, error)
	freeBytes       func(path string) (int64, error)
	runBinary       func(ctx context.Context, target string) (string, error)
	renameFile      func(oldpath, newpath string) error
	copyFile        func(src, dst string, mode os.FileMode) error
	removeOrHideOld func(path string) error
}

// NewUpdater creates an Updater that updates Repository from the given Version.
func NewUpdater(config Config) *Updater {
	u := &Updater{
		config:          config,
		signingCert:     releaseSigningCertificate,
		executablePath:  selfupdate.ExecutablePath,
		freeBytes:       diskFreeBytes,
		runBinary:       execBinaryVersion,
		renameFile:      os.Rename,
		copyFile:        copyBinary,
		removeOrHideOld: removeOrHide,
	}
	u.newProvider = func() (releaseProvider, error) {
		validator, err := newReleaseValidator(u.signingCert)
		if err != nil {
			return nil, err
		}
		return newGitHubProvider(config.Repository, config.Version, validator)
	}
	return u
}

// Run executes the staged update flow. onProgress may be nil. A cancelled
// context aborts the download before the running binary is touched.
func (u *Updater) Run(ctx context.Context, onProgress ProgressFunc) (*Result, error) {
	if _, err := semver.NewVersion(u.config.Version); err != nil {
		return nil, fmt.Errorf("could not parse version: %w", err)
	}

	target, err := u.executablePath()
	if err != nil {
		return nil, fmt.Errorf("could not locate executable path: %w", err)
	}

	// A previous run may have been killed between the swap renames; make
	// sure the target binary is present before doing anything else.
	if err := u.recoverInterrupted(target); err != nil {
		return nil, fmt.Errorf("could not recover interrupted update: %w", err)
	}

	provider, err := u.newProvider()
	if err != nil {
		return nil, err
	}

	cand, found, err := provider.latest(ctx)
	if err != nil {
		return nil, fmt.Errorf("error occurred while detecting version: %w", err)
	}
	if !found {
		return nil, fmt.Errorf("latest version for %s/%s could not be found from github repository", u.config.Repository, u.config.Version)
	}
	if cand == nil {
		return &Result{UpToDate: true}, nil
	}

	validator, err := newReleaseValidator(u.signingCert)
	if err != nil {
		return nil, err
	}

	// Stage: isolated temp directory, removed on every exit path so a
	// corrupt or partial candidate can never linger.
	stage, err := u.stageCandidate(ctx, provider, validator, cand, onProgress)
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(stage.dir)

	backupDir := u.config.BackupDir
	if backupDir == "" {
		backupDir = filepath.Join(filepath.Dir(target), ".qui-backups")
	}

	report, err := u.preflight(target, backupDir, cand, stage)
	if err != nil {
		return nil, err
	}

	if u.config.DryRun {
		return &Result{DryRun: true, Report: report}, nil
	}

	backupPath, err := u.backupCurrent(target, backupDir)
	if err != nil {
		return nil, fmt.Errorf("could not back up current binary: %w", err)
	}

	if err := u.apply(ctx, target, stage.candidatePath, backupPath, report); err != nil {
		return nil, err
	}

	u.pruneBackups(backupDir, filepath.Base(target), u.maxBackups())

	return &Result{Updated: true, Report: report}, nil
}

func (u *Updater) maxBackups() int {
	if u.config.MaxBackups > 0 {
		return u.config.MaxBackups
	}
	return defaultMaxBackups
}

// ErrInsufficientSpace is returned when the target or backup filesystem
// cannot hold the staged update.
var ErrInsufficientSpace = errors.New("insufficient disk space")

// ErrNotWritable is returned when a directory needed for the update is not writable.
var ErrNotWritable = errors.New("directory is not writable")
