// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package update

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// PreflightReport is the decision record produced before a binary is replaced.
type PreflightReport struct {
	TargetPath       string
	BackupDir        string
	CurrentVersion   string
	CandidateVersion string
	CurrentSize      int64
	CandidateSize    int64
	CurrentSHA256    string
	CandidateSHA256  string
	FreeTargetBytes  int64
	FreeBackupBytes  int64
	CheckedAt        time.Time
}

// preflight hashes both binaries and confirms the target and backup
// filesystems have enough free space and are writable.
func (u *Updater) preflight(target, backupDir string, cand *candidate, stage *stagedUpdate) (PreflightReport, error) {
	report := PreflightReport{
		TargetPath:       target,
		BackupDir:        backupDir,
		CurrentVersion:   u.config.Version,
		CandidateVersion: cand.version,
		CandidateSize:    stage.candidateSize,
		CandidateSHA256:  stage.candidateHash,
		CheckedAt:        time.Now().UTC(),
	}

	currentStat, err := os.Stat(target)
	if err != nil {
		return report, fmt.Errorf("could not stat current binary: %w", err)
	}
	report.CurrentSize = currentStat.Size()

	report.CurrentSHA256, _, err = hashFile(target)
	if err != nil {
		return report, fmt.Errorf("could not hash current binary: %w", err)
	}

	targetDir := filepath.Dir(target)
	report.FreeTargetBytes, err = u.freeBytes(targetDir)
	if err != nil {
		return report, fmt.Errorf("could not determine free disk space for %s: %w", targetDir, err)
	}
	report.FreeBackupBytes, err = u.freeBytes(backupDir)
	if err != nil {
		// The backup directory may not exist yet; resolve it against its
		// existing parent so the space check still works.
		if report.FreeBackupBytes, err = u.freeBytes(existingParent(backupDir)); err != nil {
			return report, fmt.Errorf("could not determine free disk space for %s: %w", backupDir, err)
		}
	}

	if err := ensureSpace(report.FreeTargetBytes, report.FreeBackupBytes, report.CandidateSize, report.CurrentSize); err != nil {
		return report, err
	}

	if err := probeWritable(targetDir); err != nil {
		return report, fmt.Errorf("target directory %s: %w", targetDir, err)
	}
	if err := os.MkdirAll(backupDir, 0o700); err != nil {
		return report, fmt.Errorf("could not create backup directory %s: %w", backupDir, err)
	}
	if err := probeWritable(backupDir); err != nil {
		return report, fmt.Errorf("backup directory %s: %w", backupDir, err)
	}

	return report, nil
}

// ensureSpace verifies the target filesystem can hold the new binary while
// the old one is still in place, and the backup filesystem can hold the copy.
func ensureSpace(freeTarget, freeBackup, candidateSize, currentSize int64) error {
	if freeTarget < candidateSize {
		return fmt.Errorf("%w: %s available in target directory, need %s for candidate",
			ErrInsufficientSpace, formatBytes(freeTarget), formatBytes(candidateSize))
	}
	if freeBackup < currentSize {
		return fmt.Errorf("%w: %s available in backup directory, need %s for current binary",
			ErrInsufficientSpace, formatBytes(freeBackup), formatBytes(currentSize))
	}
	return nil
}

// probeWritable verifies a directory accepts new files without leaving one behind.
func probeWritable(dir string) error {
	probe, err := os.CreateTemp(dir, ".qui-write-probe-*")
	if err != nil {
		return fmt.Errorf("%w: %w", ErrNotWritable, err)
	}
	name := probe.Name()
	_ = probe.Close()
	_ = os.Remove(name)
	return nil
}

func existingParent(dir string) string {
	for {
		parent := filepath.Dir(dir)
		if parent == dir {
			return dir
		}
		if _, err := os.Stat(parent); err == nil {
			return parent
		}
		dir = parent
	}
}

// hashFile returns the lowercase hex SHA-256 and size of path.
func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()

	hasher := sha256.New()
	size, err := io.Copy(hasher, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(hasher.Sum(nil)), size, nil
}

func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
