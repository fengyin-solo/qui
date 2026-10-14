// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package update

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const backupSuffix = ".bak"

// backupCurrent copies the running binary into backupDir before the swap. The
// copy is the last-resort restore source when rename rollback fails.
func (u *Updater) backupCurrent(target, backupDir string) (string, error) {
	mode := os.FileMode(0o755)
	if stat, err := os.Stat(target); err == nil {
		mode = stat.Mode()
	}

	name := backupFileName(filepath.Base(target), u.config.Version, time.Now().UnixNano())
	backupPath := filepath.Join(backupDir, name)
	if err := u.copyFile(target, backupPath, mode); err != nil {
		return "", err
	}
	return backupPath, nil
}

// backupFileName builds a sortable, collision-free backup name, e.g.
// qui_1.2.3_1728990123456789000.bak.
func backupFileName(targetBase, version string, unixNano int64) string {
	prefix := strings.TrimSuffix(targetBase, filepath.Ext(targetBase))
	return fmt.Sprintf("%s_%s_%d%s", prefix, sanitizeVersion(version), unixNano, backupSuffix)
}

func sanitizeVersion(version string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", " ", "_", string(filepath.Separator), "_")
	return replacer.Replace(version)
}

// pruneBackups keeps at most maxBackups copies of targetBase, removing the
// oldest first. Failures are best effort: the update already succeeded and a
// future run rotates again.
func (u *Updater) pruneBackups(backupDir, targetBase string, maxBackups int) {
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		return
	}

	prefix := strings.TrimSuffix(targetBase, filepath.Ext(targetBase)) + "_"
	type backup struct {
		path string
		mod  time.Time
	}
	var backups []backup
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, prefix) && strings.HasSuffix(name, backupSuffix) {
			info, err := entry.Info()
			if err != nil {
				continue
			}
			backups = append(backups, backup{path: filepath.Join(backupDir, name), mod: info.ModTime()})
		}
	}

	if len(backups) <= maxBackups {
		return
	}

	sort.Slice(backups, func(i, j int) bool {
		if backups[i].mod.Equal(backups[j].mod) {
			return backups[i].path < backups[j].path
		}
		return backups[i].mod.Before(backups[j].mod)
	})

	for _, b := range backups[:len(backups)-maxBackups] {
		_ = os.Remove(b.path)
	}
}

// copyBinary copies src to dst preserving (or applying) the executable mode
// and flushing the result to disk.
func copyBinary(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	if mode == 0 {
		if stat, err := in.Stat(); err == nil {
			mode = stat.Mode()
		} else {
			mode = 0o755
		}
	}

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}

	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}
