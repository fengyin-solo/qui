// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package update

import (
	"context"
	"os/exec"
	"strings"
	"time"
)

// execBinaryVersion runs the replaced binary with the "version" subcommand to
// prove the file is executable on this platform and reports the expected
// version.
func execBinaryVersion(ctx context.Context, target string) (string, error) {
	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	out, err := exec.CommandContext(runCtx, target, "version").Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}
