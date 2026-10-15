-- Copyright (c) 2025-2026, s0up and the autobrr contributors.
-- SPDX-License-Identifier: GPL-2.0-or-later

-- Async restore runs: one queued/running restore per qBittorrent instance, with
-- persisted status, progress, cancellation and the snapshot run it restores.
CREATE TABLE IF NOT EXISTS instance_restore_runs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id      INTEGER NOT NULL,
    backup_run_id    INTEGER NOT NULL,
    mode             TEXT NOT NULL,
    dry_run          INTEGER NOT NULL DEFAULT 0,
    options_json     TEXT NOT NULL DEFAULT '{}',
    status           TEXT NOT NULL DEFAULT 'queued',
    requested_by     TEXT NOT NULL DEFAULT '',
    requested_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at       DATETIME,
    completed_at     DATETIME,
    error            TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    progress_current INTEGER NOT NULL DEFAULT 0,
    progress_total   INTEGER NOT NULL DEFAULT 0,
    plan_json        TEXT,
    result_json      TEXT,
    created_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (instance_id) REFERENCES instances(id) ON DELETE CASCADE,
    FOREIGN KEY (backup_run_id) REFERENCES instance_backup_runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_restore_runs_instance_status
    ON instance_restore_runs(instance_id, status);
CREATE INDEX IF NOT EXISTS idx_restore_runs_status
    ON instance_restore_runs(status);

-- Per-operation journal. It is both the user-visible operation log and the
-- idempotency source for resume: succeeded operations are never replayed.
CREATE TABLE IF NOT EXISTS instance_restore_run_ops (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    restore_run_id INTEGER NOT NULL,
    seq            INTEGER NOT NULL DEFAULT 0,
    domain         TEXT NOT NULL,
    action         TEXT NOT NULL,
    target         TEXT NOT NULL DEFAULT '',
    status         TEXT NOT NULL DEFAULT 'pending',
    message        TEXT,
    detail_json    TEXT,
    updated_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (restore_run_id) REFERENCES instance_restore_runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_restore_run_ops_run
    ON instance_restore_run_ops(restore_run_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_restore_run_ops_key
    ON instance_restore_run_ops(restore_run_id, domain, action, target);
CREATE INDEX IF NOT EXISTS idx_restore_run_ops_run_status
    ON instance_restore_run_ops(restore_run_id, status);
