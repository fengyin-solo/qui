// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package models

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/autobrr/qui/internal/dbinterface"
)

// RestoreRunStatus is the lifecycle state of an asynchronous restore run.
type RestoreRunStatus string

const (
	RestoreStatusQueued      RestoreRunStatus = "queued"
	RestoreStatusRunning     RestoreRunStatus = "running"
	RestoreStatusSuccess     RestoreRunStatus = "success"
	RestoreStatusFailed      RestoreRunStatus = "failed"
	RestoreStatusCanceled    RestoreRunStatus = "canceled"
	RestoreStatusInterrupted RestoreRunStatus = "interrupted"
)

// ActiveRestoreStatuses are the states that imply a run may still execute.
var ActiveRestoreStatuses = []RestoreRunStatus{RestoreStatusQueued, RestoreStatusRunning}

// RestoreOpStatus is the per-operation state recorded in the restore journal.
type RestoreOpStatus string

const (
	RestoreOpPending   RestoreOpStatus = "pending"
	RestoreOpRunning   RestoreOpStatus = "running"
	RestoreOpSucceeded RestoreOpStatus = "succeeded"
	RestoreOpFailed    RestoreOpStatus = "failed"
	RestoreOpSkipped   RestoreOpStatus = "skipped"
)

// RestoreOptionsJSON is the persisted execution options for a restore run.
type RestoreOptionsJSON struct {
	StartPaused        bool     `json:"startPaused"`
	SkipHashCheck      bool     `json:"skipHashCheck"`
	AutoResumeVerified bool     `json:"autoResumeVerified"`
	ExcludeHashes      []string `json:"excludeHashes,omitempty"`
}

// RestoreRun is one asynchronous restore attempt of a snapshot backup run.
type RestoreRun struct {
	ID              int64              `json:"id"`
	InstanceID      int                `json:"instanceId"`
	BackupRunID     int64              `json:"backupRunId"`
	Mode            string             `json:"mode"`
	DryRun          bool               `json:"dryRun"`
	Options         RestoreOptionsJSON `json:"options"`
	Status          RestoreRunStatus   `json:"status"`
	RequestedBy     string             `json:"requestedBy"`
	RequestedAt     time.Time          `json:"requestedAt"`
	StartedAt       *time.Time         `json:"startedAt,omitempty"`
	CompletedAt     *time.Time         `json:"completedAt,omitempty"`
	Error           *string            `json:"error,omitempty"`
	CancelRequested bool               `json:"cancelRequested"`
	ProgressCurrent int                `json:"progressCurrent"`
	ProgressTotal   int                `json:"progressTotal"`
	PlanJSON        *string            `json:"planJson,omitempty"`
	ResultJSON      *string            `json:"resultJson,omitempty"`
	CreatedAt       time.Time          `json:"createdAt"`
	UpdatedAt       time.Time          `json:"updatedAt"`
}

// RestoreRunOp is a single journaled restore operation. The
// (RestoreRunID, Domain, Action, Target) tuple is unique and drives resume
// idempotency: succeeded operations are never replayed.
type RestoreRunOp struct {
	ID           int64           `json:"id"`
	RestoreRunID int64           `json:"restoreRunId"`
	Seq          int             `json:"seq"`
	Domain       string          `json:"domain"`
	Action       string          `json:"action"`
	Target       string          `json:"target"`
	Status       RestoreOpStatus `json:"status"`
	Message      *string         `json:"message,omitempty"`
	DetailJSON   *string         `json:"detailJson,omitempty"`
	UpdatedAt    time.Time       `json:"updatedAt"`
}

// RestoreOpKey joins an op identity into the map key used by OpStateMap and the
// executor. Domain and action come from fixed enums without the delimiter, so a
// target (the final segment) may safely contain it.
func RestoreOpKey(domain, action, target string) string {
	return domain + "|" + action + "|" + target
}

// Key returns the op's identity key.
func (o *RestoreRunOp) Key() string {
	return RestoreOpKey(o.Domain, o.Action, o.Target)
}

// RestoreStore persists restore runs and their operation journal.
type RestoreStore struct {
	db dbinterface.Querier
}

func NewRestoreStore(db dbinterface.Querier) *RestoreStore {
	return &RestoreStore{db: db}
}

const restoreRunColumns = `
	id, instance_id, backup_run_id, mode, dry_run, options_json, status,
	requested_by, requested_at, started_at, completed_at, error, cancel_requested,
	progress_current, progress_total, plan_json, result_json, created_at, updated_at
`

func scanRestoreRun(scanner interface{ Scan(...any) error }) (*RestoreRun, error) {
	var run RestoreRun
	var optionsJSON, planJSON, resultJSON, errMsg sql.NullString
	var startedAt, completedAt sql.NullTime
	var dryRun, cancelRequested int

	if err := scanner.Scan(
		&run.ID,
		&run.InstanceID,
		&run.BackupRunID,
		&run.Mode,
		&dryRun,
		&optionsJSON,
		&run.Status,
		&run.RequestedBy,
		&run.RequestedAt,
		&startedAt,
		&completedAt,
		&errMsg,
		&cancelRequested,
		&run.ProgressCurrent,
		&run.ProgressTotal,
		&planJSON,
		&resultJSON,
		&run.CreatedAt,
		&run.UpdatedAt,
	); err != nil {
		return nil, err
	}

	run.DryRun = SQLiteIntToBool(dryRun)
	run.CancelRequested = SQLiteIntToBool(cancelRequested)
	if startedAt.Valid {
		run.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		run.CompletedAt = &completedAt.Time
	}
	if errMsg.Valid {
		run.Error = &errMsg.String
	}
	if planJSON.Valid {
		run.PlanJSON = &planJSON.String
	}
	if resultJSON.Valid {
		run.ResultJSON = &resultJSON.String
	}
	if strings.TrimSpace(optionsJSON.String) != "" {
		if err := json.Unmarshal([]byte(optionsJSON.String), &run.Options); err != nil {
			return nil, fmt.Errorf("decode restore options: %w", err)
		}
	}

	return &run, nil
}

// CreateRun inserts a queued restore run and populates its ID.
func (s *RestoreStore) CreateRun(ctx context.Context, run *RestoreRun) error {
	if run == nil {
		return errors.New("run cannot be nil")
	}

	optionsJSON, err := json.Marshal(run.Options)
	if err != nil {
		return fmt.Errorf("marshal restore options: %w", err)
	}

	err = s.db.QueryRowContext(ctx, `
		INSERT INTO instance_restore_runs (
			instance_id, backup_run_id, mode, dry_run, options_json, status,
			requested_by, requested_at, started_at, completed_at, error,
			cancel_requested, progress_current, progress_total, plan_json, result_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id
	`,
		run.InstanceID, run.BackupRunID, run.Mode, BoolToSQLite(run.DryRun), string(optionsJSON), string(run.Status),
		run.RequestedBy, run.RequestedAt, run.StartedAt, run.CompletedAt, run.Error,
		BoolToSQLite(run.CancelRequested), run.ProgressCurrent, run.ProgressTotal, run.PlanJSON, run.ResultJSON,
	).Scan(&run.ID)
	if err != nil {
		return err
	}

	return nil
}

// GetRun loads one restore run.
func (s *RestoreStore) GetRun(ctx context.Context, id int64) (*RestoreRun, error) {
	row := s.db.QueryRowContext(ctx, "SELECT "+restoreRunColumns+" FROM instance_restore_runs WHERE id = ?", id)
	return scanRestoreRun(row)
}

// GetRunRawOptions is like GetRun but does not fail on a corrupt options blob:
// startup recovery uses it to detect and report damaged runs.
func (s *RestoreStore) GetRunRawOptions(ctx context.Context, id int64) (*RestoreRun, string, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, instance_id, backup_run_id, mode, dry_run, options_json, status,
			requested_by, requested_at, started_at, completed_at, error, cancel_requested,
			progress_current, progress_total, plan_json, result_json, created_at, updated_at
		FROM instance_restore_runs WHERE id = ?
	`, id)

	var run RestoreRun
	var optionsJSON, planJSON, resultJSON, errMsg sql.NullString
	var startedAt, completedAt sql.NullTime
	var dryRun, cancelRequested int

	if err := row.Scan(
		&run.ID, &run.InstanceID, &run.BackupRunID, &run.Mode, &dryRun, &optionsJSON, &run.Status,
		&run.RequestedBy, &run.RequestedAt, &startedAt, &completedAt, &errMsg, &cancelRequested,
		&run.ProgressCurrent, &run.ProgressTotal, &planJSON, &resultJSON, &run.CreatedAt, &run.UpdatedAt,
	); err != nil {
		return nil, "", err
	}
	run.DryRun = SQLiteIntToBool(dryRun)
	run.CancelRequested = SQLiteIntToBool(cancelRequested)
	if startedAt.Valid {
		run.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		run.CompletedAt = &completedAt.Time
	}
	if errMsg.Valid {
		run.Error = &errMsg.String
	}
	if planJSON.Valid {
		run.PlanJSON = &planJSON.String
	}
	if resultJSON.Valid {
		run.ResultJSON = &resultJSON.String
	}
	return &run, optionsJSON.String, nil
}

// ListByInstance returns restore runs for an instance, newest first.
func (s *RestoreStore) ListByInstance(ctx context.Context, instanceID, limit, offset int) ([]*RestoreRun, error) {
	if limit <= 0 {
		limit = 25
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT `+restoreRunColumns+`
		FROM instance_restore_runs
		WHERE instance_id = ?
		ORDER BY requested_at DESC, id DESC
		LIMIT ? OFFSET ?
	`, instanceID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []*RestoreRun
	for rows.Next() {
		run, err := scanRestoreRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

// TransitionStatus moves a run to `to` only while its status is one of `from`.
// It reports whether the transition was applied, so callers never overwrite a
// run that another path already finalized.
func (s *RestoreStore) TransitionStatus(ctx context.Context, id int64, from []RestoreRunStatus, to RestoreRunStatus, startedAt, completedAt *time.Time, errMsg *string) (bool, error) {
	if len(from) == 0 {
		return false, errors.New("transition requires source statuses")
	}

	placeholders := buildInPlaceholders(len(from))
	args := make([]any, 0, len(from)+5)
	args = append(args, string(to), completedAt, startedAt, errMsg, id)
	for _, status := range from {
		args = append(args, string(status))
	}

	res, err := s.db.ExecContext(ctx, `
		UPDATE instance_restore_runs
		SET status = ?, completed_at = ?, started_at = COALESCE(started_at, ?), error = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ? AND status IN `+placeholders,
		args...)
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

// SetCancelRequested persists a cancellation request. The worker observes it at
// the next operation boundary.
func (s *RestoreStore) SetCancelRequested(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE instance_restore_runs
		SET cancel_requested = 1, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, id)
	return err
}

// UpdateProgress persists progress counters. Callers throttle this.
func (s *RestoreStore) UpdateProgress(ctx context.Context, id int64, current, total int) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE instance_restore_runs
		SET progress_current = ?, progress_total = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, current, total, id)
	return err
}

// ReplacePlan stores the generated plan and reconciles the operation journal
// with the freshly expanded op set in one transaction:
//   - new ops are inserted as pending (existing rows are untouched);
//   - pending/running ops absent from the new set are marked skipped.
//
// Succeeded and failed rows are always preserved.
func (s *RestoreStore) ReplacePlan(ctx context.Context, id int64, planJSON string, total int, ops []RestoreRunOp) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin replace plan: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `
		UPDATE instance_restore_runs
		SET plan_json = ?, progress_total = ?, progress_current = 0, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, planJSON, total, id); err != nil {
		return fmt.Errorf("store plan: %w", err)
	}

	newKeys := make(map[string]struct{}, len(ops))
	for i := range ops {
		op := &ops[i]
		newKeys[op.Key()] = struct{}{}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO instance_restore_run_ops (restore_run_id, seq, domain, action, target, status)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(restore_run_id, domain, action, target) DO NOTHING
		`, id, op.Seq, op.Domain, op.Action, op.Target, string(RestoreOpPending)); err != nil {
			return fmt.Errorf("insert restore op: %w", err)
		}
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT id, domain, action, target
		FROM instance_restore_run_ops
		WHERE restore_run_id = ? AND status IN (?, ?)
	`, id, string(RestoreOpPending), string(RestoreOpRunning))
	if err != nil {
		return fmt.Errorf("load pending ops: %w", err)
	}
	type staleOp struct {
		id  int64
		key string
	}
	var stale []staleOp
	for rows.Next() {
		var opID int64
		var domain, action, target string
		if err := rows.Scan(&opID, &domain, &action, &target); err != nil {
			rows.Close()
			return err
		}
		if _, ok := newKeys[RestoreOpKey(domain, action, target)]; !ok {
			stale = append(stale, staleOp{id: opID})
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}

	for _, op := range stale {
		if _, err := tx.ExecContext(ctx, `
			UPDATE instance_restore_run_ops SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?
		`, string(RestoreOpSkipped), op.id); err != nil {
			return fmt.Errorf("skip stale op: %w", err)
		}
	}

	return tx.Commit()
}

// SaveResult finalizes a run with its terminal status, result summary and error.
func (s *RestoreStore) SaveResult(ctx context.Context, id int64, status RestoreRunStatus, resultJSON, errMsg *string, completedAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE instance_restore_runs
		SET status = ?, result_json = ?, error = ?, completed_at = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, string(status), resultJSON, errMsg, completedAt, id)
	return err
}

// MarkOp updates one journal operation by its identity key.
func (s *RestoreStore) MarkOp(ctx context.Context, runID int64, domain, action, target string, status RestoreOpStatus, message *string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE instance_restore_run_ops
		SET status = ?, message = ?, updated_at = CURRENT_TIMESTAMP
		WHERE restore_run_id = ? AND domain = ? AND action = ? AND target = ?
	`, string(status), message, runID, domain, action, target)
	return err
}

// ListOps returns the journal for a run ordered by sequence.
func (s *RestoreStore) ListOps(ctx context.Context, runID int64) ([]*RestoreRunOp, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, restore_run_id, seq, domain, action, target, status, message, detail_json, updated_at
		FROM instance_restore_run_ops
		WHERE restore_run_id = ?
		ORDER BY seq ASC, id ASC
	`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ops []*RestoreRunOp
	for rows.Next() {
		var op RestoreRunOp
		var message, detail sql.NullString
		if err := rows.Scan(
			&op.ID, &op.RestoreRunID, &op.Seq, &op.Domain, &op.Action, &op.Target,
			&op.Status, &message, &detail, &op.UpdatedAt,
		); err != nil {
			return nil, err
		}
		if message.Valid {
			op.Message = &message.String
		}
		if detail.Valid {
			op.DetailJSON = &detail.String
		}
		ops = append(ops, &op)
	}
	return ops, rows.Err()
}

// OpStateMap returns identity key -> status for every op in a run.
func (s *RestoreStore) OpStateMap(ctx context.Context, runID int64) (map[string]RestoreOpStatus, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT domain, action, target, status
		FROM instance_restore_run_ops
		WHERE restore_run_id = ?
	`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	states := make(map[string]RestoreOpStatus)
	for rows.Next() {
		var domain, action, target, status string
		if err := rows.Scan(&domain, &action, &target, &status); err != nil {
			return nil, err
		}
		states[RestoreOpKey(domain, action, target)] = RestoreOpStatus(status)
	}
	return states, rows.Err()
}

// ActiveRestoreRef identifies a run left queued/running by a previous session.
type ActiveRestoreRef struct {
	ID         int64
	InstanceID int
}

// FindActiveRunsIDs lists queued/running restore run ids without decoding any
// JSON columns, so a single damaged row cannot abort startup recovery.
func (s *RestoreStore) FindActiveRunsIDs(ctx context.Context) ([]ActiveRestoreRef, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, instance_id
		FROM instance_restore_runs
		WHERE status IN (?, ?)
		ORDER BY requested_at ASC, id ASC
	`, string(RestoreStatusQueued), string(RestoreStatusRunning))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var refs []ActiveRestoreRef
	for rows.Next() {
		var ref ActiveRestoreRef
		if err := rows.Scan(&ref.ID, &ref.InstanceID); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, rows.Err()
}

// ResetForResume moves an interrupted/failed/canceled run back to queued so the
// worker replays it. The journal is retained; succeeded ops are skipped.
func (s *RestoreStore) ResetForResume(ctx context.Context, id int64, requestedAt time.Time) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE instance_restore_runs
		SET status = ?, cancel_requested = 0, error = NULL, started_at = NULL,
		    completed_at = NULL, progress_current = 0, requested_at = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ? AND status IN (?, ?, ?)
	`,
		string(RestoreStatusQueued), requestedAt, id,
		string(RestoreStatusInterrupted), string(RestoreStatusFailed), string(RestoreStatusCanceled),
	)
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

// MarkRunsStatus sets status (and optional error) for a batch of run ids.
func (s *RestoreStore) MarkRunsStatus(ctx context.Context, ids []int64, status RestoreRunStatus, errMsg *string) error {
	if len(ids) == 0 {
		return nil
	}

	const chunkSize = 900
	for i := 0; i < len(ids); i += chunkSize {
		end := min(i+chunkSize, len(ids))
		chunk := ids[i:end]

		if _, err := s.db.ExecContext(ctx,
			"UPDATE instance_restore_runs SET status = ?, error = ?, updated_at = CURRENT_TIMESTAMP WHERE id IN "+buildInPlaceholders(len(chunk)),
			append([]any{string(status), errMsg}, int64SliceToAny(chunk)...)...,
		); err != nil {
			return err
		}
	}
	return nil
}

// CancelAllQueued cancels every queued (never started) run and returns the
// affected count. Running runs are left to reach a safe boundary.
func (s *RestoreStore) CancelAllQueued(ctx context.Context) (int64, error) {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `
		UPDATE instance_restore_runs
		SET status = ?, completed_at = ?, cancel_requested = 1, updated_at = CURRENT_TIMESTAMP
		WHERE status = ?
	`, string(RestoreStatusCanceled), now, string(RestoreStatusQueued))
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
}

func int64SliceToAny(ids []int64) []any {
	out := make([]any, len(ids))
	for i, id := range ids {
		out[i] = id
	}
	return out
}
