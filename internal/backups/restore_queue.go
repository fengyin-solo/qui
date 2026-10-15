// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package backups

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/autobrr/qui/internal/models"
)

const restoreQueueBuffer = 16

// ErrRestoreInstanceBusy is returned when a queued or running restore already
// holds the instance and the new request is not identical to it.
var ErrRestoreInstanceBusy = errors.New("restore already active for this instance")

// ErrRestoreNotActive is returned when cancel/resume targets a run that is not
// queued or running.
var ErrRestoreNotActive = errors.New("restore run is not active")

// RestoreRequest enqueues a restore of one snapshot run.
type RestoreRequest struct {
	InstanceID  int
	BackupRunID int64
	Mode        RestoreMode
	Options     RestoreOptions
	RequestedBy string
}

type restoreJob struct {
	restoreRunID int64
}

// restoreInflight helpers serialize restores per instance.

func (s *Service) currentRestoreRun(instanceID int) (int64, bool) {
	s.restoreInflightMu.Lock()
	defer s.restoreInflightMu.Unlock()
	id, ok := s.restoreInflight[instanceID]
	return id, ok
}

func (s *Service) markRestoreInstance(instanceID int, runID int64) bool {
	s.restoreInflightMu.Lock()
	defer s.restoreInflightMu.Unlock()
	if _, exists := s.restoreInflight[instanceID]; exists {
		return false
	}
	s.restoreInflight[instanceID] = runID
	return true
}

func (s *Service) setRestoreInstanceRun(instanceID int, runID int64) {
	s.restoreInflightMu.Lock()
	defer s.restoreInflightMu.Unlock()
	s.restoreInflight[instanceID] = runID
}

func (s *Service) clearRestoreInstance(instanceID int, runID int64) {
	s.restoreInflightMu.Lock()
	defer s.restoreInflightMu.Unlock()
	if current, ok := s.restoreInflight[instanceID]; ok && current == runID {
		delete(s.restoreInflight, instanceID)
	}
}

func (s *Service) registerRestoreCancel(runID int64, cancel context.CancelFunc) {
	s.restoreCancelsMu.Lock()
	defer s.restoreCancelsMu.Unlock()
	s.restoreCancels[runID] = cancel
}

func (s *Service) unregisterRestoreCancel(runID int64) {
	s.restoreCancelsMu.Lock()
	defer s.restoreCancelsMu.Unlock()
	delete(s.restoreCancels, runID)
}

func (s *Service) setRestoreProgress(runID int64, current, total int) {
	s.restoreProgressMu.Lock()
	s.restoreProgress[runID] = &BackupProgress{Current: current, Total: total, Percentage: percentage(current, total)}
	s.restoreProgressMu.Unlock()
}

func (s *Service) deleteRestoreProgress(runID int64) {
	s.restoreProgressMu.Lock()
	delete(s.restoreProgress, runID)
	s.restoreProgressMu.Unlock()
}

func percentage(current, total int) float64 {
	if total <= 0 {
		return 0
	}
	return float64(current) / float64(total) * 100
}

// GetRestoreRunProgress returns live progress for a run, falling back to the
// persisted counters when no in-memory entry exists.
func (s *Service) GetRestoreRunProgress(ctx context.Context, runID int64) *BackupProgress {
	s.restoreProgressMu.RLock()
	p := s.restoreProgress[runID]
	s.restoreProgressMu.RUnlock()
	if p != nil {
		return &BackupProgress{Current: p.Current, Total: p.Total, Percentage: p.Percentage}
	}
	if s.restoreStore != nil {
		if run, err := s.restoreStore.GetRun(ctx, runID); err == nil {
			return &BackupProgress{
				Current:    run.ProgressCurrent,
				Total:      run.ProgressTotal,
				Percentage: percentage(run.ProgressCurrent, run.ProgressTotal),
			}
		}
	}
	return nil
}

func restoreDedupKey(mode RestoreMode, opts RestoreOptions) string {
	excludes := append([]string(nil), opts.ExcludeHashes...)
	for i, hash := range excludes {
		excludes[i] = normalizeLowerTrim(hash)
	}
	sort.Strings(excludes)
	return strings.Join([]string{
		string(mode),
		fmt.Sprintf("%t", opts.DryRun),
		fmt.Sprintf("%t", opts.StartPaused),
		fmt.Sprintf("%t", opts.SkipHashCheck),
		fmt.Sprintf("%t", opts.AutoResumeVerified),
		strings.Join(excludes, ","),
	}, "|")
}

func restoreOptionsFromRun(run *models.RestoreRun) RestoreOptions {
	return RestoreOptions{
		DryRun:             run.DryRun,
		StartPaused:        run.Options.StartPaused,
		SkipHashCheck:      run.Options.SkipHashCheck,
		AutoResumeVerified: run.Options.AutoResumeVerified,
		ExcludeHashes:      append([]string(nil), run.Options.ExcludeHashes...),
	}
}

// QueueRestoreRun creates and enqueues a restore run. An identical active
// request for the instance returns the existing run (reused=true); a different
// active restore returns ErrRestoreInstanceBusy.
func (s *Service) QueueRestoreRun(ctx context.Context, req RestoreRequest) (*models.RestoreRun, bool, error) {
	if s == nil || s.restoreStore == nil || s.store == nil {
		return nil, false, errors.New("restore service unavailable")
	}

	mode := req.Mode
	if mode == "" {
		mode = RestoreModeIncremental
	}
	if !isValidRestoreMode(mode) {
		return nil, false, fmt.Errorf("unsupported restore mode: %s", mode)
	}
	req.Mode = mode

	snapshot, err := s.store.GetRun(ctx, req.BackupRunID)
	if err != nil {
		return nil, false, err
	}
	if snapshot.InstanceID != req.InstanceID {
		return nil, false, sql.ErrNoRows
	}

	requestedBy := strings.TrimSpace(req.RequestedBy)
	if requestedBy == "" {
		requestedBy = "api"
	}

	newKey := restoreDedupKey(mode, req.Options)

	if existingID, busy := s.currentRestoreRun(req.InstanceID); busy {
		existing, getErr := s.restoreStore.GetRun(ctx, existingID)
		if getErr == nil && isRestoreActive(existing.Status) &&
			existing.BackupRunID == req.BackupRunID &&
			restoreDedupKey(RestoreMode(existing.Mode), restoreOptionsFromRun(existing)) == newKey {
			return existing, true, nil
		}
		if getErr == nil {
			return existing, false, ErrRestoreInstanceBusy
		}
		return nil, false, ErrRestoreInstanceBusy
	}

	run := &models.RestoreRun{
		InstanceID:  req.InstanceID,
		BackupRunID: req.BackupRunID,
		Mode:        string(mode),
		DryRun:      req.Options.DryRun,
		Options: models.RestoreOptionsJSON{
			StartPaused:        req.Options.StartPaused,
			SkipHashCheck:      req.Options.SkipHashCheck,
			AutoResumeVerified: req.Options.AutoResumeVerified,
			ExcludeHashes:      append([]string(nil), req.Options.ExcludeHashes...),
		},
		Status:      models.RestoreStatusQueued,
		RequestedBy: requestedBy,
		RequestedAt: s.now(),
	}

	if !s.markRestoreInstance(req.InstanceID, 0) {
		return nil, false, ErrRestoreInstanceBusy
	}

	if err := s.restoreStore.CreateRun(ctx, run); err != nil {
		s.clearRestoreInstance(req.InstanceID, 0)
		return nil, false, err
	}
	s.setRestoreInstanceRun(req.InstanceID, run.ID)

	select {
	case <-ctx.Done():
		// Keep the row so the queued work is visible; shutdown/startup recovery
		// reconciles it. Release the instance marker for this process.
		s.clearRestoreInstance(req.InstanceID, run.ID)
		return nil, false, ctx.Err()
	case s.restoreJobs <- restoreJob{restoreRunID: run.ID}:
	}

	s.emitRestoreRunActivity(req.InstanceID, run.ID)
	return run, false, nil
}

func isRestoreActive(status models.RestoreRunStatus) bool {
	return status == models.RestoreStatusQueued || status == models.RestoreStatusRunning
}

func (s *Service) restoreWorker(ctx context.Context) {
	defer s.wg.Done()

	for {
		select {
		case <-ctx.Done():
			s.drainRestoreQueue()
			return
		case job := <-s.restoreJobs:
			s.handleRestoreJob(ctx, job)
		}
	}
}

// drainRestoreQueue clears jobs left in the channel after shutdown. Queued rows
// were already marked canceled by cancelQueuedRestores; the running job (never
// back in the channel) finalized through its own cancellation path.
func (s *Service) drainRestoreQueue() {
	for {
		select {
		case job := <-s.restoreJobs:
			run, err := s.restoreStore.GetRun(context.Background(), job.restoreRunID)
			if err == nil {
				s.clearRestoreInstance(run.InstanceID, job.restoreRunID)
				s.deleteRestoreProgress(job.restoreRunID)
				s.emitRestoreRunActivity(run.InstanceID, job.restoreRunID)
			} else {
				s.restoreInflightMu.Lock()
				for instanceID, id := range s.restoreInflight {
					if id == job.restoreRunID {
						delete(s.restoreInflight, instanceID)
					}
				}
				s.restoreInflightMu.Unlock()
			}
		default:
			return
		}
	}
}

func (s *Service) handleRestoreJob(ctx context.Context, job restoreJob) {
	run, err := s.restoreStore.GetRun(ctx, job.restoreRunID)
	if err != nil {
		// Row is gone (deleted) - just release any marker.
		s.restoreInflightMu.Lock()
		for instanceID, id := range s.restoreInflight {
			if id == job.restoreRunID {
				delete(s.restoreInflight, instanceID)
			}
		}
		s.restoreInflightMu.Unlock()
		return
	}

	finalizeSkipped := func() {
		s.clearRestoreInstance(run.InstanceID, run.ID)
		s.deleteRestoreProgress(run.ID)
		s.emitRestoreRunActivity(run.InstanceID, run.ID)
	}

	// Canceled before it ever ran (queued cancel request or shutdown sweep).
	if run.Status != models.RestoreStatusQueued || run.CancelRequested {
		if run.Status == models.RestoreStatusQueued {
			now := s.now()
			if _, err := s.restoreStore.TransitionStatus(ctx, run.ID,
				[]models.RestoreRunStatus{models.RestoreStatusQueued},
				models.RestoreStatusCanceled, nil, &now, nil); err != nil {
				log.Warn().Err(err).Int64("restoreRunID", run.ID).Msg("Failed to cancel queued restore")
			}
		}
		finalizeSkipped()
		return
	}

	started := s.now()
	ok, err := s.restoreStore.TransitionStatus(ctx, run.ID,
		[]models.RestoreRunStatus{models.RestoreStatusQueued},
		models.RestoreStatusRunning, &started, nil, nil)
	if err != nil {
		log.Error().Err(err).Int64("restoreRunID", run.ID).Msg("Failed to mark restore running")
		finalizeSkipped()
		return
	}
	if !ok {
		finalizeSkipped()
		return
	}
	run.Status = models.RestoreStatusRunning
	run.StartedAt = &started
	s.emitRestoreRunActivity(run.InstanceID, run.ID)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.registerRestoreCancel(run.ID, cancel)

	s.executeQueuedRestore(runCtx, run)

	s.unregisterRestoreCancel(run.ID)
	s.clearRestoreInstance(run.InstanceID, run.ID)
	s.deleteRestoreProgress(run.ID)
	s.emitRestoreRunActivity(run.InstanceID, run.ID)
}

// executeQueuedRestore plans and applies one restore run, persisting its
// terminal state. It never returns an error: every outcome is recorded on the
// run so clients can observe or replay it.
func (s *Service) executeQueuedRestore(ctx context.Context, run *models.RestoreRun) {
	mode := RestoreMode(run.Mode)
	opts := restoreOptionsFromRun(run)

	finish := func(status models.RestoreRunStatus, result *RestoreResult, errMsg *string) {
		var resultJSON *string
		if result != nil {
			// Plan is already stored in plan_json; keep result_json focused on
			// the execution summary.
			summary := *result
			summary.Plan = nil
			if data, err := json.Marshal(summary); err == nil {
				text := string(data)
				resultJSON = &text
			}
		}
		if err := s.restoreStore.SaveResult(context.Background(), run.ID, status, resultJSON, errMsg, s.now()); err != nil {
			log.Error().Err(err).Int64("restoreRunID", run.ID).Msg("Failed to persist restore result")
		}
	}

	fail := func(msg string, result *RestoreResult) {
		if ctx.Err() != nil {
			finish(models.RestoreStatusCanceled, result, new("Restore canceled"))
			return
		}
		finish(models.RestoreStatusFailed, result, &msg)
	}

	var planOpts *RestorePlanOptions
	if len(opts.ExcludeHashes) > 0 {
		planOpts = &RestorePlanOptions{ExcludeHashes: opts.ExcludeHashes}
	}

	plan, err := s.PlanRestoreDiff(ctx, run.BackupRunID, mode, planOpts)
	if err != nil {
		fail(fmt.Sprintf("build restore plan: %v", err), nil)
		return
	}

	planData, err := json.Marshal(plan)
	if err != nil {
		fail(fmt.Sprintf("encode restore plan: %v", err), nil)
		return
	}

	ops := expandRestoreOps(plan)
	if err := s.restoreStore.ReplacePlan(ctx, run.ID, string(planData), len(ops), ops); err != nil {
		fail(fmt.Sprintf("persist restore plan: %v", err), nil)
		return
	}

	result := &RestoreResult{
		Mode:       plan.Mode,
		RunID:      plan.RunID,
		InstanceID: plan.InstanceID,
		DryRun:     opts.DryRun,
		Plan:       plan,
	}

	if opts.DryRun {
		finish(models.RestoreStatusSuccess, result, nil)
		return
	}

	if s.reader == nil || s.categoryWriter == nil || s.tagWriter == nil || s.torrentWriter == nil {
		fail("sync manager unavailable", result)
		return
	}

	states, err := s.restoreStore.OpStateMap(ctx, run.ID)
	if err != nil {
		fail(fmt.Sprintf("load restore journal: %v", err), result)
		return
	}

	hook := &journalHook{
		svc:    s,
		runID:  run.ID,
		states: states,
		total:  len(ops),
	}
	s.setRestoreProgress(run.ID, 0, len(ops))

	applied, warnings, opErrors, abort := s.applyRestorePlan(ctx, plan, opts, hook)
	hook.flushProgress(true)

	result.Applied = applied
	result.Warnings = warnings
	result.Errors = opErrors

	// A cancel can surface either as an explicit boundary abort or as a
	// context error from the final in-flight operation; both are cancellation.
	if abort != nil || ctx.Err() != nil {
		finish(models.RestoreStatusCanceled, result, new("Restore canceled at a safe boundary"))
		return
	}
	if len(opErrors) > 0 {
		msg := fmt.Sprintf("%d restore operation(s) failed; resume to retry them", len(opErrors))
		finish(models.RestoreStatusFailed, result, &msg)
		return
	}
	finish(models.RestoreStatusSuccess, result, nil)
}

//go:fix inline
func stringPtr(v string) *string { return new(v) }

// journalHook implements restoreHook against the persisted operation journal.
// Journal and progress writes use a background context so they still land when
// the run's context is canceled at a boundary.
type journalHook struct {
	svc        *Service
	runID      int64
	states     map[string]models.RestoreOpStatus
	total      int
	current    int
	lastFlush  time.Time
	progressMu sync.Mutex
}

func (h *journalHook) split(key string) (domain, action, target string) {
	parts := strings.SplitN(key, "|", 3)
	if len(parts) != 3 {
		return "", "", key
	}
	return parts[0], parts[1], parts[2]
}

func (h *journalHook) mark(key string, status models.RestoreOpStatus, message *string) {
	domain, action, target := h.split(key)
	h.states[key] = status
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.svc.restoreStore.MarkOp(ctx, h.runID, domain, action, target, status, message); err != nil {
		log.Warn().Err(err).Int64("restoreRunID", h.runID).Str("op", key).Msg("Failed to journal restore operation")
	}
}

func (h *journalHook) start(runCtx context.Context, key string) (bool, error) {
	if err := runCtx.Err(); err != nil {
		return false, err
	}
	if h.states[key] == models.RestoreOpSucceeded {
		return false, nil
	}
	h.mark(key, models.RestoreOpRunning, nil)
	return true, nil
}

func (h *journalHook) finish(key string, execErr error) {
	if execErr != nil {
		msg := execErr.Error()
		h.mark(key, models.RestoreOpFailed, &msg)
		return
	}
	h.mark(key, models.RestoreOpSucceeded, nil)
}

func (h *journalHook) tick() {
	h.progressMu.Lock()
	h.current++
	current, total := h.current, h.total
	force := time.Since(h.lastFlush) >= 250*time.Millisecond
	if force {
		h.lastFlush = time.Now()
	}
	h.progressMu.Unlock()

	h.svc.setRestoreProgress(h.runID, current, total)
	if force {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.svc.restoreStore.UpdateProgress(ctx, h.runID, current, total); err != nil {
			log.Debug().Err(err).Int64("restoreRunID", h.runID).Msg("Failed to persist restore progress")
		}
	}
}

func (h *journalHook) flushProgress(_ bool) {
	h.progressMu.Lock()
	current, total := h.current, h.total
	h.progressMu.Unlock()
	h.svc.setRestoreProgress(h.runID, current, total)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = h.svc.restoreStore.UpdateProgress(ctx, h.runID, current, total)
}

// CancelRestoreRun requests cancellation. A queued run is skipped by the worker;
// a running run stops at its next operation boundary.
func (s *Service) CancelRestoreRun(ctx context.Context, runID int64) (*models.RestoreRun, error) {
	run, err := s.restoreStore.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	if !isRestoreActive(run.Status) {
		return nil, ErrRestoreNotActive
	}

	if err := s.restoreStore.SetCancelRequested(ctx, runID); err != nil {
		return nil, err
	}

	if run.Status == models.RestoreStatusRunning {
		s.restoreCancelsMu.Lock()
		cancel := s.restoreCancels[runID]
		s.restoreCancelsMu.Unlock()
		if cancel != nil {
			cancel()
		}
	}

	run.CancelRequested = true
	s.emitRestoreRunActivity(run.InstanceID, runID)
	return run, nil
}

// ResumeRestoreRun re-queues an interrupted, failed or canceled run. The
// journal is retained so already-succeeded operations are not replayed.
func (s *Service) ResumeRestoreRun(ctx context.Context, runID int64) (*models.RestoreRun, error) {
	run, err := s.restoreStore.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}

	if isRestoreActive(run.Status) {
		return nil, ErrRestoreInstanceBusy
	}
	if run.Status == models.RestoreStatusSuccess {
		return nil, errors.New("restore run already completed")
	}

	if !s.markRestoreInstance(run.InstanceID, runID) {
		return nil, ErrRestoreInstanceBusy
	}

	now := s.now()
	ok, err := s.restoreStore.ResetForResume(ctx, runID, now)
	if err != nil {
		s.clearRestoreInstance(run.InstanceID, runID)
		return nil, err
	}
	if !ok {
		s.clearRestoreInstance(run.InstanceID, runID)
		return nil, ErrRestoreNotActive
	}
	run.Status = models.RestoreStatusQueued
	run.CancelRequested = false
	run.Error = nil
	run.RequestedAt = now

	select {
	case <-ctx.Done():
		s.clearRestoreInstance(run.InstanceID, runID)
		return nil, ctx.Err()
	case s.restoreJobs <- restoreJob{restoreRunID: runID}:
	}

	s.emitRestoreRunActivity(run.InstanceID, runID)
	return run, nil
}

// ListRestoreRuns returns restore run history for an instance.
func (s *Service) ListRestoreRuns(ctx context.Context, instanceID, limit, offset int) ([]*models.RestoreRun, error) {
	return s.restoreStore.ListByInstance(ctx, instanceID, limit, offset)
}

// GetRestoreRun loads one restore run.
func (s *Service) GetRestoreRun(ctx context.Context, runID int64) (*models.RestoreRun, error) {
	return s.restoreStore.GetRun(ctx, runID)
}

// ListRestoreOps returns the operation journal for a run.
func (s *Service) ListRestoreOps(ctx context.Context, runID int64) ([]*models.RestoreRunOp, error) {
	return s.restoreStore.ListOps(ctx, runID)
}

// recoverInterruptedRestoreRuns marks queued/running restores from a previous
// process as interrupted. Runs whose snapshot or persisted data is damaged are
// marked failed and reported instead of being offered for replay.
func (s *Service) recoverInterruptedRestoreRuns(ctx context.Context) {
	if s == nil || s.restoreStore == nil {
		return
	}

	refs, err := s.restoreStore.FindActiveRunsIDs(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to list interrupted restore runs")
		return
	}
	if len(refs) == 0 {
		return
	}

	log.Info().Int("count", len(refs)).Msg("Recovering interrupted restore runs")

	var interrupted []models.ActiveRestoreRef
	for _, ref := range refs {
		if reason := s.restoreCorruptionReason(ctx, ref.ID); reason != "" {
			msg := "Restore run is damaged and cannot be resumed: " + reason
			if err := s.restoreStore.MarkRunsStatus(ctx, []int64{ref.ID}, models.RestoreStatusFailed, &msg); err != nil {
				log.Warn().Err(err).Int64("restoreRunID", ref.ID).Msg("Failed to mark damaged restore run failed")
			} else {
				log.Warn().Int64("restoreRunID", ref.ID).Str("reason", reason).Msg("Skipping damaged restore run")
			}
			s.emitRestoreRunActivity(ref.InstanceID, ref.ID)
			continue
		}
		interrupted = append(interrupted, ref)
	}

	if len(interrupted) > 0 {
		ids := make([]int64, len(interrupted))
		for i, ref := range interrupted {
			ids[i] = ref.ID
		}
		msg := "Restore interrupted by application restart"
		if err := s.restoreStore.MarkRunsStatus(ctx, ids, models.RestoreStatusInterrupted, &msg); err != nil {
			log.Warn().Err(err).Msg("Failed to mark restore runs interrupted")
			return
		}
		for _, ref := range interrupted {
			s.emitRestoreRunActivity(ref.InstanceID, ref.ID)
		}
	}
}

// restoreCorruptionReason returns a non-empty reason when an active restore run
// cannot safely be offered for resume: missing snapshot run, or unreadable
// options/plan JSON.
func (s *Service) restoreCorruptionReason(ctx context.Context, runID int64) string {
	run, rawOptions, err := s.restoreStore.GetRunRawOptions(ctx, runID)
	if err != nil {
		return fmt.Sprintf("load run: %v", err)
	}

	var options models.RestoreOptionsJSON
	if strings.TrimSpace(rawOptions) != "" {
		if err := json.Unmarshal([]byte(rawOptions), &options); err != nil {
			return fmt.Sprintf("invalid options: %v", err)
		}
	}
	if !isValidRestoreMode(RestoreMode(run.Mode)) {
		return "unsupported restore mode: " + run.Mode
	}
	if run.PlanJSON != nil && strings.TrimSpace(*run.PlanJSON) != "" {
		var plan RestorePlan
		if err := json.Unmarshal([]byte(*run.PlanJSON), &plan); err != nil {
			return fmt.Sprintf("invalid plan: %v", err)
		}
	}
	if _, err := s.store.GetRun(ctx, run.BackupRunID); err != nil {
		return fmt.Sprintf("snapshot run unavailable: %v", err)
	}
	return ""
}

// cancelQueuedRestores marks never-started restores canceled during shutdown.
func (s *Service) cancelQueuedRestores() {
	if s == nil || s.restoreStore == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if n, err := s.restoreStore.CancelAllQueued(ctx); err != nil {
		log.Warn().Err(err).Msg("Failed to cancel queued restores during shutdown")
	} else if n > 0 {
		log.Info().Int64("count", n).Msg("Canceled queued restores during shutdown")
	}
}
