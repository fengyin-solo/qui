// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package backups

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/autobrr/qui/internal/models"
	"github.com/autobrr/qui/internal/qbittorrent"
)

// RestoreOptions control restore execution behaviour.
type RestoreOptions struct {
	DryRun             bool
	StartPaused        bool
	SkipHashCheck      bool
	AutoResumeVerified bool
	ExcludeHashes      []string
}

// RestoreError captures an operation failure during restore execution.
type RestoreError struct {
	Operation string `json:"operation"`
	Target    string `json:"target"`
	Message   string `json:"message"`
}

// RestoreApplied summarises the actions that were successfully applied.
type RestoreApplied struct {
	Categories CategoryApplied `json:"categories"`
	Tags       TagApplied      `json:"tags"`
	Torrents   TorrentApplied  `json:"torrents"`
}

// CategoryApplied summarises category operations.
type CategoryApplied struct {
	Created []string `json:"created,omitempty"`
	Updated []string `json:"updated,omitempty"`
	Deleted []string `json:"deleted,omitempty"`
}

// TagApplied summarises tag operations.
type TagApplied struct {
	Created []string `json:"created,omitempty"`
	Deleted []string `json:"deleted,omitempty"`
}

// TorrentApplied summarises torrent operations.
type TorrentApplied struct {
	Added   []string `json:"added,omitempty"`
	Updated []string `json:"updated,omitempty"`
	Deleted []string `json:"deleted,omitempty"`
}

// RestoreResult describes the outcome of a restore execution.
type RestoreResult struct {
	Mode       RestoreMode    `json:"mode"`
	RunID      int64          `json:"runId"`
	InstanceID int            `json:"instanceId"`
	DryRun     bool           `json:"dryRun"`
	Plan       *RestorePlan   `json:"plan,omitempty"`
	Applied    RestoreApplied `json:"applied"`
	Warnings   []string       `json:"warnings,omitempty"`
	Errors     []RestoreError `json:"errors,omitempty"`
}

// Restore operation journal domains and actions. A journal key is one mutating
// qBittorrent call and the unit of progress, cancellation and resume
// idempotency.
const (
	restoreDomainCategory = "category"
	restoreDomainTag      = "tag"
	restoreDomainTorrent  = "torrent"

	restoreActionCreate      = "create"
	restoreActionUpdate      = "update"
	restoreActionDelete      = "delete"
	restoreActionAdd         = "add"
	restoreActionSetCategory = "set_category"
	restoreActionSetTags     = "set_tags"
)

// restoreHook observes each mutating operation during a restore. start enforces
// the cancellation boundary and skips work a previous attempt already finished;
// finish journals the outcome; tick drives progress. Implementations must be
// safe for the single-worker restore queue.
type restoreHook interface {
	start(ctx context.Context, key string) (proceed bool, abort error)
	finish(key string, execErr error)
	tick()
}

// nopRestoreHook preserves pre-queue behavior for direct unit tests: it only
// honors context cancellation and keeps no journal.
type nopRestoreHook struct{}

func (nopRestoreHook) start(ctx context.Context, _ string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return true, nil
}
func (nopRestoreHook) tick()                    {}
func (nopRestoreHook) finish(_ string, _ error) {}

// applyRestorePlan executes a fully built plan through the journaling hook. It
// returns the applied summary, warnings and per-op errors. A non-nil error is
// returned only when execution aborted at a cancellation boundary.
func (s *Service) applyRestorePlan(ctx context.Context, plan *RestorePlan, opts RestoreOptions, hook restoreHook) (RestoreApplied, []string, []RestoreError, error) {
	var applied RestoreApplied
	var errs []RestoreError

	if err := s.applyCategoryPlan(ctx, plan, &applied, &errs, hook); err != nil {
		return applied, nil, errs, err
	}
	if err := s.applyTagPlan(ctx, plan, &applied, &errs, hook); err != nil {
		return applied, nil, errs, err
	}

	excludeSet := buildHashSet(opts.ExcludeHashes)
	warnings, err := s.applyTorrentPlan(ctx, plan, &applied, &errs, excludeSet, opts, hook)
	return applied, warnings, errs, err
}

func (s *Service) applyCategoryPlan(ctx context.Context, plan *RestorePlan, applied *RestoreApplied, errs *[]RestoreError, hook restoreHook) error {
	instanceID := plan.InstanceID

	for _, spec := range plan.Categories.Create {
		key := models.RestoreOpKey(restoreDomainCategory, restoreActionCreate, spec.Name)
		proceed, err := hook.start(ctx, key)
		if err != nil {
			return err
		}
		if proceed {
			err = s.categoryWriter.CreateCategory(ctx, instanceID, spec.Name, spec.SavePath)
			hook.finish(key, err)
			if err != nil {
				appendRestoreError(errs, "create_category", spec.Name, err)
				log.Warn().Err(err).Int("instanceID", instanceID).Str("category", spec.Name).Msg("Restore: create category failed")
			} else {
				applied.Categories.Created = append(applied.Categories.Created, spec.Name)
			}
		}
		hook.tick()
	}

	for _, update := range plan.Categories.Update {
		key := models.RestoreOpKey(restoreDomainCategory, restoreActionUpdate, update.Name)
		proceed, err := hook.start(ctx, key)
		if err != nil {
			return err
		}
		if proceed {
			err = s.categoryWriter.EditCategory(ctx, instanceID, update.Name, update.DesiredPath)
			hook.finish(key, err)
			if err != nil {
				appendRestoreError(errs, "update_category", update.Name, err)
				log.Warn().Err(err).Int("instanceID", instanceID).Str("category", update.Name).Msg("Restore: update category failed")
			} else {
				applied.Categories.Updated = append(applied.Categories.Updated, update.Name)
			}
		}
		hook.tick()
	}

	if len(plan.Categories.Delete) == 0 {
		return nil
	}

	targets := make([]string, 0, len(plan.Categories.Delete))
	for _, name := range plan.Categories.Delete {
		key := models.RestoreOpKey(restoreDomainCategory, restoreActionDelete, name)
		proceed, err := hook.start(ctx, key)
		if err != nil {
			return err
		}
		if proceed {
			targets = append(targets, name)
		} else {
			hook.tick()
		}
	}
	if len(targets) == 0 {
		return nil
	}

	if err := s.categoryWriter.RemoveCategories(ctx, instanceID, targets); err != nil {
		log.Warn().Err(err).Int("instanceID", instanceID).Strs("categories", targets).Msg("Restore: batch category removal failed, retry individually")
		for _, name := range targets {
			key := models.RestoreOpKey(restoreDomainCategory, restoreActionDelete, name)
			err := s.categoryWriter.RemoveCategories(ctx, instanceID, []string{name})
			hook.finish(key, err)
			if err != nil {
				appendRestoreError(errs, "delete_category", name, err)
			} else {
				applied.Categories.Deleted = append(applied.Categories.Deleted, name)
			}
			hook.tick()
		}
		return nil
	}

	for _, name := range targets {
		hook.finish(models.RestoreOpKey(restoreDomainCategory, restoreActionDelete, name), nil)
		applied.Categories.Deleted = append(applied.Categories.Deleted, name)
		hook.tick()
	}
	return nil
}

func (s *Service) applyTagPlan(ctx context.Context, plan *RestorePlan, applied *RestoreApplied, errs *[]RestoreError, hook restoreHook) error {
	instanceID := plan.InstanceID

	if len(plan.Tags.Create) > 0 {
		tags := make([]string, 0, len(plan.Tags.Create))
		for _, spec := range plan.Tags.Create {
			key := models.RestoreOpKey(restoreDomainTag, restoreActionCreate, spec.Name)
			proceed, err := hook.start(ctx, key)
			if err != nil {
				return err
			}
			if proceed {
				tags = append(tags, spec.Name)
			} else {
				hook.tick()
			}
		}

		if len(tags) > 0 {
			if err := s.tagWriter.CreateTags(ctx, instanceID, tags); err != nil {
				log.Warn().Err(err).Int("instanceID", instanceID).Strs("tags", tags).Msg("Restore: batch tag creation failed, retry individually")
				for _, tag := range tags {
					key := models.RestoreOpKey(restoreDomainTag, restoreActionCreate, tag)
					err := s.tagWriter.CreateTags(ctx, instanceID, []string{tag})
					hook.finish(key, err)
					if err != nil {
						appendRestoreError(errs, "create_tag", tag, err)
					} else {
						applied.Tags.Created = append(applied.Tags.Created, tag)
					}
					hook.tick()
				}
			} else {
				for _, tag := range tags {
					hook.finish(models.RestoreOpKey(restoreDomainTag, restoreActionCreate, tag), nil)
					applied.Tags.Created = append(applied.Tags.Created, tag)
					hook.tick()
				}
			}
		}
	}

	if len(plan.Tags.Delete) == 0 {
		return nil
	}

	targets := make([]string, 0, len(plan.Tags.Delete))
	for _, tag := range plan.Tags.Delete {
		key := models.RestoreOpKey(restoreDomainTag, restoreActionDelete, tag)
		proceed, err := hook.start(ctx, key)
		if err != nil {
			return err
		}
		if proceed {
			targets = append(targets, tag)
		} else {
			hook.tick()
		}
	}
	if len(targets) == 0 {
		return nil
	}

	if err := s.tagWriter.DeleteTags(ctx, instanceID, targets); err != nil {
		log.Warn().Err(err).Int("instanceID", instanceID).Strs("tags", targets).Msg("Restore: batch tag deletion failed, retry individually")
		for _, tag := range targets {
			key := models.RestoreOpKey(restoreDomainTag, restoreActionDelete, tag)
			err := s.tagWriter.DeleteTags(ctx, instanceID, []string{tag})
			hook.finish(key, err)
			if err != nil {
				appendRestoreError(errs, "delete_tag", tag, err)
			} else {
				applied.Tags.Deleted = append(applied.Tags.Deleted, tag)
			}
			hook.tick()
		}
		return nil
	}

	for _, tag := range targets {
		hook.finish(models.RestoreOpKey(restoreDomainTag, restoreActionDelete, tag), nil)
		applied.Tags.Deleted = append(applied.Tags.Deleted, tag)
		hook.tick()
	}
	return nil
}

func (s *Service) applyTorrentPlan(ctx context.Context, plan *RestorePlan, applied *RestoreApplied, errs *[]RestoreError, exclude map[string]struct{}, opts RestoreOptions, hook restoreHook) ([]string, error) {
	instanceID := plan.InstanceID
	var warnings []string
	var pendingResume []string
	pinnedSavePaths := 0

	for _, spec := range plan.Torrents.Add {
		if shouldSkipTorrent(spec.Manifest.Hash, exclude) {
			continue
		}

		hash := spec.Manifest.Hash
		key := models.RestoreOpKey(restoreDomainTorrent, restoreActionAdd, hash)
		proceed, err := hook.start(ctx, key)
		if err != nil {
			return warnings, err
		}
		if !proceed {
			hook.tick()
			continue
		}

		blobPath := strings.TrimSpace(spec.Manifest.TorrentBlob)
		if blobPath == "" {
			blobErr := errors.New("missing torrent blob")
			hook.finish(key, blobErr)
			msg := fmt.Sprintf("Torrent %s missing cached blob reference", hash)
			warnings = append(warnings, msg)
			appendRestoreError(errs, "add_torrent", hash, blobErr)
			hook.tick()
			continue
		}

		payload, loadErr := s.loadTorrentBlobData(blobPath)
		if loadErr != nil {
			hook.finish(key, loadErr)
			appendRestoreError(errs, "add_torrent", hash, loadErr)
			log.Warn().Err(loadErr).Int("instanceID", instanceID).Str("hash", hash).Msg("Restore: failed to load torrent blob")
			hook.tick()
			continue
		}

		options := map[string]string{}
		paused := "false"
		stopped := "false"
		if opts.StartPaused || opts.SkipHashCheck {
			paused = "true"
			stopped = "true"
		}
		options["paused"] = paused
		options["stopped"] = stopped
		if opts.SkipHashCheck {
			options["skip_checking"] = "true"
		}
		categoryManaged := false
		if spec.Manifest.Category != nil {
			category := strings.TrimSpace(*spec.Manifest.Category)
			if category != "" {
				options["category"] = category
				categoryManaged = true
			}
		}
		if len(spec.Manifest.Tags) > 0 {
			options["tags"] = strings.Join(spec.Manifest.Tags, ",")
		}
		// Category and tags ride on the add. A post-add SetCategory raced the
		// sync cache and failed for a torrent qB had accepted (#2259).

		// Pin the captured per-torrent save path so torrents whose on-disk
		// location diverges from their category (cross-seed hardlinks, manual
		// relocations, Auto TMM off) land where their data already is. The path
		// is only present when capture decided it cannot be reproduced from the
		// category (see resolveBackupSavePath), so any non-empty value is pinned.
		// savepath-on-add (+autoTMM=false) places the torrent without moving
		// files; the path is passed verbatim because it is an opaque
		// qBittorrent-side path that may target a different host OS.
		pinned := false
		if savePath := strings.TrimSpace(spec.Manifest.SavePath); savePath != "" {
			options["autoTMM"] = "false"
			options["savepath"] = savePath
			pinned = true
		} else if categoryManaged {
			// A missing per-torrent path means capture proved the category path
			// can reproduce placement. qB does not select that path merely because
			// category is present on add; Auto TMM must be requested explicitly.
			options["autoTMM"] = "true"
		}

		_, addErr := s.torrentWriter.AddTorrent(ctx, instanceID, payload, options)
		hook.finish(key, addErr)
		if addErr != nil {
			appendRestoreError(errs, "add_torrent", hash, addErr)
			log.Warn().Err(addErr).Int("instanceID", instanceID).Str("hash", hash).Msg("Restore: add torrent failed")
			hook.tick()
			continue
		}
		if pinned {
			pinnedSavePaths++
		}

		applied.Torrents.Added = append(applied.Torrents.Added, hash)

		if opts.SkipHashCheck && opts.AutoResumeVerified {
			pendingResume = append(pendingResume, hash)
		}
		hook.tick()
	}

	if pinnedSavePaths > 0 {
		warnings = append(warnings, fmt.Sprintf("%d torrent(s) were placed at their saved path with Auto TMM disabled; ensure those locations exist on this host", pinnedSavePaths))
	}

	for _, update := range plan.Torrents.Update {
		if shouldSkipTorrent(update.Hash, exclude) {
			continue
		}

		supportedApplied := false

		for _, change := range update.Changes {
			if !change.Supported {
				message := change.Message
				if message == "" {
					message = "manual intervention required"
				}
				warnings = append(warnings, fmt.Sprintf("%s: %s (%s)", update.Hash, change.Field, message))
				continue
			}

			switch change.Field {
			case "category":
				key := models.RestoreOpKey(restoreDomainTorrent, restoreActionSetCategory, update.Hash)
				proceed, err := hook.start(ctx, key)
				if err != nil {
					return warnings, err
				}
				if proceed {
					desired := normalizeCategoryPtr(asString(change.Desired))
					err = s.torrentWriter.SetCategory(ctx, instanceID, []string{update.Hash}, desired)
					hook.finish(key, err)
					if err != nil {
						appendRestoreError(errs, "set_category", update.Hash, err)
					} else {
						supportedApplied = true
					}
				} else {
					supportedApplied = true
				}
				hook.tick()
			case "tags":
				key := models.RestoreOpKey(restoreDomainTorrent, restoreActionSetTags, update.Hash)
				proceed, err := hook.start(ctx, key)
				if err != nil {
					return warnings, err
				}
				if proceed {
					desiredTags := asStringSlice(change.Desired)
					tagPayload := strings.Join(desiredTags, ",")
					err = s.torrentWriter.SetTags(ctx, instanceID, []string{update.Hash}, tagPayload)
					hook.finish(key, err)
					if err != nil {
						appendRestoreError(errs, "set_tags", update.Hash, err)
					} else {
						supportedApplied = true
					}
				} else {
					supportedApplied = true
				}
				hook.tick()
			}
		}

		if supportedApplied {
			applied.Torrents.Updated = append(applied.Torrents.Updated, update.Hash)
		}
	}

	if len(pendingResume) > 0 && s.torrentWriter != nil {
		s.torrentWriter.ResumeWhenComplete(instanceID, pendingResume, qbittorrent.ResumeWhenCompleteOptions{})
	}

	if len(plan.Torrents.Delete) == 0 {
		return warnings, nil
	}

	deleteTargets := make([]string, 0, len(plan.Torrents.Delete))
	for _, hash := range plan.Torrents.Delete {
		if shouldSkipTorrent(hash, exclude) {
			continue
		}
		key := models.RestoreOpKey(restoreDomainTorrent, restoreActionDelete, hash)
		proceed, err := hook.start(ctx, key)
		if err != nil {
			return warnings, err
		}
		if proceed {
			deleteTargets = append(deleteTargets, hash)
		} else {
			hook.tick()
		}
	}

	if len(deleteTargets) == 0 {
		return warnings, nil
	}

	if err := s.torrentWriter.BulkAction(ctx, instanceID, deleteTargets, "delete"); err != nil {
		log.Warn().Err(err).Int("instanceID", instanceID).Strs("hashes", deleteTargets).Msg("Restore: bulk torrent delete failed, retry individually")
		for _, hash := range deleteTargets {
			key := models.RestoreOpKey(restoreDomainTorrent, restoreActionDelete, hash)
			err := s.torrentWriter.BulkAction(ctx, instanceID, []string{hash}, "delete")
			hook.finish(key, err)
			if err != nil {
				appendRestoreError(errs, "delete_torrent", hash, err)
			} else {
				applied.Torrents.Deleted = append(applied.Torrents.Deleted, hash)
			}
			hook.tick()
		}
		return warnings, nil
	}

	for _, hash := range deleteTargets {
		hook.finish(models.RestoreOpKey(restoreDomainTorrent, restoreActionDelete, hash), nil)
		applied.Torrents.Deleted = append(applied.Torrents.Deleted, hash)
		hook.tick()
	}
	return warnings, nil
}

// expandRestoreOps turns a plan into the ordered journal operations the
// executor will run. Order and keys must match the apply* functions so a resume
// can reconcile the journal with the freshly planned work.
func expandRestoreOps(plan *RestorePlan) []models.RestoreRunOp {
	ops := make([]models.RestoreRunOp, 0)
	seq := 0
	add := func(domain, action, target string) {
		ops = append(ops, models.RestoreRunOp{
			RestoreRunID: 0,
			Seq:          seq,
			Domain:       domain,
			Action:       action,
			Target:       target,
			Status:       models.RestoreOpPending,
		})
		seq++
	}

	for _, spec := range plan.Categories.Create {
		add(restoreDomainCategory, restoreActionCreate, spec.Name)
	}
	for _, update := range plan.Categories.Update {
		add(restoreDomainCategory, restoreActionUpdate, update.Name)
	}
	for _, name := range plan.Categories.Delete {
		add(restoreDomainCategory, restoreActionDelete, name)
	}
	for _, spec := range plan.Tags.Create {
		add(restoreDomainTag, restoreActionCreate, spec.Name)
	}
	for _, tag := range plan.Tags.Delete {
		add(restoreDomainTag, restoreActionDelete, tag)
	}
	for _, spec := range plan.Torrents.Add {
		add(restoreDomainTorrent, restoreActionAdd, spec.Manifest.Hash)
	}
	for _, update := range plan.Torrents.Update {
		for _, change := range update.Changes {
			if !change.Supported {
				continue
			}
			switch change.Field {
			case "category":
				add(restoreDomainTorrent, restoreActionSetCategory, update.Hash)
			case "tags":
				add(restoreDomainTorrent, restoreActionSetTags, update.Hash)
			}
		}
	}
	for _, hash := range plan.Torrents.Delete {
		add(restoreDomainTorrent, restoreActionDelete, hash)
	}

	return ops
}

func buildHashSet(items []string) map[string]struct{} {
	if len(items) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(items))
	for _, hash := range items {
		normalized := normalizeLowerTrim(hash)
		if normalized == "" {
			continue
		}
		set[normalized] = struct{}{}
	}
	if len(set) == 0 {
		return nil
	}
	return set
}

func shouldSkipTorrent(hash string, exclude map[string]struct{}) bool {
	if len(exclude) == 0 {
		return false
	}
	normalized := normalizeLowerTrim(hash)
	_, skip := exclude[normalized]
	return skip
}

func (s *Service) loadTorrentBlobData(blobPath string) ([]byte, error) {
	abs := s.ResolveBackupPath(blobPath)
	if abs == "" {
		return nil, fmt.Errorf("invalid torrent blob path %q", blobPath)
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		return nil, fmt.Errorf("read torrent blob %q: %w", blobPath, err)
	}
	return data, nil
}

func appendRestoreError(errs *[]RestoreError, operation, target string, err error) {
	if err == nil {
		return
	}
	*errs = append(*errs, RestoreError{
		Operation: operation,
		Target:    target,
		Message:   err.Error(),
	})
}

func asString(value any) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	case *string:
		if v == nil {
			return ""
		}
		return *v
	default:
		return fmt.Sprintf("%v", value)
	}
}

func asStringSlice(value any) []string {
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case []string:
		return cloneStringSlice(v)
	case *[]string:
		if v == nil {
			return nil
		}
		return cloneStringSlice(*v)
	case string:
		if strings.TrimSpace(v) == "" {
			return nil
		}
		parts := strings.Split(v, ",")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				out = append(out, trimmed)
			}
		}
		return out
	default:
		return nil
	}
}
