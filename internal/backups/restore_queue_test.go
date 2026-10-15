// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package backups

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	qbt "github.com/autobrr/go-qbittorrent"
	"github.com/stretchr/testify/require"

	"github.com/autobrr/qui/internal/database"
	"github.com/autobrr/qui/internal/models"
	"github.com/autobrr/qui/internal/qbittorrent"
)

// scriptedManager implements the backup reader and all restore writer
// interfaces with scriptable failures and a blocking add gate.
type scriptedManager struct {
	stubBackupSyncManager
	mu sync.Mutex

	createCategoryErr   error
	createCategoryCalls []string
	editCategoryCalls   []string
	createTagCalls      []string
	deleteTagCalls      []string

	addInstances []int
	addCalls     int
	addErr       error

	blockAdd   bool
	addStarted chan struct{}
	addProceed chan struct{}

	bulkFailOnce bool
	bulkCalls    int
}

func (m *scriptedManager) CreateCategory(_ context.Context, _ int, name, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.createCategoryCalls = append(m.createCategoryCalls, name)
	return m.createCategoryErr
}

func (m *scriptedManager) EditCategory(_ context.Context, _ int, name, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.editCategoryCalls = append(m.editCategoryCalls, name)
	return m.createCategoryErr
}

func (m *scriptedManager) RemoveCategories(context.Context, int, []string) error { return nil }

func (m *scriptedManager) CreateTags(_ context.Context, _ int, tags []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.createTagCalls = append(m.createTagCalls, tags...)
	return nil
}

func (m *scriptedManager) DeleteTags(_ context.Context, _ int, tags []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteTagCalls = append(m.deleteTagCalls, tags...)
	return nil
}

func (m *scriptedManager) AddTorrent(ctx context.Context, instanceID int, _ []byte, _ map[string]string) (*qbt.TorrentAddResponse, error) {
	m.mu.Lock()
	block := m.blockAdd
	m.addInstances = append(m.addInstances, instanceID)
	addErr := m.addErr
	m.mu.Unlock()

	if block {
		select {
		case m.addStarted <- struct{}{}:
		default:
		}
		select {
		case <-m.addProceed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	m.mu.Lock()
	m.addCalls++
	m.mu.Unlock()
	return nil, addErr
}

func (m *scriptedManager) SetCategory(context.Context, int, []string, string) error { return nil }
func (m *scriptedManager) SetTags(context.Context, int, []string, string) error     { return nil }
func (m *scriptedManager) ResumeWhenComplete(int, []string, qbittorrent.ResumeWhenCompleteOptions) {
}

func (m *scriptedManager) BulkAction(_ context.Context, _ int, _ []string, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bulkCalls++
	if m.bulkFailOnce {
		m.bulkFailOnce = false
		return context.DeadlineExceeded
	}
	return nil
}

func (m *scriptedManager) snapshot() (createdCats, createdTags, deletedTags []string, addInstances []int, bulkCalls int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.createCategoryCalls...),
		append([]string(nil), m.createTagCalls...),
		append([]string(nil), m.deleteTagCalls...),
		append([]int(nil), m.addInstances...),
		m.bulkCalls
}

func (m *scriptedManager) blockNextAdd() {
	m.mu.Lock()
	m.blockAdd = true
	m.addStarted = make(chan struct{}, 1)
	m.addProceed = make(chan struct{})
	m.mu.Unlock()
}

func (m *scriptedManager) waitAddBlocked(t *testing.T) {
	t.Helper()
	select {
	case <-m.addStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for add to block")
	}
}

func (m *scriptedManager) releaseAdd() {
	m.mu.Lock()
	m.blockAdd = false
	ch := m.addProceed
	m.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

func seedSnapshotRun(t *testing.T, svc *Service, store *models.BackupStore, instanceID int, withTorrent bool) int64 {
	t.Helper()
	ctx := context.Background()

	blobRel := filepath.ToSlash(filepath.Join("backups", "torrents", "aa", "bb", "cc", "aaaa1111.torrent"))
	if withTorrent {
		abs := svc.ResolveBackupPath(blobRel)
		require.NotEmpty(t, abs)
		require.NoError(t, os.MkdirAll(filepath.Dir(abs), 0o755))
		require.NoError(t, os.WriteFile(abs, []byte("torrent-bytes"), 0o600))
	}

	categories := map[string]models.CategorySnapshot{}
	var tags []string
	if withTorrent {
		categories["movies"] = models.CategorySnapshot{}
		tags = []string{"tag-a"}
	}

	run := &models.BackupRun{
		InstanceID:  instanceID,
		Kind:        models.BackupRunKindManual,
		Status:      models.BackupRunStatusSuccess,
		RequestedBy: "test",
		RequestedAt: time.Now().UTC(),
		Categories:  categories,
		Tags:        tags,
	}
	require.NoError(t, store.CreateRun(ctx, run))

	if withTorrent {
		cat := "movies"
		joinedTags := "tag-a"
		blob := blobRel
		items := []models.BackupItem{{
			RunID:           run.ID,
			TorrentHash:     "aaaa1111",
			Name:            "Movie Title",
			Category:        &cat,
			Tags:            &joinedTags,
			TorrentBlobPath: &blob,
		}}
		require.NoError(t, store.InsertItems(ctx, run.ID, items))
	}

	return run.ID
}

func setupRestoreQueueTest(t *testing.T, mgr *scriptedManager, instanceName string) (*Service, *models.BackupStore, *database.DB, int, int64) {
	t.Helper()

	db := setupTestBackupDB(t)
	instanceID := insertTestInstance(t, db, instanceName)
	store := models.NewBackupStore(db)

	svc := NewService(store, mgr, Config{WorkerCount: 1, DataDir: t.TempDir()}, nil)
	require.NotNil(t, svc.restoreStore)
	svc.Start(context.Background())
	t.Cleanup(svc.Stop)

	backupRunID := seedSnapshotRun(t, svc, store, instanceID, true)
	return svc, store, db, instanceID, backupRunID
}

func awaitRestore(t *testing.T, svc *Service, id int64) *models.RestoreRun {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, err := svc.GetRestoreRun(context.Background(), id)
		require.NoError(t, err)
		switch run.Status {
		case models.RestoreStatusSuccess, models.RestoreStatusFailed, models.RestoreStatusCanceled, models.RestoreStatusInterrupted:
			return run
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("restore run %d did not reach a terminal state", id)
	return nil
}

func restoreOpts() RestoreOptions {
	return RestoreOptions{}
}

func TestRestoreQueueReusesIdenticalRequestAndRejectsDifferent(t *testing.T) {
	t.Parallel()

	mgr := &scriptedManager{}
	svc, _, _, instanceID, backupRunID := setupRestoreQueueTest(t, mgr, "reuse")
	mgr.blockNextAdd()

	first, reused, err := svc.QueueRestoreRun(context.Background(), RestoreRequest{
		InstanceID: instanceID, BackupRunID: backupRunID, Mode: RestoreModeIncremental, Options: restoreOpts(),
	})
	require.NoError(t, err)
	require.False(t, reused)
	mgr.waitAddBlocked(t)

	identical, reused, err := svc.QueueRestoreRun(context.Background(), RestoreRequest{
		InstanceID: instanceID, BackupRunID: backupRunID, Mode: RestoreModeIncremental, Options: restoreOpts(),
	})
	require.NoError(t, err)
	require.True(t, reused, "identical active request must reuse the run")
	require.Equal(t, first.ID, identical.ID)

	_, _, err = svc.QueueRestoreRun(context.Background(), RestoreRequest{
		InstanceID: instanceID, BackupRunID: backupRunID, Mode: RestoreModeOverwrite, Options: restoreOpts(),
	})
	require.ErrorIs(t, err, ErrRestoreInstanceBusy, "different mode while busy must be rejected")

	mgr.releaseAdd()
	run := awaitRestore(t, svc, first.ID)
	require.Equal(t, models.RestoreStatusSuccess, run.Status)
}

func TestRestoreQueueSerializesAcrossInstancesAndCancelsQueued(t *testing.T) {
	t.Parallel()

	mgr := &scriptedManager{}
	svc, store, db, instanceID, backupRunID := setupRestoreQueueTest(t, mgr, "serial-a")

	// Second instance and its own snapshot.
	secondID := insertTestInstance(t, db, "serial-b")
	secondBackupID := seedSnapshotRun(t, svc, store, secondID, true)

	mgr.blockNextAdd()
	first, _, err := svc.QueueRestoreRun(context.Background(), RestoreRequest{
		InstanceID: instanceID, BackupRunID: backupRunID, Mode: RestoreModeIncremental,
	})
	require.NoError(t, err)
	mgr.waitAddBlocked(t)

	// While instance 1 runs, instance 2's restore is accepted but stays queued.
	second, _, err := svc.QueueRestoreRun(context.Background(), RestoreRequest{
		InstanceID: secondID, BackupRunID: secondBackupID, Mode: RestoreModeIncremental,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		r, err := svc.GetRestoreRun(context.Background(), second.ID)
		return err == nil && r.Status == models.RestoreStatusQueued
	}, 2*time.Second, 5*time.Millisecond)

	_, err = svc.CancelRestoreRun(context.Background(), second.ID)
	require.NoError(t, err)

	mgr.releaseAdd()
	awaitRestore(t, svc, first.ID)
	canceled := awaitRestore(t, svc, second.ID)
	require.Equal(t, models.RestoreStatusCanceled, canceled.Status)

	_, _, _, addInstances, _ := mgr.snapshot()
	require.Equal(t, []int{instanceID}, addInstances, "canceled queued restore must never touch its instance")
}

func TestRestoreQueueCancelsRunningAtSafeBoundary(t *testing.T) {
	t.Parallel()

	mgr := &scriptedManager{}
	svc, _, _, instanceID, backupRunID := setupRestoreQueueTest(t, mgr, "cancel-run")
	mgr.blockNextAdd()

	run, _, err := svc.QueueRestoreRun(context.Background(), RestoreRequest{
		InstanceID: instanceID, BackupRunID: backupRunID, Mode: RestoreModeIncremental,
	})
	require.NoError(t, err)
	mgr.waitAddBlocked(t)

	_, err = svc.CancelRestoreRun(context.Background(), run.ID)
	require.NoError(t, err)

	final := awaitRestore(t, svc, run.ID)
	require.Equal(t, models.RestoreStatusCanceled, final.Status)
	require.True(t, final.CancelRequested)
}

func TestRestoreAllThreeModesAndDryRun(t *testing.T) {
	t.Parallel()

	mgr := &scriptedManager{}
	svc, _, _, instanceID, backupRunID := setupRestoreQueueTest(t, mgr, "modes")

	// Dry run plans but mutates nothing.
	dry, _, err := svc.QueueRestoreRun(context.Background(), RestoreRequest{
		InstanceID: instanceID, BackupRunID: backupRunID, Mode: RestoreModeIncremental,
		Options: RestoreOptions{DryRun: true},
	})
	require.NoError(t, err)
	dryRun := awaitRestore(t, svc, dry.ID)
	require.Equal(t, models.RestoreStatusSuccess, dryRun.Status)
	require.NotNil(t, dryRun.PlanJSON)
	require.NotEmpty(t, *dryRun.PlanJSON)
	createdCats, _, _, addInstances, _ := mgr.snapshot()
	require.Empty(t, createdCats)
	require.Empty(t, addInstances)

	for _, mode := range []RestoreMode{RestoreModeIncremental, RestoreModeOverwrite, RestoreModeComplete} {
		run, _, err := svc.QueueRestoreRun(context.Background(), RestoreRequest{
			InstanceID: instanceID, BackupRunID: backupRunID, Mode: mode,
		})
		require.NoError(t, err)
		final := awaitRestore(t, svc, run.ID)
		require.Equal(t, models.RestoreStatusSuccess, final.Status, "mode %s should succeed", mode)
	}
}

func TestRestoreOperationFailureRecordedThenResumeRetries(t *testing.T) {
	t.Parallel()

	mgr := &scriptedManager{createCategoryErr: context.DeadlineExceeded}
	svc, _, _, instanceID, backupRunID := setupRestoreQueueTest(t, mgr, "op-fail")

	run, _, err := svc.QueueRestoreRun(context.Background(), RestoreRequest{
		InstanceID: instanceID, BackupRunID: backupRunID, Mode: RestoreModeIncremental,
	})
	require.NoError(t, err)

	failed := awaitRestore(t, svc, run.ID)
	require.Equal(t, models.RestoreStatusFailed, failed.Status)
	require.NotNil(t, failed.Error)

	ops, err := svc.ListRestoreOps(context.Background(), run.ID)
	require.NoError(t, err)
	var categoryOp *models.RestoreRunOp
	for _, op := range ops {
		if op.Domain == "category" && op.Action == "create" {
			categoryOp = op
		}
	}
	require.NotNil(t, categoryOp)
	require.Equal(t, models.RestoreOpFailed, categoryOp.Status)

	// Clear the fault and resume; the failed category op replays and succeeds.
	mgr.mu.Lock()
	mgr.createCategoryErr = nil
	mgr.mu.Unlock()

	resumed, err := svc.ResumeRestoreRun(context.Background(), run.ID)
	require.NoError(t, err)
	require.Equal(t, models.RestoreStatusQueued, resumed.Status)

	final := awaitRestore(t, svc, run.ID)
	require.Equal(t, models.RestoreStatusSuccess, final.Status)
	createdCats, _, _, _, _ := mgr.snapshot()
	require.Contains(t, createdCats, "movies")
}

func TestRestoreBulkDeleteFallsBackPerTarget(t *testing.T) {
	t.Parallel()

	mgr := &scriptedManager{bulkFailOnce: true}
	// Live instance holds an extra torrent and tag absent from an empty snapshot.
	mgr.torrents = []qbt.Torrent{{Hash: "dead0000", Name: "Gone"}}
	mgr.tags = []string{"junk"}

	db := setupTestBackupDB(t)
	instanceID := insertTestInstance(t, db, "bulk-delete")
	store := models.NewBackupStore(db)
	svc := NewService(store, mgr, Config{WorkerCount: 1, DataDir: t.TempDir()}, nil)
	svc.Start(context.Background())
	t.Cleanup(svc.Stop)

	emptySnapshot := seedSnapshotRun(t, svc, store, instanceID, false)

	run, _, err := svc.QueueRestoreRun(context.Background(), RestoreRequest{
		InstanceID: instanceID, BackupRunID: emptySnapshot, Mode: RestoreModeComplete,
	})
	require.NoError(t, err)

	final := awaitRestore(t, svc, run.ID)
	require.Equal(t, models.RestoreStatusSuccess, final.Status)

	_, _, deletedTags, _, bulkCalls := mgr.snapshot()
	require.Contains(t, deletedTags, "junk")
	require.GreaterOrEqual(t, bulkCalls, 2, "failed bulk delete must retry each hash individually")

	ops, err := svc.ListRestoreOps(context.Background(), run.ID)
	require.NoError(t, err)
	var deleteOp *models.RestoreRunOp
	for _, op := range ops {
		if op.Domain == "torrent" && op.Action == "delete" && op.Target == "dead0000" {
			deleteOp = op
		}
	}
	require.NotNil(t, deleteOp)
	require.Equal(t, models.RestoreOpSucceeded, deleteOp.Status)
}

func TestRestoreRecoveryMarksInterruptedAndSkipsDamaged(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mgr := &scriptedManager{}
	db := setupTestBackupDB(t)
	instanceID := insertTestInstance(t, db, "recovery")
	store := models.NewBackupStore(db)
	svc := NewService(store, mgr, Config{WorkerCount: 1, DataDir: t.TempDir()}, nil)
	backupRunID := seedSnapshotRun(t, svc, store, instanceID, true)

	// A running restore with one completed category op and one pending tag op.
	res, err := db.ExecContext(ctx, `
		INSERT INTO instance_restore_runs
			(instance_id, backup_run_id, mode, dry_run, options_json, status, requested_by, requested_at)
		VALUES (?, ?, 'incremental', 0, '{}', 'running', 'test', ?)
	`, instanceID, backupRunID, time.Now().UTC())
	require.NoError(t, err)
	interruptedID, err := res.LastInsertId()
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		INSERT INTO instance_restore_run_ops (restore_run_id, seq, domain, action, target, status)
		VALUES (?, 0, 'category', 'create', 'movies', 'succeeded'),
		       (?, 1, 'tag', 'create', 'tag-a', 'pending')
	`, interruptedID, interruptedID)
	require.NoError(t, err)

	// A damaged run with unreadable options.
	res, err = db.ExecContext(ctx, `
		INSERT INTO instance_restore_runs
			(instance_id, backup_run_id, mode, dry_run, options_json, status, requested_by, requested_at)
		VALUES (?, ?, 'incremental', 0, '{not-json', 'running', 'test', ?)
	`, instanceID, backupRunID, time.Now().UTC())
	require.NoError(t, err)
	damagedID, err := res.LastInsertId()
	require.NoError(t, err)

	svc.Start(ctx)
	t.Cleanup(svc.Stop)

	require.Eventually(t, func() bool {
		r, err := svc.GetRestoreRun(ctx, interruptedID)
		return err == nil && r.Status == models.RestoreStatusInterrupted
	}, 2*time.Second, 5*time.Millisecond)

	damaged, _, err := svc.restoreStore.GetRunRawOptions(ctx, damagedID)
	require.NoError(t, err)
	require.Equal(t, models.RestoreStatusFailed, damaged.Status)
	require.NotNil(t, damaged.Error)

	// Resume: the already-succeeded category op must never be replayed even
	// though the fresh plan still requests it (live state is empty).
	resumed, err := svc.ResumeRestoreRun(ctx, interruptedID)
	require.NoError(t, err)
	require.Equal(t, models.RestoreStatusQueued, resumed.Status)

	final := awaitRestore(t, svc, interruptedID)
	require.Equal(t, models.RestoreStatusSuccess, final.Status)

	createdCats, createdTags, _, addInstances, _ := mgr.snapshot()
	require.NotContains(t, createdCats, "movies", "succeeded op must not be replayed")
	require.Contains(t, createdTags, "tag-a", "pending op must be replayed")
	require.Equal(t, []int{instanceID}, addInstances)
}
