// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package update

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/creativeprojects/go-selfupdate"
	"github.com/stretchr/testify/require"
)

const (
	testCurrentVersion = "1.0.0"
	testNewVersion     = "1.2.3"
)

func TestNewSelfUpdater(t *testing.T) {
	updater, err := newSelfUpdater()
	require.NoError(t, err)
	require.NotNil(t, updater)
}

func TestReleaseValidatorValidatesSignedChecksums(t *testing.T) {
	certificate, privateKey := generateECDSACertificate(t)
	validator, err := newReleaseValidator(certificate)
	require.NoError(t, err)

	assetName := "qui_1.2.3_linux_x86_64.tar.gz"
	asset := []byte("release payload")
	checksums := fmt.Appendf(nil, "%x  %s\n", sha256.Sum256(asset), assetName)
	signature := signECDSA(t, privateKey, checksums)

	require.Equal(t, releaseChecksumsAsset, validator.GetValidationAssetName(assetName))
	require.Equal(t, releaseChecksumsAsset+".sig", validator.GetValidationAssetName(releaseChecksumsAsset))
	require.NoError(t, validator.Validate(assetName, asset, checksums))
	require.NoError(t, validator.Validate(releaseChecksumsAsset, checksums, signature))
}

func TestReleaseValidatorRejectsTamperedSignedChecksums(t *testing.T) {
	certificate, privateKey := generateECDSACertificate(t)
	validator, err := newReleaseValidator(certificate)
	require.NoError(t, err)

	checksums := []byte("deadbeef  qui_1.2.3_linux_x86_64.tar.gz\n")
	signature := signECDSA(t, privateKey, checksums)
	tamperedChecksums := []byte("feedface  qui_1.2.3_linux_x86_64.tar.gz\n")

	err = validator.Validate(releaseChecksumsAsset, tamperedChecksums, signature)
	require.Error(t, err)
}

func TestReleaseValidatorRejectsInvalidPublicKey(t *testing.T) {
	_, err := newReleaseValidator([]byte("not a public key"))
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Staged updater test harness
// ---------------------------------------------------------------------------

const (
	testArchiveID   = 100
	testChecksumsID = 200
	testSignatureID = 300
)

type fixture struct {
	t        *testing.T
	root     string
	target   string
	exeName  string
	backup   string
	stage    string
	certPEM  []byte
	certKey  *ecdsa.PrivateKey
	provider *fakeProvider

	assetName string
	archive   []byte
	checksums []byte
	newBinary []byte
	oldBinary []byte
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	root := t.TempDir()
	exeName := "qui"
	if runtime.GOOS == "windows" {
		exeName = "qui.exe"
	}

	f := &fixture{
		t:         t,
		root:      root,
		target:    filepath.Join(root, "bin", exeName),
		exeName:   exeName,
		backup:    filepath.Join(root, "backups"),
		stage:     filepath.Join(root, "stage"),
		newBinary: []byte("new binary contents for 1.2.3\n"),
		oldBinary: []byte("current binary contents at 1.0.0\n"),
	}
	f.assetName = fmt.Sprintf("qui_%s_%s_%s.tar.gz", testNewVersion, runtime.GOOS, runtime.GOARCH)

	require.NoError(t, os.MkdirAll(filepath.Dir(f.target), 0o755))
	require.NoError(t, os.MkdirAll(f.backup, 0o755))
	require.NoError(t, os.MkdirAll(f.stage, 0o755))
	require.NoError(t, os.WriteFile(f.target, f.oldBinary, 0o755))

	f.certPEM, f.certKey = generateECDSACertificate(t)
	f.archive = buildArchive(t, exeName, f.newBinary)
	f.checksums = fmt.Appendf(nil, "%x  %s\n", sha256.Sum256(f.archive), f.assetName)

	f.provider = &fakeProvider{
		found: true,
		cand:  f.candidate(),
		assets: map[int64][]byte{
			testArchiveID:   f.archive,
			testChecksumsID: f.checksums,
			testSignatureID: signECDSA(t, f.certKey, f.checksums),
		},
		calls: map[int64]int{},
	}

	return f
}

func (f *fixture) candidate() *candidate {
	return &candidate{
		version:     testNewVersion,
		assetID:     testArchiveID,
		assetName:   f.assetName,
		assetSize:   int64(len(f.archive)),
		checksumsID: testChecksumsID,
		signatureID: testSignatureID,
		os:          runtime.GOOS,
		arch:        runtime.GOARCH,
	}
}

func (f *fixture) config() Config {
	return Config{
		Repository: "autobrr/qui",
		Version:    testCurrentVersion,
		BackupDir:  f.backup,
		StageDir:   f.stage,
		MaxBackups: 3,
	}
}

func (f *fixture) updater() *Updater {
	u := NewUpdater(f.config())
	u.signingCert = f.certPEM
	u.newProvider = func() (releaseProvider, error) { return f.provider, nil }
	u.executablePath = func() (string, error) { return f.target, nil }
	u.freeBytes = func(string) (int64, error) { return 1 << 40, nil }
	u.runBinary = func(context.Context, string) (string, error) { return testNewVersion, nil }
	return u
}

func (f *fixture) targetBytes() []byte {
	b, err := os.ReadFile(f.target)
	require.NoError(f.t, err)
	return b
}

func (f *fixture) backupFiles() []string {
	entries, err := os.ReadDir(f.backup)
	require.NoError(f.t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), backupSuffix) {
			names = append(names, e.Name())
		}
	}
	return names
}

func (f *fixture) stageDirs() []string {
	entries, err := os.ReadDir(f.stage)
	require.NoError(f.t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func (f *fixture) assertTargetUntouched() {
	require.Equal(f.t, f.oldBinary, f.targetBytes())
}

func (f *fixture) assertTargetUpdated() {
	require.Equal(f.t, f.newBinary, f.targetBytes())
}

func (f *fixture) assertNoSwapLeftovers() {
	base := filepath.Base(f.target)
	dir := filepath.Dir(f.target)
	for _, suffix := range []string{".new", ".old"} {
		_, err := os.Stat(filepath.Join(dir, "."+base+suffix))
		require.True(f.t, os.IsNotExist(err), "leftover swap file %s", suffix)
	}
}

// buildArchive creates a gzipped tar holding one executable entry.
func buildArchive(t *testing.T, exeName string, payload []byte) []byte {
	t.Helper()

	var buf strings.Builder
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: exeName,
		Mode: 0o755,
		Size: int64(len(payload)),
	}))
	_, err := tw.Write(payload)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())
	return []byte(buf.String())
}

type fakeProvider struct {
	cand      *candidate
	found     bool
	latestErr error

	assets     map[int64][]byte
	assetErrs  map[int64]error
	truncateAt map[int64]int
	blocking   map[int64]bool

	mu    sync.Mutex
	calls map[int64]int
}

func (p *fakeProvider) latest(context.Context) (*candidate, bool, error) {
	return p.cand, p.found, p.latestErr
}

func (p *fakeProvider) downloadAsset(ctx context.Context, assetID int64) (io.ReadCloser, error) {
	p.mu.Lock()
	p.calls[assetID]++
	p.mu.Unlock()

	if err := p.assetErrs[assetID]; err != nil {
		return nil, err
	}
	data := p.assets[assetID]
	if p.blocking[assetID] {
		return io.NopCloser(&blockingReader{ctx: ctx}), nil
	}
	if n, ok := p.truncateAt[assetID]; ok {
		if n > len(data) {
			n = len(data)
		}
		return io.NopCloser(&errorAfterReader{data: data[:n]}), nil
	}
	return io.NopCloser(strings.NewReader(string(data))), nil
}

// errorAfterReader returns its data once and then a read error, simulating a
// connection drop mid-download.
type errorAfterReader struct {
	data []byte
	pos  int
}

func (r *errorAfterReader) Read(p []byte) (int, error) {
	if r.pos < len(r.data) {
		n := copy(p, r.data[r.pos:])
		r.pos += n
		return n, nil
	}
	return 0, io.ErrUnexpectedEOF
}

// blockingReader returns one byte then blocks until the context is cancelled.
type blockingReader struct {
	ctx   context.Context
	fired bool
}

func (r *blockingReader) Read(p []byte) (int, error) {
	if !r.fired {
		r.fired = true
		p[0] = 'x'
		return 1, nil
	}
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

// ---------------------------------------------------------------------------
// Staged flow tests
// ---------------------------------------------------------------------------

func TestUpdaterAppliesVerifiedCandidate(t *testing.T) {
	f := newFixture(t)
	u := f.updater()

	var progress []Progress
	var progressMu sync.Mutex

	result, err := u.Run(context.Background(), func(p Progress) {
		progressMu.Lock()
		progress = append(progress, p)
		progressMu.Unlock()
	})
	require.NoError(t, err)
	require.True(t, result.Updated)
	require.False(t, result.DryRun)
	require.Equal(t, testCurrentVersion, result.Report.CurrentVersion)
	require.Equal(t, testNewVersion, result.Report.CandidateVersion)

	f.assertTargetUpdated()
	f.assertNoSwapLeftovers()
	require.Empty(t, f.stageDirs(), "staging directory must be removed")

	backups := f.backupFiles()
	require.Len(t, backups, 1)
	backupBytes, err := os.ReadFile(filepath.Join(f.backup, backups[0]))
	require.NoError(t, err)
	require.Equal(t, f.oldBinary, backupBytes)

	require.NotEmpty(t, progress)
	final := progress[len(progress)-1]
	require.Equal(t, int64(len(f.archive)), final.BytesRead)
	require.Equal(t, int64(len(f.archive)), final.TotalSize)
}

func TestUpdaterAlreadyLatest(t *testing.T) {
	f := newFixture(t)
	f.provider.cand = nil
	u := f.updater()

	result, err := u.Run(context.Background(), nil)
	require.NoError(t, err)
	require.True(t, result.UpToDate)

	f.assertTargetUntouched()
	f.assertNoSwapLeftovers()
	require.Empty(t, f.backupFiles())
}

func TestUpdaterReleaseNotFound(t *testing.T) {
	f := newFixture(t)
	f.provider.found = false
	u := f.updater()

	_, err := u.Run(context.Background(), nil)
	require.Error(t, err)
	f.assertTargetUntouched()
}

func TestUpdaterRejectsInvalidCurrentVersion(t *testing.T) {
	f := newFixture(t)
	cfg := f.config()
	cfg.Version = "not-a-version"
	u := f.updater()
	u.config = cfg

	_, err := u.Run(context.Background(), nil)
	require.Error(t, err)
}

func TestUpdaterDownloadAssetFails(t *testing.T) {
	f := newFixture(t)
	f.provider.assetErrs = map[int64]error{testArchiveID: errors.New("connection refused")}
	u := f.updater()

	_, err := u.Run(context.Background(), nil)
	require.Error(t, err)

	f.assertTargetUntouched()
	f.assertNoSwapLeftovers()
	require.Empty(t, f.backupFiles())
	require.Empty(t, f.stageDirs())
}

func TestUpdaterTruncatedDownloadFails(t *testing.T) {
	f := newFixture(t)
	f.provider.truncateAt = map[int64]int{testArchiveID: 4}
	u := f.updater()

	_, err := u.Run(context.Background(), nil)
	require.Error(t, err)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	f.assertTargetUntouched()
	require.Empty(t, f.backupFiles())
	require.Empty(t, f.stageDirs())
}

func TestUpdaterRejectsBadSignature(t *testing.T) {
	f := newFixture(t)

	// Sign the checksums with a different key than the embedded candidate.
	_, otherKey := generateECDSACertificate(t)
	f.provider.assets[testSignatureID] = signECDSA(t, otherKey, f.checksums)
	u := f.updater()

	_, err := u.Run(context.Background(), nil)
	require.Error(t, err)
	require.ErrorIs(t, err, selfupdate.ErrECDSAValidationFailed)

	f.assertTargetUntouched()
	require.Empty(t, f.backupFiles())
	require.Empty(t, f.stageDirs())
}

func TestUpdaterRejectsChecksumMismatch(t *testing.T) {
	f := newFixture(t)

	tampered := make([]byte, len(f.archive))
	copy(tampered, f.archive)
	tampered[len(tampered)-1] ^= 0xff
	f.provider.assets[testArchiveID] = tampered
	u := f.updater()

	_, err := u.Run(context.Background(), nil)
	require.Error(t, err)
	require.ErrorIs(t, err, selfupdate.ErrChecksumValidationFailed)

	f.assertTargetUntouched()
	require.Empty(t, f.backupFiles())
	require.Empty(t, f.stageDirs())
}

func TestUpdaterInsufficientTargetSpace(t *testing.T) {
	f := newFixture(t)
	u := f.updater()
	u.freeBytes = func(path string) (int64, error) {
		if path == filepath.Dir(f.target) {
			return 1, nil
		}
		return 1 << 40, nil
	}

	_, err := u.Run(context.Background(), nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInsufficientSpace)

	f.assertTargetUntouched()
	require.Empty(t, f.backupFiles())
}

func TestUpdaterInsufficientBackupSpace(t *testing.T) {
	f := newFixture(t)
	u := f.updater()
	u.freeBytes = func(path string) (int64, error) {
		if path == f.backup {
			return 1, nil
		}
		return 1 << 40, nil
	}

	_, err := u.Run(context.Background(), nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInsufficientSpace)

	f.assertTargetUntouched()
}

func TestUpdaterTargetDirectoryNotWritable(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix permission model")
	}
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions")
	}

	f := newFixture(t)
	readOnly := filepath.Join(f.root, "readonly")
	require.NoError(t, os.MkdirAll(readOnly, 0o755))
	f.target = filepath.Join(readOnly, f.exeName)
	require.NoError(t, os.WriteFile(f.target, f.oldBinary, 0o755))
	require.NoError(t, os.Chmod(readOnly, 0o555))
	t.Cleanup(func() { _ = os.Chmod(readOnly, 0o755) })

	_, err := f.updater().Run(context.Background(), nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotWritable)

	require.Equal(t, f.oldBinary, f.targetBytes())
}

func TestUpdaterRollsBackWhenSwapFails(t *testing.T) {
	f := newFixture(t)
	u := f.updater()
	u.renameFile = func(oldpath, newpath string) error {
		if filepath.Base(oldpath) == "."+f.exeName+".new" && filepath.Base(newpath) == f.exeName {
			return errors.New("simulated rename failure")
		}
		return os.Rename(oldpath, newpath)
	}

	_, err := u.Run(context.Background(), nil)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "rollback")

	f.assertTargetUntouched()
	f.assertNoSwapLeftovers()

	// The persistent backup exists before the swap was attempted.
	require.Len(t, f.backupFiles(), 1)
}

func TestUpdaterRestoresBackupWhenBothRenamesFail(t *testing.T) {
	f := newFixture(t)
	u := f.updater()
	u.renameFile = func(oldpath, newpath string) error {
		// Fail every rename that places something at the target path.
		if filepath.Base(newpath) == f.exeName {
			return errors.New("simulated target rename failure")
		}
		return os.Rename(oldpath, newpath)
	}

	_, err := u.Run(context.Background(), nil)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "rollback also failed")

	// The rename rollback failed too, so the persistent backup copy is the
	// path that brought the original binary back.
	f.assertTargetUntouched()
	f.assertNoSwapLeftovers()
	require.Len(t, f.backupFiles(), 1)
}

func TestUpdaterRollbackErrorWhenRestoreImpossible(t *testing.T) {
	f := newFixture(t)
	u := f.updater()
	u.renameFile = func(oldpath, newpath string) error {
		if filepath.Base(newpath) == f.exeName {
			return errors.New("simulated target rename failure")
		}
		return os.Rename(oldpath, newpath)
	}
	u.copyFile = func(src, dst string, mode os.FileMode) error {
		if filepath.Base(dst) == f.exeName && strings.HasSuffix(src, backupSuffix) {
			return errors.New("simulated backup restore failure")
		}
		return copyBinary(src, dst, mode)
	}

	_, err := u.Run(context.Background(), nil)
	require.Error(t, err)
	var rollbackErr *RollbackError
	require.ErrorAs(t, err, &rollbackErr)
	require.ErrorContains(t, rollbackErr.RollbackErr, "simulated backup restore failure")

	// Nothing could be restored automatically; the target stays missing.
	_, statErr := os.Stat(f.target)
	require.True(t, os.IsNotExist(statErr))
}

func TestUpdaterRecoversInterruptedSwap(t *testing.T) {
	f := newFixture(t)
	old := filepath.Join(filepath.Dir(f.target), "."+f.exeName+".old")
	staged := filepath.Join(filepath.Dir(f.target), "."+f.exeName+".new")
	require.NoError(t, os.WriteFile(old, f.oldBinary, 0o755))
	require.NoError(t, os.WriteFile(staged, []byte("partial"), 0o755))
	require.NoError(t, os.Remove(f.target))

	result, err := f.updater().Run(context.Background(), nil)
	require.NoError(t, err)
	require.True(t, result.Updated)

	f.assertTargetUpdated()
	f.assertNoSwapLeftovers()
}

func TestUpdaterRemovesStaleSwapArtifactsWhenHealthy(t *testing.T) {
	f := newFixture(t)
	old := filepath.Join(filepath.Dir(f.target), "."+f.exeName+".old")
	staged := filepath.Join(filepath.Dir(f.target), "."+f.exeName+".new")
	require.NoError(t, os.WriteFile(old, []byte("ancient"), 0o755))
	require.NoError(t, os.WriteFile(staged, []byte("partial"), 0o755))

	_, err := f.updater().Run(context.Background(), nil)
	require.NoError(t, err)

	f.assertTargetUpdated()
	f.assertNoSwapLeftovers()
}

func TestUpdaterRestoresWhenPostReplaceHashMismatches(t *testing.T) {
	f := newFixture(t)
	u := f.updater()
	u.renameFile = func(oldpath, newpath string) error {
		// Simulate disk corruption of the target immediately after the swap.
		if err := os.Rename(oldpath, newpath); err != nil {
			return err
		}
		if filepath.Base(oldpath) == "."+f.exeName+".new" && filepath.Base(newpath) == f.exeName {
			return os.WriteFile(newpath, []byte("corrupted on disk"), 0o755)
		}
		return nil
	}

	_, err := u.Run(context.Background(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "hash")

	f.assertTargetUntouched()
	f.assertNoSwapLeftovers()
}

func TestUpdaterRestoresWhenRuntimeVersionMismatches(t *testing.T) {
	f := newFixture(t)
	u := f.updater()
	u.runBinary = func(context.Context, string) (string, error) { return "9.9.9", nil }

	_, err := u.Run(context.Background(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "reported version")

	f.assertTargetUntouched()
	f.assertNoSwapLeftovers()
}

func TestUpdaterRestoresWhenBinaryFailsToRun(t *testing.T) {
	f := newFixture(t)
	u := f.updater()
	u.runBinary = func(context.Context, string) (string, error) {
		return "", errors.New("exit status 1")
	}

	_, err := u.Run(context.Background(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to run")

	f.assertTargetUntouched()
}

func TestUpdaterCancelsDownload(t *testing.T) {
	f := newFixture(t)
	f.provider.blocking = map[int64]bool{testArchiveID: true}
	u := f.updater()

	ctx, cancel := context.WithCancel(context.Background())
	onProgress := func(p Progress) {
		if p.BytesRead > 0 {
			cancel()
		}
	}

	_, err := u.Run(ctx, onProgress)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)

	f.assertTargetUntouched()
	require.Empty(t, f.backupFiles())
	require.Empty(t, f.stageDirs())
}

func TestUpdaterDryRunDoesNotReplace(t *testing.T) {
	f := newFixture(t)
	cfg := f.config()
	cfg.DryRun = true
	u := f.updater()
	u.config = cfg

	result, err := u.Run(context.Background(), nil)
	require.NoError(t, err)
	require.True(t, result.DryRun)
	require.False(t, result.Updated)
	expectedHash := fmt.Sprintf("%x", sha256.Sum256(f.newBinary))
	require.Equal(t, expectedHash, result.Report.CandidateSHA256)
	require.NotEmpty(t, result.Report.CurrentSHA256)

	f.assertTargetUntouched()
	require.Empty(t, f.backupFiles())
	require.Empty(t, f.stageDirs())
}

func TestUpdaterPrunesOldBackups(t *testing.T) {
	f := newFixture(t)
	cfg := f.config()
	cfg.MaxBackups = 2
	u := f.updater()
	u.config = cfg

	preExisting := make([]string, 0, 5)
	baseTime := time.Now().Add(-24 * time.Hour)
	for i := range 5 {
		name := backupFileName(f.exeName, fmt.Sprintf("0.9.%d", i), int64(i))
		preExisting = append(preExisting, name)
		path := filepath.Join(f.backup, name)
		require.NoError(t, os.WriteFile(path, []byte("old"), 0o600))
		mt := baseTime.Add(time.Duration(i) * time.Hour)
		require.NoError(t, os.Chtimes(path, mt, mt))
	}
	require.NoError(t, os.WriteFile(filepath.Join(f.backup, "unrelated.txt"), []byte("keep"), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(f.backup, "keepdir"), 0o700))

	result, err := u.Run(context.Background(), nil)
	require.NoError(t, err)
	require.True(t, result.Updated)

	remaining := f.backupFiles()
	require.Len(t, remaining, 2)
	// The newest pre-existing backup plus the backup just created survive.
	require.Contains(t, remaining, preExisting[len(preExisting)-1])
	for _, gone := range preExisting[:4] {
		require.NotContains(t, remaining, gone)
	}

	_, err = os.Stat(filepath.Join(f.backup, "unrelated.txt"))
	require.NoError(t, err)
	info, err := os.Stat(filepath.Join(f.backup, "keepdir"))
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

// ---------------------------------------------------------------------------
// Small unit tests
// ---------------------------------------------------------------------------

func TestVerifyChecksum(t *testing.T) {
	asset := []byte("payload")
	line := fmt.Appendf(nil, "%x  asset.tar.gz\n", sha256.Sum256(asset))
	good := fmt.Sprintf("%x", sha256.Sum256(asset))

	require.NoError(t, verifyChecksum("asset.tar.gz", good, line))
	require.NoError(t, verifyChecksum("asset.tar.gz", good, bytesReplaceCRLF(line)))
	require.ErrorIs(t, verifyChecksum("missing.tar.gz", good, line), selfupdate.ErrHashNotFound)
	require.ErrorIs(t, verifyChecksum("asset.tar.gz", strings.Repeat("0", len(good)), line), selfupdate.ErrChecksumValidationFailed)
}

func bytesReplaceCRLF(in []byte) []byte {
	return []byte(strings.ReplaceAll(string(in), "\n", "\r\n"))
}

func TestEnsureSpace(t *testing.T) {
	require.NoError(t, ensureSpace(100, 100, 50, 50))
	require.ErrorIs(t, ensureSpace(40, 100, 50, 50), ErrInsufficientSpace)
	require.ErrorIs(t, ensureSpace(100, 40, 50, 50), ErrInsufficientSpace)
}

func TestBackupFileName(t *testing.T) {
	name := backupFileName("qui.exe", "1.2.3", 42)
	require.Equal(t, "qui_1.2.3_42.bak", name)
}

func TestVersionsEqual(t *testing.T) {
	require.True(t, versionsEqual("1.2.3\n", "v1.2.3"))
	require.True(t, versionsEqual("1.2.3", "1.2.3"))
	require.False(t, versionsEqual("1.2.4", "1.2.3"))
	require.False(t, versionsEqual("oops", "1.2.3"))
}

func TestFormatBytes(t *testing.T) {
	require.Equal(t, "512 B", formatBytes(512))
	require.Equal(t, "1.0 KiB", formatBytes(1024))
	require.Equal(t, "1.5 MiB", formatBytes(1024*1024+512*1024))
}

// ---------------------------------------------------------------------------
// Existing key helpers
// ---------------------------------------------------------------------------

func generateECDSACertificate(t *testing.T) ([]byte, *ecdsa.PrivateKey) {
	t.Helper()

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageCodeSigning},
	}

	certificateDER, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER}), privateKey
}

func signECDSA(t *testing.T, privateKey *ecdsa.PrivateKey, data []byte) []byte {
	t.Helper()

	digest := sha256.Sum256(data)
	signature, err := ecdsa.SignASN1(rand.Reader, privateKey, digest[:])
	require.NoError(t, err)

	return signature
}
