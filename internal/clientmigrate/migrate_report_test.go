package clientmigrate

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/autobrr/go-torrent/bencode"
	"github.com/autobrr/go-torrent/metainfo"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- fixtures -------------------------------------------------------------

const testPieceLength = int64(16384)

func numPiecesForLength(length int64) int {
	return int((length + testPieceLength - 1) / testPieceLength)
}

// buildTorrent writes a minimal v1 single-file .torrent and returns its id
func buildTorrent(t *testing.T, path, name string, length int64) (hash string, info metainfo.Info, mi metainfo.MetaInfo) {
	t.Helper()

	numPieces := numPiecesForLength(length)
	info = metainfo.Info{
		Name:        name,
		PieceLength: testPieceLength,
		Pieces:      make([]byte, numPieces*20),
		Length:      length,
	}
	infoBytes, err := bencode.Marshal(map[string]any{
		"name":         name,
		"piece length": testPieceLength,
		"pieces":       string(info.Pieces),
		"length":       length,
	})
	require.NoError(t, err)

	mi = metainfo.MetaInfo{InfoBytes: infoBytes}
	hash = mi.HashInfoBytes().HexString()

	f, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, mi.Write(f))
	require.NoError(t, f.Close())

	return hash, info, mi
}

// writeDelugeTorrent creates one source .torrent and returns its resume blob
func writeDelugeTorrent(t *testing.T, stateDir, saveDir, name string, length int64, complete bool) (string, string) {
	t.Helper()

	tmp := filepath.Join(stateDir, "."+name+".tmp")
	hash, info, _ := buildTorrent(t, tmp, name, length)
	// deluge names session files by infohash
	require.NoError(t, os.Rename(tmp, filepath.Join(stateDir, hash+".torrent")))

	pieces := strings.Repeat("\x00", info.NumPieces())
	if complete {
		pieces = strings.Repeat("\x01", info.NumPieces())
	}

	infoHash, err := hex.DecodeString(hash)
	require.NoError(t, err)

	fr := Fastresume{
		InfoHash:          infoHash,
		Pieces:            pieces,
		FilePriority:      []int{1},
		SavePath:          saveDir,
		LibTorrentVersion: "2.0.14.0",
	}
	blob, err := bencode.Marshal(&fr)
	require.NoError(t, err)

	return hash, string(blob)
}

func writeDelugeFastresume(t *testing.T, stateDir string, entries map[string]string) {
	t.Helper()
	outer := make(map[string]any, len(entries))
	for h, blob := range entries {
		outer[h] = blob
	}
	dat, err := bencode.Marshal(outer)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "torrents.fastresume"), dat, 0o600))
}

func readTestReport(t *testing.T, opts Options) *MigrationReport {
	t.Helper()
	dat, err := os.ReadFile(reportFilePath(opts))
	require.NoError(t, err)
	var report MigrationReport
	require.NoError(t, json.Unmarshal(dat, &report))
	return &report
}

func reportByHash(report *MigrationReport) map[string]ReportEntry {
	m := make(map[string]ReportEntry, len(report.Entries))
	for _, e := range report.Entries {
		m[e.InfoHash] = e
	}
	return m
}

func assertNoTempFiles(t *testing.T, dir string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.tmp-*"))
	require.NoError(t, err)
	assert.Empty(t, matches, "no staged temp files may remain in: %s", dir)
}

func noFileExists(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	assert.True(t, os.IsNotExist(err), "expected %s to not exist, got err=%v", path, err)
}

func bytesRepeating(n int, b byte) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

// ---- resume after interruption + failure retry + exit codes -----------

func TestDelugeMigrateResumeAndRetry(t *testing.T) {
	stateDir := t.TempDir()
	qbitDir := t.TempDir()
	saveDir := t.TempDir()

	h1, blob1 := writeDelugeTorrent(t, stateDir, saveDir, "First.Movie.2024.1080p-WEB", 1000, true)
	h2, blob2 := writeDelugeTorrent(t, stateDir, saveDir, "Second.Movie.2024.1080p-WEB", 2000, true)
	h3, blob3 := writeDelugeTorrent(t, stateDir, saveDir, "Third.Movie.2024.1080p-WEB", 3000, true)
	writeDelugeFastresume(t, stateDir, map[string]string{h1: blob1, h2: blob2, h3: blob3})

	opts := Options{Source: ClientTypeDeluge, SourceDir: stateDir, QbitDir: qbitDir}

	// run 1: committing the second torrent fails, like a disk error mid-run
	di := NewDelugeImporter(opts)
	di.commit = func(fr *Fastresume, info *metainfo.Info, infoHash []byte, torrentSrc, torrentID string) error {
		if torrentID == h2 {
			return errors.New("simulated write failure")
		}
		return defaultCommitArtifacts(opts.QbitDir, fr, info, infoHash, torrentSrc, torrentID)
	}
	err := di.Migrate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 failed torrent")

	// two artifacts installed, the failed one has nothing, no staged temp files
	for _, h := range []string{h1, h3} {
		require.FileExists(t, filepath.Join(qbitDir, h+".torrent"))
		require.FileExists(t, filepath.Join(qbitDir, h+".fastresume"))
	}
	noFileExists(t, filepath.Join(qbitDir, h2+".torrent"))
	noFileExists(t, filepath.Join(qbitDir, h2+".fastresume"))
	assertNoTempFiles(t, qbitDir)

	report := readTestReport(t, opts)
	imported, failed, skipped := report.tally()
	assert.Equal(t, 2, imported)
	assert.Equal(t, 1, failed)
	assert.Equal(t, 0, skipped)

	entries := reportByHash(report)
	assert.Equal(t, ResultImported, entries[h1].Result)
	assert.Equal(t, ResultImported, entries[h3].Result)
	assert.Equal(t, ResultFailed, entries[h2].Result)
	assert.Equal(t, 1, entries[h2].Attempts)
	assert.Contains(t, entries[h2].Reason, "simulated write failure")
	assert.NotEmpty(t, entries[h2].Name)

	// run 2: failure cleared; only h2 is retried, h1/h3 skipped by report
	require.NoError(t, NewDelugeImporter(opts).Migrate())
	require.FileExists(t, filepath.Join(qbitDir, h2+".torrent"))
	require.FileExists(t, filepath.Join(qbitDir, h2+".fastresume"))
	assertNoTempFiles(t, qbitDir)

	report = readTestReport(t, opts)
	imported, failed, skipped = report.tally()
	assert.Equal(t, 3, imported)
	assert.Equal(t, 0, failed)
	assert.Equal(t, 0, skipped)
	entries = reportByHash(report)
	assert.Equal(t, ResultImported, entries[h2].Result)
	assert.Equal(t, 2, entries[h2].Attempts, "the failed torrent was retried once")
	assert.Equal(t, 1, entries[h1].Attempts)
	assert.Equal(t, 1, entries[h3].Attempts)

	// run 3: nothing is written again; entries recorded imported are not retried
	require.NoError(t, NewDelugeImporter(opts).Migrate())
	report = readTestReport(t, opts)
	entries = reportByHash(report)
	assert.Equal(t, 1, entries[h1].Attempts)
	assert.Equal(t, 2, entries[h2].Attempts)
	assert.Equal(t, 1, entries[h3].Attempts)
}

func TestMigrateResumeIgnoresTruncatedArtifactsAndTemps(t *testing.T) {
	stateDir := t.TempDir()
	qbitDir := t.TempDir()
	saveDir := t.TempDir()

	h1, blob1 := writeDelugeTorrent(t, stateDir, saveDir, "Interrupted.Movie.2024.1080p-WEB", 1000, true)
	writeDelugeFastresume(t, stateDir, map[string]string{h1: blob1})

	// simulate a kill during the previous run: a truncated final fastresume
	// plus leftover staged temp files, and no report recording success
	require.NoError(t, os.WriteFile(filepath.Join(qbitDir, h1+".fastresume"), []byte("d12:garbage"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(qbitDir, h1+".fastresume.tmp-111"), []byte("partial"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(qbitDir, h1+".torrent.tmp-222"), []byte("partial"), 0o600))

	opts := Options{Source: ClientTypeDeluge, SourceDir: stateDir, QbitDir: qbitDir}
	require.NoError(t, NewDelugeImporter(opts).Migrate())

	// the truncated file is replaced with valid data, leftovers are removed
	dat, err := os.ReadFile(filepath.Join(qbitDir, h1+".fastresume"))
	require.NoError(t, err)
	var fr Fastresume
	require.NoError(t, bencode.NewDecoder(bytes.NewReader(dat)).Decode(&fr))
	require.FileExists(t, filepath.Join(qbitDir, h1+".torrent"))
	assertNoTempFiles(t, qbitDir)

	report := readTestReport(t, opts)
	entries := reportByHash(report)
	assert.Equal(t, ResultImported, entries[h1].Result)
}

// ---- resume verification -------------------------------------------------

func TestVerifyResumeData(t *testing.T) {
	t.Parallel()

	info := testInfo() // 3 pieces, files 30000 + 12000
	hash := bytesRepeating(20, 0xab)

	newGood := func() Fastresume {
		fr := Fastresume{
			InfoHash:     hash,
			Pieces:       "\x01\x01\x01",
			FilePriority: []int{1, 1},
		}
		fr.setFileMeta(&info)
		return fr
	}

	good := newGood()
	goodDat, err := bencode.Marshal(&good)
	require.NoError(t, err)
	require.NoError(t, verifyResumeData(goodDat, &info, hash))

	tests := []struct {
		name    string
		mutate  func(fr *Fastresume)
		wantSub string
		hash    []byte
	}{
		{
			name:    "infohash mismatch",
			mutate:  func(fr *Fastresume) { fr.InfoHash = bytesRepeating(20, 0xcd) },
			wantSub: "infohash",
			hash:    hash,
		},
		{
			name:    "piece length mismatch",
			mutate:  func(fr *Fastresume) { fr.PieceLength = 8192 },
			wantSub: "piece length",
			hash:    hash,
		},
		{
			name:    "piece count mismatch",
			mutate:  func(fr *Fastresume) { fr.Pieces = "\x01\x01" },
			wantSub: "piece count",
			hash:    hash,
		},
		{
			name:    "file priority count mismatch",
			mutate:  func(fr *Fastresume) { fr.FilePriority = []int{1} },
			wantSub: "file priority count",
			hash:    hash,
		},
		{
			name:    "file size mismatch",
			mutate:  func(fr *Fastresume) { fr.FileSizes[1][1] = "999" },
			wantSub: "size 999",
			hash:    hash,
		},
		{
			name:    "file list count mismatch",
			mutate:  func(fr *Fastresume) { fr.FileSizes = fr.FileSizes[:1] },
			wantSub: "file sizes count",
			hash:    hash,
		},
		{
			name:    "mapped files count mismatch",
			mutate:  func(fr *Fastresume) { fr.MappedFiles = []string{"only-one"} },
			wantSub: "mapped files count",
			hash:    hash,
		},
		{
			name:    "missing v1 infohash",
			mutate:  func(fr *Fastresume) {},
			wantSub: "v1 infohash",
			hash:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fr := newGood()
			tt.mutate(&fr)
			dat, err := bencode.Marshal(&fr)
			require.NoError(t, err)

			err = verifyResumeData(dat, &info, tt.hash)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantSub)
		})
	}

	t.Run("garbled bencode fails", func(t *testing.T) {
		t.Parallel()
		err := verifyResumeData([]byte("not bencode"), &info, hash)
		require.Error(t, err)
	})
}

func TestDelugeMigrateResumeVerificationFailure(t *testing.T) {
	stateDir := t.TempDir()
	qbitDir := t.TempDir()
	saveDir := t.TempDir()

	h1, blob1 := writeDelugeTorrent(t, stateDir, saveDir, "Bad.Resume.2024.1080p-WEB", 1000, true)
	writeDelugeFastresume(t, stateDir, map[string]string{h1: blob1})

	opts := Options{Source: ClientTypeDeluge, SourceDir: stateDir, QbitDir: qbitDir}
	di := NewDelugeImporter(opts)
	di.commit = func(fr *Fastresume, info *metainfo.Info, infoHash []byte, torrentSrc, torrentID string) error {
		fr.PieceLength++ // corrupt before staging
		return defaultCommitArtifacts(opts.QbitDir, fr, info, infoHash, torrentSrc, torrentID)
	}
	err := di.Migrate()
	require.Error(t, err)

	noFileExists(t, filepath.Join(qbitDir, h1+".fastresume"))
	noFileExists(t, filepath.Join(qbitDir, h1+".torrent"))
	assertNoTempFiles(t, qbitDir)

	report := readTestReport(t, opts)
	entries := reportByHash(report)
	assert.Equal(t, ResultFailed, entries[h1].Result)
	assert.Contains(t, entries[h1].Reason, "piece length")

	// the failed verification is retried successfully on the next run
	require.NoError(t, NewDelugeImporter(opts).Migrate())
	report = readTestReport(t, opts)
	entries = reportByHash(report)
	assert.Equal(t, ResultImported, entries[h1].Result)
}

// ---- atomic writes --------------------------------------------------------

func TestWriteFileAtomicReplacesAndLeavesNoTemp(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "resume.fastresume")

	require.NoError(t, writeFileAtomic(path, []byte("first"), 0o644))
	dat, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "first", string(dat))

	require.NoError(t, writeFileAtomic(path, []byte("second longer"), 0o600))
	dat, err = os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "second longer", string(dat))
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	assertNoTempFiles(t, filepath.Join(dir, "sub"))
}

func TestDefaultCommitCleansTempWhenSourceUnreadable(t *testing.T) {
	t.Parallel()

	qbitDir := t.TempDir()
	info := testInfo()
	fr := Fastresume{InfoHash: bytesRepeating(20, 1), Pieces: "\x01\x01\x01", FilePriority: []int{1, 1}}
	fr.setFileMeta(&info)

	err := defaultCommitArtifacts(qbitDir, &fr, &info, fr.InfoHash, filepath.Join(qbitDir, "missing.torrent"), "ab12")
	require.Error(t, err)
	assertNoTempFiles(t, qbitDir)
	noFileExists(t, filepath.Join(qbitDir, "ab12.fastresume"))
	noFileExists(t, filepath.Join(qbitDir, "ab12.torrent"))
}

// ---- exit codes -----------------------------------------------------------

func TestSkippedEntriesDoNotFailRun(t *testing.T) {
	stateDir := t.TempDir()
	qbitDir := t.TempDir()
	saveDir := t.TempDir()

	h1, blob1 := writeDelugeTorrent(t, stateDir, saveDir, "Incomplete.Movie.2024.1080p-WEB", 20000, false)
	writeDelugeFastresume(t, stateDir, map[string]string{h1: blob1})

	opts := Options{Source: ClientTypeDeluge, SourceDir: stateDir, QbitDir: qbitDir}
	require.NoError(t, NewDelugeImporter(opts).Migrate())

	report := readTestReport(t, opts)
	_, failed, skipped := report.tally()
	assert.Equal(t, 0, failed)
	assert.Equal(t, 1, skipped)
	noFileExists(t, filepath.Join(qbitDir, h1+".fastresume"))
}

func TestMigraterReturnsErrorOnFailedEntry(t *testing.T) {
	stateDir := t.TempDir()
	qbitDir := t.TempDir()

	// outer fastresume entry without a matching .torrent -> failed
	fr := Fastresume{Pieces: "\x01", FilePriority: []int{1}, SavePath: t.TempDir()}
	blob, err := bencode.Marshal(&fr)
	require.NoError(t, err)
	outer, err := bencode.Marshal(map[string]any{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": string(blob)})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "torrents.fastresume"), outer, 0o600))

	opts := Options{Source: ClientTypeDeluge, SourceDir: stateDir, QbitDir: qbitDir, SkipBackup: true}
	err = New(opts).Migrate(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed torrent")
}

func TestCorruptPreviousReportFailsRun(t *testing.T) {
	stateDir := t.TempDir()
	qbitDir := t.TempDir()
	saveDir := t.TempDir()

	h1, blob1 := writeDelugeTorrent(t, stateDir, saveDir, "Whatever.2024.1080p-WEB", 1000, true)
	writeDelugeFastresume(t, stateDir, map[string]string{h1: blob1})

	opts := Options{Source: ClientTypeDeluge, SourceDir: stateDir, QbitDir: qbitDir}
	require.NoError(t, os.WriteFile(reportFilePath(opts), []byte("{not json"), 0o600))

	err := NewDelugeImporter(opts).Migrate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "previous migration report")
}

// ---- dry run ---------------------------------------------------------------

func TestDelugeDryRunReport(t *testing.T) {
	stateDir := t.TempDir()
	qbitDir := t.TempDir()
	saveDir := t.TempDir()

	h1, blob1 := writeDelugeTorrent(t, stateDir, saveDir, "Importable.2024.1080p-WEB", 1000, true)
	h2, blob2 := writeDelugeTorrent(t, stateDir, saveDir, "Incomplete.2024.1080p-WEB", 20000, false)
	// h3 has resume state but its .torrent was removed from the source
	h3, blob3 := writeDelugeTorrent(t, stateDir, saveDir, "Ghost.2024.1080p-WEB", 1000, true)
	require.NoError(t, os.Remove(filepath.Join(stateDir, h3+".torrent")))
	writeDelugeFastresume(t, stateDir, map[string]string{h1: blob1, h2: blob2, h3: blob3})

	dryOpts := Options{Source: ClientTypeDeluge, SourceDir: stateDir, QbitDir: qbitDir, DryRun: true}

	// failed entries make even a dry run exit non-zero
	require.Error(t, NewDelugeImporter(dryOpts).Migrate())

	// no artifacts were written, but a categorized dry-run report exists
	matches, err := filepath.Glob(filepath.Join(qbitDir, "*.torrent"))
	require.NoError(t, err)
	assert.Empty(t, matches)
	frMatches, err := filepath.Glob(filepath.Join(qbitDir, "*.fastresume"))
	require.NoError(t, err)
	assert.Empty(t, frMatches)

	dryReport := readTestReport(t, dryOpts)
	assert.True(t, dryReport.DryRun)
	imported, failed, skipped := dryReport.tally()
	assert.Equal(t, 1, imported)
	assert.Equal(t, 1, failed)
	assert.Equal(t, 1, skipped)

	// the real report must not exist: a dry run cannot poison resume state
	realOpts := Options{Source: ClientTypeDeluge, SourceDir: stateDir, QbitDir: qbitDir}
	noFileExists(t, reportFilePath(realOpts))

	// a real run after the dry run starts fresh: h1 is imported, not skipped
	h3Again, _ := writeDelugeTorrent(t, stateDir, saveDir, "Ghost.2024.1080p-WEB", 1000, true)
	require.Equal(t, h3, h3Again)
	require.NoError(t, NewDelugeImporter(realOpts).Migrate())
	report := readTestReport(t, realOpts)
	assert.False(t, report.DryRun)
	entries := reportByHash(report)
	assert.Equal(t, ResultImported, entries[h1].Result)
	assert.Equal(t, 1, entries[h1].Attempts, "dry-run report must not be treated as a previous successful run")
	assert.Equal(t, ResultSkipped, entries[h2].Result)
	assert.Equal(t, ResultImported, entries[h3].Result)
}

// ---- rtorrent + transmission end-to-end resume -----------------------------

func writeRTorrentSession(t *testing.T, sessionDir, saveDir, name string, length int64) string {
	t.Helper()

	tmp := filepath.Join(sessionDir, "."+name+".tmp")
	hash, info, _ := buildTorrent(t, tmp, name, length)
	torrentPath := filepath.Join(sessionDir, hash+".torrent")
	require.NoError(t, os.Rename(tmp, torrentPath))

	numPieces := info.NumPieces()
	resume := map[string]any{
		"bitfield": int64(numPieces),
		"files": []map[string]int64{
			{"completed": length, "mtime": 0, "priority": 1},
		},
	}
	resumeDat, err := bencode.Marshal(resume)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(torrentPath+libtorrentStateFileExtension, resumeDat, 0o600))

	rtDat, err := bencode.Marshal(map[string]any{
		"complete":         int64(1),
		"directory":        saveDir,
		"state":            int64(1),
		"total_downloaded": length,
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(torrentPath+rtStateFileExtension, rtDat, 0o600))

	return hash
}

func TestRTorrentMigrateResumesFromReport(t *testing.T) {
	sessionDir := t.TempDir()
	qbitDir := t.TempDir()
	saveDir := t.TempDir()

	h1 := writeRTorrentSession(t, sessionDir, saveDir, "RT.Movie.2024.1080p-WEB", 1000)
	opts := Options{Source: ClientTypeRTorrent, SourceDir: sessionDir, QbitDir: qbitDir}

	require.NoError(t, NewRTorrentImporter(opts).Migrate())
	require.FileExists(t, filepath.Join(qbitDir, h1+".fastresume"))
	require.FileExists(t, filepath.Join(qbitDir, h1+".torrent"))

	// second run resumes from the report and rewrites nothing
	require.NoError(t, NewRTorrentImporter(opts).Migrate())
	report := readTestReport(t, opts)
	entries := reportByHash(report)
	assert.Equal(t, ResultImported, entries[h1].Result)
	assert.Equal(t, 1, entries[h1].Attempts)
}

func writeTransmissionSession(t *testing.T, baseDir, saveDir, name string, length int64) string {
	t.Helper()

	torrentsDir := filepath.Join(baseDir, "torrents")
	resumeDir := filepath.Join(baseDir, "resume")
	require.NoError(t, os.MkdirAll(torrentsDir, 0o700))
	require.NoError(t, os.MkdirAll(resumeDir, 0o700))

	hash, _, _ := buildTorrent(t, filepath.Join(torrentsDir, name+".torrent"), name, length)

	resume := TransmissionResumeFile{
		Destination: saveDir,
		Progress:    TransmissionResumeFileProgress{Blocks: "all"},
	}
	dat, err := bencode.Marshal(&resume)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(resumeDir, name+".resume"), dat, 0o600))

	return hash
}

func TestTransmissionMigrateResumesFromReport(t *testing.T) {
	baseDir := t.TempDir()
	qbitDir := t.TempDir()
	saveDir := t.TempDir()

	h1 := writeTransmissionSession(t, baseDir, saveDir, "TM.Movie.2024.1080p-WEB", 1000)
	opts := Options{Source: ClientTypeTransmission, SourceDir: baseDir, QbitDir: qbitDir}

	require.NoError(t, NewTransmissionImporter(opts).Migrate())
	require.FileExists(t, filepath.Join(qbitDir, h1+".fastresume"))
	require.FileExists(t, filepath.Join(qbitDir, h1+".torrent"))

	// second run resumes from the report and rewrites nothing
	require.NoError(t, NewTransmissionImporter(opts).Migrate())
	report := readTestReport(t, opts)
	entries := reportByHash(report)
	assert.Equal(t, ResultImported, entries[h1].Result)
	assert.Equal(t, 1, entries[h1].Attempts)
}
