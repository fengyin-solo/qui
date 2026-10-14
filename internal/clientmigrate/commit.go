package clientmigrate

import (
	"os"
	"path/filepath"

	"github.com/autobrr/go-torrent/bencode"
	"github.com/autobrr/go-torrent/metainfo"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// tempPatternSuffix makes staged files recognizable so a crashed run can
// never mistake a leftover temp file for a completed artifact
const tempPatternSuffix = ".tmp-*"

// stageTempFile writes data to a freshly created temp file in dir, named after
// base, flushes it to disk and returns the temp path. The caller must either
// install the file with replaceFile or remove it.
func stageTempFile(dir, base string, data []byte, mode os.FileMode) (string, error) {
	f, err := os.CreateTemp(dir, base+tempPatternSuffix)
	if err != nil {
		return "", err
	}

	tmp := f.Name()
	cleanup := func() { _ = os.Remove(tmp) }

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		cleanup()
		return "", err
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		cleanup()
		return "", err
	}

	if err := f.Close(); err != nil {
		cleanup()
		return "", err
	}

	if err := os.Chmod(tmp, mode); err != nil {
		cleanup()
		return "", err
	}

	return tmp, nil
}

// writeFileAtomic writes data to path via a same-directory temp file and an
// atomic rename, so a kill mid-write never leaves a truncated final file.
func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return errors.Wrapf(err, "could not create directory for: %s", path)
	}

	tmp, err := stageTempFile(dir, filepath.Base(path), data, mode)
	if err != nil {
		return errors.Wrapf(err, "could not stage file: %s", path)
	}

	if err := replaceFile(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return errors.Wrapf(err, "could not install file: %s", path)
	}

	return nil
}

// cleanupStaleTempFiles removes temp files left in dir by a run that was
// killed before install. Leftover temps must never count as completed work.
func cleanupStaleTempFiles(dir string) {
	patterns := []string{
		"*.fastresume" + tempPatternSuffix,
		"*.torrent" + tempPatternSuffix,
		"migration-report-*.json" + tempPatternSuffix,
	}

	for _, pattern := range patterns {
		matches, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			continue
		}
		for _, match := range matches {
			if err := os.Remove(match); err != nil {
				log.Warn().Err(err).Msgf("Could not remove stale temp file: %s", match)
			}
		}
	}
}

// commitFunc writes the qBittorrent .fastresume and .torrent for one torrent.
type commitFunc func(fr *Fastresume, info *metainfo.Info, infoHash []byte, torrentSrc, torrentID string) error

// defaultCommitArtifacts stages both artifacts in qbitDir, re-decodes the
// written resume data and verifies it against the torrent before installing
// either file. A verification or write failure leaves no artifact behind.
func defaultCommitArtifacts(qbitDir string, fr *Fastresume, info *metainfo.Info, infoHash []byte, torrentSrc, torrentID string) error {
	resumePath := filepath.Join(qbitDir, torrentID+".fastresume")
	torrentPath := filepath.Join(qbitDir, torrentID+".torrent")

	resumeData, err := bencode.Marshal(fr)
	if err != nil {
		return errors.Wrapf(err, "could not encode fastresume: %s", resumePath)
	}

	resumeTmp, err := stageTempFile(qbitDir, filepath.Base(resumePath), resumeData, 0o644)
	if err != nil {
		return errors.Wrapf(err, "could not write fastresume: %s", resumePath)
	}
	removeResumeTmp := true
	defer func() {
		if removeResumeTmp {
			_ = os.Remove(resumeTmp)
		}
	}()

	// verify what actually landed on disk, not the in-memory struct
	written, err := os.ReadFile(resumeTmp)
	if err != nil {
		return errors.Wrapf(err, "could not read back fastresume: %s", resumeTmp)
	}
	if err := verifyResumeData(written, info, infoHash); err != nil {
		return errors.Wrapf(err, "resume verification failed for: %s", torrentID)
	}

	torrentData, err := os.ReadFile(torrentSrc)
	if err != nil {
		return errors.Wrapf(err, "could not read source torrent: %s", torrentSrc)
	}

	mode := os.FileMode(0o644)
	if srcInfo, err := os.Stat(torrentSrc); err == nil {
		mode = srcInfo.Mode().Perm()
	}

	torrentTmp, err := stageTempFile(qbitDir, filepath.Base(torrentPath), torrentData, mode)
	if err != nil {
		return errors.Wrapf(err, "could not write torrent: %s", torrentPath)
	}
	removeTorrentTmp := true
	defer func() {
		if removeTorrentTmp {
			//nolint:gosec // G703: torrentTmp is the staged temp created above in --qbit-dir
			_ = os.Remove(torrentTmp)
		}
	}()

	if err := replaceFile(resumeTmp, resumePath); err != nil {
		return errors.Wrapf(err, "could not install fastresume: %s", resumePath)
	}
	removeResumeTmp = false

	if err := replaceFile(torrentTmp, torrentPath); err != nil {
		return errors.Wrapf(err, "could not install torrent: %s", torrentPath)
	}
	removeTorrentTmp = false

	return nil
}
