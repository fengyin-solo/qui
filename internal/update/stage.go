// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package update

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/creativeprojects/go-selfupdate"
)

const (
	archiveName   = "archive.download"
	candidateName = "candidate"
)

const copyBufferSize = 32 * 1024

// stagedUpdate is a fully downloaded and verified candidate in an isolated
// temp directory. The directory is removed by the caller.
type stagedUpdate struct {
	dir           string
	candidatePath string
	candidateHash string
	candidateSize int64
}

// stageCandidate downloads the signed checksums, verifies the ECDSA signature,
// streams the archive into an isolated temp directory while reporting progress,
// verifies the archive checksum, and decompresses the candidate binary.
func (u *Updater) stageCandidate(ctx context.Context, provider releaseProvider, validator validatorForChecksums, cand *candidate, onProgress ProgressFunc) (_ *stagedUpdate, err error) {
	dir, err := os.MkdirTemp(u.config.StageDir, "qui-update-*")
	if err != nil {
		return nil, fmt.Errorf("could not create staging directory: %w", err)
	}
	// Discard everything on failure: a corrupt candidate must never be reused.
	defer func() {
		if err != nil {
			_ = os.RemoveAll(dir)
		}
	}()

	checksums, err := u.downloadAssetBytes(ctx, provider, cand.checksumsID)
	if err != nil {
		return nil, fmt.Errorf("could not download checksums: %w", err)
	}
	signature, err := u.downloadAssetBytes(ctx, provider, cand.signatureID)
	if err != nil {
		return nil, fmt.Errorf("could not download checksums signature: %w", err)
	}

	if err := validator.Validate(releaseChecksumsAsset, checksums, signature); err != nil {
		return nil, fmt.Errorf("checksums signature verification failed: %w", err)
	}

	archivePath := filepath.Join(dir, archiveName)
	archiveHash, err := u.downloadAssetToFile(ctx, provider, cand.assetID, cand.assetName, archivePath, cand.assetSize, onProgress)
	if err != nil {
		return nil, err
	}

	if err := verifyChecksum(cand.assetName, archiveHash, checksums); err != nil {
		return nil, fmt.Errorf("candidate verification failed: %w", err)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	target, err := u.executablePath()
	if err != nil {
		return nil, fmt.Errorf("could not locate executable path: %w", err)
	}

	candidatePath := filepath.Join(dir, candidateName)
	if err := decompressCandidate(archivePath, cand, filepath.Base(target), candidatePath); err != nil {
		return nil, err
	}

	hash, size, err := hashFile(candidatePath)
	if err != nil {
		return nil, fmt.Errorf("could not hash candidate binary: %w", err)
	}
	if size == 0 {
		return nil, errors.New("candidate binary is empty")
	}

	return &stagedUpdate{
		dir:           dir,
		candidatePath: candidatePath,
		candidateHash: hash,
		candidateSize: size,
	}, nil
}

// validatorForChecksums is the subset of selfupdate.Validator used to verify
// the ECDSA signature over the checksums file.
type validatorForChecksums interface {
	Validate(filename string, release, asset []byte) error
}

func decompressCandidate(archivePath string, cand *candidate, exeName, candidatePath string) error {
	archive, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("could not open downloaded archive: %w", err)
	}
	defer archive.Close()

	reader, err := selfupdate.DecompressCommand(archive, cand.assetName, exeName, cand.os, cand.arch)
	if err != nil {
		return fmt.Errorf("could not decompress candidate archive: %w", err)
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}

	out, err := os.OpenFile(candidatePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o700)
	if err != nil {
		return fmt.Errorf("could not create candidate binary: %w", err)
	}
	defer out.Close()

	if _, err := io.Copy(out, reader); err != nil {
		return fmt.Errorf("could not extract candidate binary: %w", err)
	}
	if err := out.Sync(); err != nil {
		return fmt.Errorf("could not flush candidate binary: %w", err)
	}
	return nil
}

// downloadAssetBytes fully reads a small validation asset, honoring ctx.
func (u *Updater) downloadAssetBytes(ctx context.Context, provider releaseProvider, assetID int64) ([]byte, error) {
	rc, err := provider.downloadAsset(ctx, assetID)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	var buf bytes.Buffer
	if _, err := copyStream(ctx, &buf, rc); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// downloadAssetToFile streams an asset into dst while hashing its contents
// and reporting throttled progress. A cancelled context aborts the transfer.
func (u *Updater) downloadAssetToFile(ctx context.Context, provider releaseProvider, assetID int64, assetName, dst string, totalSize int64, onProgress ProgressFunc) (string, error) {
	rc, err := provider.downloadAsset(ctx, assetID)
	if err != nil {
		return "", fmt.Errorf("could not download %s: %w", assetName, err)
	}
	defer rc.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return "", err
	}
	var closed bool
	defer func() {
		if !closed {
			_ = out.Close()
		}
	}()

	hasher := sha256.New()
	var lastReport time.Time
	n, err := copyStream(ctx, io.MultiWriter(out, hasher), rc, func(read int64) {
		if onProgress == nil {
			return
		}
		if time.Since(lastReport) >= progressInterval {
			onProgress(Progress{Asset: assetName, BytesRead: read, TotalSize: totalSize})
			lastReport = time.Now()
		}
	})
	if err != nil {
		return "", fmt.Errorf("could not download %s: %w", assetName, err)
	}

	if onProgress != nil {
		onProgress(Progress{Asset: assetName, BytesRead: n, TotalSize: totalSize})
	}

	if err := out.Sync(); err != nil {
		return "", fmt.Errorf("could not flush %s: %w", assetName, err)
	}
	if err := out.Close(); err != nil {
		return "", err
	}
	closed = true

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// copyStream copies from src to dst in bounded chunks, failing fast when ctx
// is cancelled. onRead (may be nil) is invoked after every successful chunk.
func copyStream(ctx context.Context, dst io.Writer, src io.Reader, onRead ...func(read int64)) (int64, error) {
	buf := make([]byte, copyBufferSize)
	var total int64
	cb := func(int64) {}
	if len(onRead) > 0 && onRead[0] != nil {
		cb = onRead[0]
	}
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		nr, readErr := src.Read(buf)
		if nr > 0 {
			nw, writeErr := dst.Write(buf[:nr])
			if nw > 0 {
				total += int64(nw)
				cb(total)
			}
			if writeErr != nil {
				return total, writeErr
			}
			if nw != nr {
				return total, io.ErrShortWrite
			}
		}
		if readErr == io.EOF {
			return total, nil
		}
		if readErr != nil {
			return total, readErr
		}
	}
}

// verifyChecksum checks that got matches the "<sha256>  <filename>" entry for
// filename in a GNU-style checksums file.
func verifyChecksum(filename, got string, checksums []byte) error {
	expected := ""
	scanner := bufio.NewScanner(bytes.NewReader(checksums))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[len(fields)-1] == filename {
			expected = fields[0]
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if expected == "" {
		return fmt.Errorf("%w for %q", selfupdate.ErrHashNotFound, filename)
	}

	want, err := hex.DecodeString(expected)
	if err != nil {
		return fmt.Errorf("invalid checksum %q: %w", expected, selfupdate.ErrChecksumValidationFailed)
	}
	have, err := hex.DecodeString(got)
	if err != nil {
		return fmt.Errorf("invalid computed checksum %q: %w", got, selfupdate.ErrChecksumValidationFailed)
	}
	if !bytes.Equal(want, have) {
		return fmt.Errorf("expected %q, found %q: %w", expected, got, selfupdate.ErrChecksumValidationFailed)
	}
	return nil
}
