// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package update

import (
	"context"
	"fmt"
	"io"

	"github.com/creativeprojects/go-selfupdate"
)

// candidate describes a release asset selected for the current platform.
type candidate struct {
	version     string
	assetID     int64
	assetName   string
	assetSize   int64
	checksumsID int64
	signatureID int64
	os          string
	arch        string
}

// releaseProvider finds the newest release and streams its assets.
type releaseProvider interface {
	// latest returns the candidate when a newer release exists. A nil
	// candidate means the running version is already up to date.
	latest(ctx context.Context) (*candidate, bool, error)
	// downloadAsset streams a release asset by its ID.
	downloadAsset(ctx context.Context, assetID int64) (io.ReadCloser, error)
}

// githubProvider adapts go-selfupdate to the staged updater.
type githubProvider struct {
	currentVersion string
	updater        *selfupdate.Updater
	source         selfupdate.Source
	repository     selfupdate.Repository

	// release backs asset downloads after latest succeeds.
	release *selfupdate.Release
}

func newGitHubProvider(repository, currentVersion string, validator selfupdate.Validator) (*githubProvider, error) {
	source, err := selfupdate.NewGitHubSource(selfupdate.GitHubConfig{})
	if err != nil {
		return nil, fmt.Errorf("could not create release source: %w", err)
	}

	updater, err := selfupdate.NewUpdater(selfupdate.Config{
		Source:    source,
		Validator: validator,
	})
	if err != nil {
		return nil, fmt.Errorf("could not create updater: %w", err)
	}

	return &githubProvider{
		currentVersion: currentVersion,
		updater:        updater,
		source:         source,
		repository:     selfupdate.ParseSlug(repository),
	}, nil
}

func (p *githubProvider) latest(ctx context.Context) (*candidate, bool, error) {
	rel, found, err := p.updater.DetectLatest(ctx, p.repository)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}

	if rel.LessOrEqual(p.currentVersion) {
		return nil, true, nil
	}

	cand := &candidate{
		version:   rel.Version(),
		assetID:   rel.AssetID,
		assetName: rel.AssetName,
		assetSize: int64(rel.AssetByteSize),
		os:        rel.OS,
		arch:      rel.Arch,
	}

	for _, step := range rel.ValidationChain {
		switch step.ValidationAssetName {
		case releaseChecksumsAsset:
			cand.checksumsID = step.ValidationAssetID
		case releaseChecksumsAsset + ".sig":
			cand.signatureID = step.ValidationAssetID
		}
	}
	if cand.checksumsID == 0 || cand.signatureID == 0 {
		return nil, false, fmt.Errorf("release %s is missing signed checksums assets", cand.version)
	}

	p.release = rel
	return cand, true, nil
}

func (p *githubProvider) downloadAsset(ctx context.Context, assetID int64) (io.ReadCloser, error) {
	return p.source.DownloadReleaseAsset(ctx, p.release, assetID)
}
