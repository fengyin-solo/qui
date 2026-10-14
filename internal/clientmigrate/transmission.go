package clientmigrate

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/autobrr/go-torrent/bencode"
	"github.com/autobrr/go-torrent/metainfo"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type TransmissionImport struct {
	opts   Options
	commit commitFunc
}

func NewTransmissionImporter(opts Options) *TransmissionImport {
	return &TransmissionImport{opts: opts}
}

func (i *TransmissionImport) Migrate() error {
	torrentsDir := filepath.Join(i.opts.SourceDir, "torrents")

	sourceDirInfo, err := os.Stat(torrentsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return errors.Errorf("source directory does not exist: %s", torrentsDir)
		}

		return errors.Wrapf(err, "source directory error: %s", torrentsDir)
	}

	if !sourceDirInfo.IsDir() {
		return errors.Errorf("source is a file, not a directory: %s", torrentsDir)
	}

	if !i.opts.DryRun {
		if err := os.MkdirAll(i.opts.QbitDir, os.ModePerm); err != nil {
			return errors.Wrapf(err, "qbit directory error: %s", i.opts.QbitDir)
		}
	}

	run, err := newMigrateRun(i.opts, i.commit)
	if err != nil {
		return err
	}

	matches, err := filepath.Glob(filepath.Join(torrentsDir, "*.torrent"))
	if err != nil {
		return errors.Wrapf(err, "glob error: %s", torrentsDir)
	}

	totalJobs := len(matches)

	log.Info().Msgf("Total torrents to process: %d", totalJobs)

	positionNum := 0
	for _, match := range matches {
		positionNum++

		// keep the exact source basename: resume files share it, and legacy
		// Transmission <=2.9x names are "<Name>.<16hex>" with original casing
		baseName := strings.TrimSuffix(filepath.Base(match), filepath.Ext(match))

		file, err := metainfo.LoadFromFile(match)
		if err != nil {
			log.Error().Err(err).Msgf("Could not load torrent file %s", match)
			if recordErr := run.record("", baseName, ResultFailed, "could not load torrent file: "+err.Error()); recordErr != nil {
				return recordErr
			}
			continue
		}

		metaInfo, err := file.UnmarshalInfo()
		if err != nil {
			log.Error().Err(err).Msgf("Could not unmarshal torrent file %s", match)
			if recordErr := run.record("", baseName, ResultFailed, "could not unmarshal torrent metadata: "+err.Error()); recordErr != nil {
				return recordErr
			}
			continue
		}

		name := metaInfo.BestName()

		// qBittorrent requires BT_backup files named by infohash
		torrentID := file.HashInfoBytes().HexString()
		infoHash := file.HashInfoBytes().Bytes()

		// a previous run already imported this infohash; never write it again
		if run.alreadyImported(torrentID) {
			log.Info().Msgf("(%d/%d) %s already imported in a previous run, skipping", positionNum, totalJobs, torrentID)
			run.skippedThisRun++
			continue
		}

		// v2 and hybrid torrents cannot round-trip through this format: the
		// v1 infohash naming is wrong for v2-only, and the block bitfield
		// covers the padded v1 stream while UpvertedFiles is the v2 tree view
		if metaInfo.MetaVersion == 2 {
			log.Warn().Msgf("(%d/%d) %s is a BitTorrent v2 torrent, not supported by this importer, skipping", positionNum, totalJobs, name)
			if recordErr := run.record(torrentID, name, ResultSkipped, "BitTorrent v2 torrent not supported"); recordErr != nil {
				return recordErr
			}
			continue
		}

		// check for FILE.resume
		resumeFilePath := filepath.Join(i.opts.SourceDir, "resume", baseName+".resume")

		resumeFile, err := i.decodeResumeFile(resumeFilePath)
		if err != nil {
			log.Error().Err(err).Msgf("Could not decode transmission resume file %s for %s", resumeFilePath, torrentID)
			if recordErr := run.record(torrentID, name, ResultFailed, "could not decode transmission resume file: "+err.Error()); recordErr != nil {
				return recordErr
			}
			continue
		}

		// complete means every block of every wanted (non-dnd) file is
		// downloaded; deselected files may legitimately be missing
		pieces, complete := transmissionPieces(resumeFile, &metaInfo)
		if !complete {
			log.Warn().Msgf("(%d/%d) %s is not fully downloaded, skipping: %s", positionNum, totalJobs, name, match)
			if recordErr := run.record(torrentID, name, ResultSkipped, "torrent is not fully downloaded"); recordErr != nil {
				return recordErr
			}
			continue
		}

		// a corrupt entry with an unusable download dir would import a torrent
		// qBittorrent resolves against its own default download dir
		if !filepath.IsAbs(filepath.Clean(resumeFile.Destination)) {
			log.Warn().Msgf("(%d/%d) %s has an unusable download dir %q, skipping", positionNum, totalJobs, name, resumeFile.Destination)
			if recordErr := run.record(torrentID, name, ResultSkipped, "download dir is not absolute: "+resumeFile.Destination); recordErr != nil {
				return recordErr
			}
			continue
		}

		if i.opts.DryRun {
			log.Info().Msgf("dry-run: (%d/%d) would import: %s", positionNum, totalJobs, torrentID)
			if err := run.record(torrentID, name, ResultImported, ""); err != nil {
				return err
			}
			continue
		}

		// completed-torrent timestamps: done-date is legitimately 0 for
		// torrents Transmission adopted from existing data, fall back to
		// added-date like Transmission 4.x itself does
		completedTime := resumeFile.DoneDate
		if completedTime == 0 {
			completedTime = resumeFile.AddedDate
		}

		// auto-managed keeps qBittorrent's queueing and share limits in
		// charge; a paused torrent must stay out of auto-management or
		// qBittorrent would resume it
		paused := boolToInt(resumeFile.Paused)

		newFastResume := Fastresume{
			ActiveTime:                resumeFile.DownloadingTimeSeconds + resumeFile.SeedingTimeSeconds,
			AddedTime:                 resumeFile.AddedDate,
			Allocation:                "sparse",
			ApplyIPFilter:             1,
			AutoManaged:               1 - paused,
			CompletedTime:             completedTime,
			DisableDHT:                0,
			DisableLSD:                0,
			DisablePEX:                0,
			DownloadRateLimit:         transmissionSpeedLimit(resumeFile.SpeedLimitDown),
			FileFormat:                "libtorrent resume file",
			FileVersion:               1,
			FilePriority:              []int{},
			FinishedTime:              resumeFile.SeedingTimeSeconds,
			LastDownload:              resumeFile.ActivityDate,
			LastSeenComplete:          completedTime,
			LastUpload:                resumeFile.ActivityDate,
			LibTorrentVersion:         "1.2.11.0",
			MaxConnections:            16777215,
			MaxUploads:                -1,
			NumComplete:               16777215,
			NumDownloaded:             16777215,
			NumIncomplete:             0,
			Paused:                    paused,
			Peers:                     "",
			Peers6:                    "",
			QbtCategory:               "",
			QbtContentLayout:          "Original",
			QbtFirstLastPiecePriority: 0,
			QbtName:                   "",
			QbtRatioLimit:             transmissionRatioLimit(resumeFile.RatioLimit),
			QbtSavePath:               resumeFile.Destination,
			QbtSeedStatus:             1,
			QbtSeedingTimeLimit:       -2,
			QbtTags:                   append(resumeFile.Labels, "migrated"),
			SavePath:                  resumeFile.Destination,
			SeedMode:                  0,
			SeedingTime:               resumeFile.SeedingTimeSeconds,
			SequentialDownload:        0,
			ShareMode:                 0,
			StopWhenReady:             0,
			SuperSeeding:              0,
			TotalDownloaded:           resumeFile.Downloaded,
			TotalUploaded:             resumeFile.Uploaded,
			UploadMode:                0,
			UploadRateLimit:           transmissionSpeedLimit(resumeFile.SpeedLimitUp),
			URLList:                   file.UrlList,
		}

		// destination is the parent directory in every Transmission version:
		// file subpaths already start with the torrent name
		if metaInfo.IsDir() {
			// legacy and should be removed sometime with 4.3.X
			newFastResume.QbtHasRootFolder = 1
		} else {
			newFastResume.QbtHasRootFolder = 0
		}

		// handle trackers
		newFastResume.Trackers = file.UpvertedAnnounceList()

		newFastResume.FilePriority = transmissionFilePriorities(resumeFile, len(metaInfo.UpvertedFiles()))

		// pieces overlapping only deselected files stay unset
		newFastResume.Pieces = pieces

		// Set 20 byte SHA1 hash
		newFastResume.InfoHash = infoHash

		// fields verified after writing: piece length and the ordered file list
		newFastResume.setFileMeta(&metaInfo)

		if err = run.commit(&newFastResume, &metaInfo, infoHash, match, torrentID); err != nil {
			log.Error().Err(err).Msgf("Could not import %s", torrentID)
			if recordErr := run.record(torrentID, name, ResultFailed, err.Error()); recordErr != nil {
				return recordErr
			}
			continue
		}

		if err := run.record(torrentID, name, ResultImported, ""); err != nil {
			return err
		}

		log.Info().Msgf("(%d/%d) successfully imported: %s %s", positionNum, totalJobs, torrentID, name)
	}

	logImportSummary(i.opts.DryRun, run.importedThisRun, run.failedThisRun, run.skippedThisRun, totalJobs)

	return run.finish()
}

// transmissionPieces expands the resume file's block bitfield into a qBittorrent
// piece bitfield (one byte per piece) and reports whether every block of every
// wanted (non-dnd) file is present. Blocks are 16 KiB (or the piece length when
// smaller), MSB-first in the raw bitfield; "all"/"none" are literal strings
func transmissionPieces(resume *TransmissionResumeFile, info *metainfo.Info) (string, bool) {
	numPieces := info.NumPieces()
	if numPieces == 0 || info.PieceLength <= 0 {
		return "", false
	}

	files := info.UpvertedFiles()

	blockSize := min(info.PieceLength, 16384)
	totalLength := info.TotalLength()
	numBlocks := int((totalLength + blockSize - 1) / blockSize)

	haveBlock := func(int) bool { return true }
	switch resume.Progress.Blocks {
	case "all":
	case "none", "":
		haveBlock = func(int) bool { return false }
	default:
		raw := resume.Progress.Blocks
		if len(raw) < (numBlocks+7)/8 {
			return "", false
		}
		haveBlock = func(b int) bool { return raw[b/8]>>(7-b%8)&1 == 1 }
	}

	wantedFiles := make([]bool, len(files))
	for i := range wantedFiles {
		wantedFiles[i] = true
	}
	if len(resume.Dnd) == len(files) {
		for i, dnd := range resume.Dnd {
			wantedFiles[i] = dnd == 0
		}
	}

	wantedBlock := make([]bool, numBlocks)
	var offset int64
	for i, f := range files {
		if wantedFiles[i] && f.Length > 0 {
			start := int(offset / blockSize)
			end := int((offset + f.Length - 1) / blockSize)
			for b := start; b <= end && b < numBlocks; b++ {
				wantedBlock[b] = true
			}
		}
		offset += f.Length
	}

	complete := true
	for b := range numBlocks {
		if wantedBlock[b] && !haveBlock(b) {
			complete = false
			break
		}
	}

	// map pieces to blocks by byte range: blocks are laid out over the whole
	// torrent stream and need not align to piece boundaries
	pieces := make([]byte, numPieces)
	for p := range numPieces {
		startByte := int64(p) * info.PieceLength
		endByte := min(startByte+info.PieceLength, totalLength)
		start := int(startByte / blockSize)
		end := int((endByte - 1) / blockSize)
		pieces[p] = 1
		for b := start; b <= end && b < numBlocks; b++ {
			if !haveBlock(b) {
				pieces[p] = 0
				break
			}
		}
	}

	return string(pieces), complete
}

// transmissionFilePriorities maps transmission per-file state to qBittorrent
// priorities: dnd files become 0 (skip), high priority becomes 6
func transmissionFilePriorities(resume *TransmissionResumeFile, fileCount int) []int {
	prios := make([]int, fileCount)
	for i := range prios {
		prios[i] = 1
	}

	if len(resume.Priority) == fileCount {
		for i, p := range resume.Priority {
			if p == 1 {
				prios[i] = 6
			}
		}
	}

	if len(resume.Dnd) == fileCount {
		for i, dnd := range resume.Dnd {
			if dnd == 1 {
				prios[i] = 0
			}
		}
	}

	return prios
}

func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// transmissionSpeedLimit maps a per-torrent transmission speed limit to the
// qBittorrent rate limit field: bytes/sec when a torrent-specific limit is
// enabled, -1 (no limit) otherwise
func transmissionSpeedLimit(limit TransmissionResumeFileSpeedLimit) int64 {
	if limit.UseSpeedLimit == 1 && limit.UseGlobalSpeedLimit == 0 && limit.SpeedBPS > 0 {
		return limit.SpeedBPS
	}
	return -1
}

// transmissionRatioLimit maps transmission's ratio-limit dict to the
// qBt-ratioLimit convention: -2000 = use global, -1000 = unlimited,
// otherwise ratio * 1000
func transmissionRatioLimit(limit TransmissionResumeFileRatioLimit) int64 {
	switch limit.RatioMode {
	case 1:
		ratio, err := strconv.ParseFloat(limit.RatioLimit, 64)
		if err != nil {
			return -2000
		}
		return int64(ratio * 1000)
	case 2:
		return -1000
	default:
		return -2000
	}
}

func (i *TransmissionImport) decodeResumeFile(path string) (*TransmissionResumeFile, error) {
	dat, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var torrentResumeFile TransmissionResumeFile
	if err := bencode.NewDecoder(bytes.NewReader(dat)).Decode(&torrentResumeFile); err != nil {
		return nil, err
	}

	return &torrentResumeFile, nil
}

type TransmissionResumeFile struct {
	Files                  []string                         `bencode:"files"`
	Name                   string                           `bencode:"name"`
	Corrupted              int64                            `bencode:"corrupt"`
	Destination            string                           `bencode:"destination"`
	IncompleteDir          string                           `bencode:"incomplete-dir"`
	Downloaded             int64                            `bencode:"downloaded"`
	Uploaded               int64                            `bencode:"uploaded"`
	Group                  string                           `bencode:"group"`
	BandwidthPriority      int                              `bencode:"bandwidth-priority"`
	Priority               []int                            `bencode:"priority"`
	DoneDate               int64                            `bencode:"done-date"`
	DownloadingTimeSeconds int64                            `bencode:"downloading-time-seconds"`
	Labels                 []string                         `bencode:"labels"`
	MaxPeers               int64                            `bencode:"max-peers"`
	Paused                 bool                             `bencode:"paused"`
	Peers                  string                           `bencode:"peers2"`
	ActivityDate           int64                            `bencode:"activity-date"`
	AddedDate              int64                            `bencode:"added-date"`
	Dnd                    []int                            `bencode:"dnd"`
	SeedingTimeSeconds     int64                            `bencode:"seeding-time-seconds"`
	Progress               TransmissionResumeFileProgress   `bencode:"progress"`
	IdleLimit              TransmissionResumeFileIdleLimit  `bencode:"idle-limit"`
	RatioLimit             TransmissionResumeFileRatioLimit `bencode:"ratio-limit"`
	SpeedLimitUp           TransmissionResumeFileSpeedLimit `bencode:"speed-limit-up"`
	SpeedLimitDown         TransmissionResumeFileSpeedLimit `bencode:"speed-limit-down"`
}

type TransmissionResumeFileProgress struct {
	Blocks string  `bencode:"blocks"`
	Have   string  `bencode:"have"`
	MTimes []int64 `bencode:"mtimes"`
	Pieces string  `bencode:"pieces"`
}

type TransmissionResumeFileSpeedLimit struct {
	SpeedBPS            int64 `bencode:"speed-Bps"`
	UseGlobalSpeedLimit int64 `bencode:"use-global-speed-limit"`
	UseSpeedLimit       int64 `bencode:"use-speed-limit"`
}

type TransmissionResumeFileRatioLimit struct {
	RatioLimit string `bencode:"ratio-limit"`
	RatioMode  int    `bencode:"ratio-mode"`
}

type TransmissionResumeFileIdleLimit struct {
	IdleLimit int64 `bencode:"idle-limit"`
	IdleMode  int   `bencode:"idle-mode"`
}
