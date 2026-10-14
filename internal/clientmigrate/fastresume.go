package clientmigrate

import (
	"bytes"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/autobrr/go-torrent/bencode"
	"github.com/autobrr/go-torrent/metainfo"

	"github.com/pkg/errors"
)

// Fastresume represents a qBittorrent fastresume file
type Fastresume struct {
	ActiveTime                int64      `bencode:"active_time"`
	AddedTime                 int64      `bencode:"added_time"`
	Allocation                string     `bencode:"allocation"`
	ApplyIPFilter             int64      `bencode:"apply_ip_filter"`
	AnnounceToDht             int64      `bencode:"announce_to_dht,omitempty"`
	AnnounceToLsd             int64      `bencode:"announce_to_lsd,omitempty"`
	AnnounceToTrackers        int64      `bencode:"announce_to_trackers,omitempty"`
	AutoManaged               int64      `bencode:"auto_managed"`
	CompletedTime             int64      `bencode:"completed_time"`
	DisableDHT                int64      `bencode:"disable_dht"`
	DisableLSD                int64      `bencode:"disable_lsd"`
	DisablePEX                int64      `bencode:"disable_pex"`
	DownloadRateLimit         int64      `bencode:"download_rate_limit"`
	FileFormat                string     `bencode:"file-format"`
	FileVersion               int64      `bencode:"file-version"`
	FilePriority              []int      `bencode:"file_priority"`
	FileSizes                 [][]any    `bencode:"file sizes,omitempty"`
	FinishedTime              int64      `bencode:"finished_time"`
	HTTPSeeds                 []string   `bencode:"httpseeds,omitempty"`
	InfoHash                  []byte     `bencode:"info-hash"`
	LastDownload              int64      `bencode:"last_download"`
	LastSeenComplete          int64      `bencode:"last_seen_complete"`
	LastUpload                int64      `bencode:"last_upload"`
	LibTorrentVersion         string     `bencode:"libtorrent-version"`
	MaxConnections            int64      `bencode:"max_connections"`
	MaxUploads                int64      `bencode:"max_uploads"`
	Name                      string     `bencode:"name,omitempty"`
	NumComplete               int64      `bencode:"num_complete"`
	NumDownloaded             int64      `bencode:"num_downloaded"`
	NumIncomplete             int64      `bencode:"num_incomplete"`
	Paused                    int64      `bencode:"paused"`
	Pieces                    string     `bencode:"pieces"`
	PieceLength               int64      `bencode:"piece length"`
	PiecePriority             []byte     `bencode:"piece_priority"`
	Peers                     string     `bencode:"peers"`
	Peers6                    string     `bencode:"peers6"`
	QbtCategory               string     `bencode:"qBt-category"`
	QbtContentLayout          string     `bencode:"qBt-contentLayout"`
	QbtHasRootFolder          int64      `bencode:"qBt-hasRootFolder"`
	QbtFirstLastPiecePriority int64      `bencode:"qBt-firstLastPiecePriority"`
	QbtName                   string     `bencode:"qBt-name"`
	QbtRatioLimit             int64      `bencode:"qBt-ratioLimit"`
	QbtSavePath               string     `bencode:"qBt-savePath"`
	QbtSeedStatus             int64      `bencode:"qBt-seedStatus"`
	QbtSeedingTimeLimit       int64      `bencode:"qBt-seedingTimeLimit"`
	QbtTags                   []string   `bencode:"qBt-tags"`
	QbtQueuePosition          int        `bencode:"qBt-queuePosition,omitempty"`
	QbtTempPathDisabled       int64      `bencode:"qBt-tempPathDisabled,omitempty"`
	SavePath                  string     `bencode:"save_path"`
	SeedMode                  int64      `bencode:"seed_mode"`
	SeedingTime               int64      `bencode:"seeding_time"`
	SequentialDownload        int64      `bencode:"sequential_download"`
	ShareMode                 int64      `bencode:"share_mode"`
	StopWhenReady             int64      `bencode:"stop_when_ready"`
	SuperSeeding              int64      `bencode:"super_seeding"`
	TotalDownloaded           int64      `bencode:"total_downloaded"`
	TotalUploaded             int64      `bencode:"total_uploaded"`
	Trackers                  [][]string `bencode:"trackers,omitempty"`
	UploadMode                int64      `bencode:"upload_mode"`
	UploadRateLimit           int64      `bencode:"upload_rate_limit"`
	URLList                   []string   `bencode:"url-list"`
	Unfinished                *[]any     `bencode:"unfinished,omitempty"`
	TorrentFilePath           string     `bencode:"-"`
	MappedFiles               []string   `bencode:"mapped_files,omitempty"`
}

// setFileMeta stamps the torrent-identifying fields that the post-write
// verification compares against the .torrent: piece length and libtorrent's
// "file sizes" entries, each a [priority, decimal size] pair in torrent order
func (fr *Fastresume) setFileMeta(info *metainfo.Info) {
	fr.PieceLength = info.PieceLength

	files := info.UpvertedFiles()
	fileSizes := make([][]any, len(files))
	for i, f := range files {
		priority := int64(1)
		if i < len(fr.FilePriority) {
			priority = int64(fr.FilePriority[i])
		}
		fileSizes[i] = []any{priority, strconv.FormatInt(f.Length, 10)}
	}
	fr.FileSizes = fileSizes
}

// verifyResumeData re-decodes encoded fastresume bytes and checks the
// torrent-identifying data against the metainfo: infohash, piece length,
// piece count and the ordered file list. Any mismatch means the written
// resume data must not be handed to qBittorrent.
func verifyResumeData(dat []byte, info *metainfo.Info, wantHash []byte) error {
	var fr Fastresume
	if err := bencode.NewDecoder(bytes.NewReader(dat)).Decode(&fr); err != nil {
		return errors.Wrap(err, "could not decode written resume data")
	}

	var problems []string

	if len(wantHash) != 20 {
		problems = append(problems, "torrent has no 20-byte v1 infohash")
	} else if !bytes.Equal(fr.InfoHash, wantHash) {
		gotHash := hex.EncodeToString(fr.InfoHash)
		if gotHash == "" {
			gotHash = "(empty)"
		}
		problems = append(problems, "infohash "+gotHash+" does not match torrent "+hex.EncodeToString(wantHash))
	}

	if fr.PieceLength != info.PieceLength {
		problems = append(problems, "piece length "+strconv.FormatInt(fr.PieceLength, 10)+" does not match torrent "+strconv.FormatInt(info.PieceLength, 10))
	}

	numPieces := info.NumPieces()
	if len(fr.Pieces) != numPieces {
		problems = append(problems, "piece count "+strconv.Itoa(len(fr.Pieces))+" does not match torrent "+strconv.Itoa(numPieces))
	}

	files := info.UpvertedFiles()
	if len(fr.FilePriority) != len(files) {
		problems = append(problems, "file priority count "+strconv.Itoa(len(fr.FilePriority))+" does not match file count "+strconv.Itoa(len(files)))
	}

	if len(fr.FileSizes) != len(files) {
		problems = append(problems, "file sizes count "+strconv.Itoa(len(fr.FileSizes))+" does not match file count "+strconv.Itoa(len(files)))
	} else {
		for i, entry := range fr.FileSizes {
			if len(entry) != 2 {
				problems = append(problems, "file sizes entry "+strconv.Itoa(i)+" is malformed")
				continue
			}
			sizeStr, ok := entry[1].(string)
			if !ok {
				problems = append(problems, "file sizes entry "+strconv.Itoa(i)+" has a non-text size")
				continue
			}
			size, err := strconv.ParseInt(sizeStr, 10, 64)
			if err != nil {
				problems = append(problems, "file sizes entry "+strconv.Itoa(i)+" has an unparseable size: "+sizeStr)
				continue
			}
			if size != files[i].Length {
				problems = append(problems, "file "+strconv.Itoa(i)+" size "+sizeStr+" does not match torrent "+strconv.FormatInt(files[i].Length, 10))
			}
		}
	}

	if len(fr.MappedFiles) > 0 && len(fr.MappedFiles) != len(files) {
		problems = append(problems, "mapped files count "+strconv.Itoa(len(fr.MappedFiles))+" does not match file count "+strconv.Itoa(len(files)))
	}

	if len(problems) > 0 {
		return errors.New("resume data mismatch: " + strings.Join(problems, "; "))
	}

	return nil
}

// ConvertFilePriority for each file set priority
func (fr *Fastresume) ConvertFilePriority(numFiles int) {
	newPrioList := make([]int, 0, numFiles)

	/*
		File priority:
		0 Do not download
		1 Normal
		6 High
	*/
	for range numFiles {
		newPrioList = append(newPrioList, 1)
	}

	fr.FilePriority = newPrioList
}
