package clientmigrate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/autobrr/go-torrent/metainfo"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// EntryResult is the per-torrent outcome recorded in the migration report
type EntryResult string

const (
	ResultImported EntryResult = "imported"
	ResultFailed   EntryResult = "failed"
	ResultSkipped  EntryResult = "skipped"
)

// ReportEntry is one torrent's audit record
type ReportEntry struct {
	InfoHash    string      `json:"infohash"`
	Name        string      `json:"name"`
	Result      EntryResult `json:"result"`
	Reason      string      `json:"reason,omitempty"`
	Attempts    int         `json:"attempts"`
	AttemptedAt time.Time   `json:"attempted_at"`
}

// MigrationReport is the structured audit trail written into the qBittorrent
// directory on every run. Real runs keep it across runs so reruns can resume;
// dry runs write a separate file that is never read back.
type MigrationReport struct {
	Source     ClientType    `json:"source"`
	DryRun     bool          `json:"dry_run"`
	StartedAt  time.Time     `json:"started_at"`
	FinishedAt time.Time     `json:"finished_at"`
	Entries    []ReportEntry `json:"entries"`
}

func reportFilePath(opts Options) string {
	name := "migration-report-" + string(opts.Source) + ".json"
	if opts.DryRun {
		name = "migration-report-" + string(opts.Source) + ".dry-run.json"
	}
	return filepath.Join(opts.QbitDir, name)
}

// entryKey identifies an entry: infohash when known, otherwise the torrent
// name/source file name
func entryKey(hash, name string) string {
	if hash != "" {
		return hash
	}
	return name
}

func loadMigrationReport(path string, source ClientType) (*MigrationReport, error) {
	dat, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &MigrationReport{Source: source, StartedAt: time.Now()}, nil
		}
		return nil, errors.Wrapf(err, "could not read previous migration report: %s", path)
	}

	var report MigrationReport
	if err := json.Unmarshal(dat, &report); err != nil {
		return nil, errors.Wrapf(err, "could not parse previous migration report, fix or remove it before retrying: %s", path)
	}
	report.Source = source
	report.StartedAt = time.Now()
	report.FinishedAt = time.Time{}

	return &report, nil
}

func saveMigrationReport(path string, report *MigrationReport) error {
	dat, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, dat, 0o644)
}

func (r *MigrationReport) tally() (imported, failed, skipped int) {
	for _, e := range r.Entries {
		switch e.Result {
		case ResultImported:
			imported++
		case ResultFailed:
			failed++
		case ResultSkipped:
			skipped++
		}
	}
	return
}

// migrateRun tracks one importer execution: the cumulative report, the set of
// already-imported infohashes and this run's counters
type migrateRun struct {
	opts       Options
	report     *MigrationReport
	reportPath string
	index      map[string]int
	imported   map[string]struct{}

	importedThisRun int
	failedThisRun   int
	skippedThisRun  int

	commit commitFunc
}

func newMigrateRun(opts Options, overrideCommit commitFunc) (*migrateRun, error) {
	r := &migrateRun{
		opts:       opts,
		reportPath: reportFilePath(opts),
		index:      make(map[string]int),
		imported:   make(map[string]struct{}),
		commit:     overrideCommit,
	}
	if r.commit == nil {
		r.commit = func(fr *Fastresume, info *metainfo.Info, infoHash []byte, torrentSrc, torrentID string) error {
			return defaultCommitArtifacts(opts.QbitDir, fr, info, infoHash, torrentSrc, torrentID)
		}
	}

	if opts.DryRun {
		r.report = &MigrationReport{Source: opts.Source, DryRun: true, StartedAt: time.Now()}
	} else {
		report, err := loadMigrationReport(r.reportPath, opts.Source)
		if err != nil {
			return nil, err
		}
		r.report = report

		// temp files from a killed run never count as completed
		cleanupStaleTempFiles(opts.QbitDir)
	}

	for i, e := range r.report.Entries {
		r.index[entryKey(e.InfoHash, e.Name)] = i
		if e.Result == ResultImported {
			r.imported[entryKey(e.InfoHash, e.Name)] = struct{}{}
		}
	}

	return r, nil
}

// alreadyImported reports whether a previous run finished this infohash
// successfully; such entries must never be written again
func (r *migrateRun) alreadyImported(hash string) bool {
	_, ok := r.imported[hash]
	return ok
}

// record upserts the entry for this torrent and persists the report on real
// runs so a kill keeps every already-committed result
func (r *migrateRun) record(hash, name string, result EntryResult, reason string) error {
	switch result {
	case ResultImported:
		r.importedThisRun++
	case ResultFailed:
		r.failedThisRun++
	case ResultSkipped:
		r.skippedThisRun++
	}

	key := entryKey(hash, name)
	now := time.Now()

	if idx, ok := r.index[key]; ok {
		e := &r.report.Entries[idx]
		if hash != "" {
			e.InfoHash = hash
		}
		if name != "" {
			e.Name = name
		}
		e.Result = result
		e.Reason = reason
		e.Attempts++
		e.AttemptedAt = now

		if result == ResultImported {
			r.imported[key] = struct{}{}
		}
	} else {
		r.report.Entries = append(r.report.Entries, ReportEntry{
			InfoHash:    hash,
			Name:        name,
			Result:      result,
			Reason:      reason,
			Attempts:    1,
			AttemptedAt: now,
		})
		r.index[key] = len(r.report.Entries) - 1
		if result == ResultImported {
			r.imported[key] = struct{}{}
		}
	}

	if r.opts.DryRun {
		return nil
	}

	if err := saveMigrationReport(r.reportPath, r.report); err != nil {
		return errors.Wrap(err, "could not persist migration report")
	}

	return nil
}

// finish writes the final report and returns an error when any entry is still
// failed, so the process exit code reflects the migration result
func (r *migrateRun) finish() error {
	r.report.FinishedAt = time.Now()

	if err := saveMigrationReport(r.reportPath, r.report); err != nil {
		if r.opts.DryRun {
			// a dry run must not create directories; emit the report in the log
			r.logReport()
			log.Warn().Err(err).Msgf("dry-run: could not write migration report to: %s", r.reportPath)
			return nil
		}
		return errors.Wrap(err, "could not write migration report")
	}

	log.Info().Msgf("Migration report written to: %s", r.reportPath)

	_, failed, _ := r.report.tally()
	if failed > 0 {
		return errors.Errorf("migration finished with %d failed torrent(s), see report: %s", failed, r.reportPath)
	}

	return nil
}

func (r *migrateRun) logReport() {
	dat, err := json.MarshalIndent(r.report, "", "  ")
	if err != nil {
		return
	}
	log.Info().Msgf("dry-run migration report:\n%s", string(dat))
}
