package lifecycle

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/util"
)

type Report struct {
	DryRun        bool                 `json:"dryRun"`
	ConfigUsed    config.LifecycleConf `json:"configUsed"`
	BackupsKept   []string             `json:"backupsKept"`
	BackupsPurged []string             `json:"backupsPurged"`
	KeepReasons   map[string][]string  `json:"keepReasons"`
	BackupTypes   map[string]string    `json:"backupTypes"`
	// DeleteTargets contains entry points passed to the checked deletion path. An
	// incremental chain contributes only its base while BackupsPurged lists every member.
	DeleteTargets []string `json:"-"`
}

const (
	incrementalChainReason        = "Incremental Chain Retained"
	invalidIncrementalChainReason = "Invalid Incremental Chain"
	pitrBaseSnapshotReason        = "PITR Base Snapshot"
)

type incrementalChain struct {
	members []backup.BackupMeta
	valid   bool
}

type pitrBaseCandidates struct {
	names               []string
	previousRestoreTime bson.Timestamp
	restoreTime         bson.Timestamp
}

func (r *Report) addKeepReason(name, reason string) {
	for _, existing := range r.KeepReasons[name] {
		if existing == reason {
			return
		}
	}
	r.KeepReasons[name] = append(r.KeepReasons[name], reason)
}

func (r *Report) countKeptSuccessfulBackups(backups []backup.BackupMeta) int {
	count := 0
	for _, bcp := range backups {
		if bcp.Status == defs.StatusDone && len(r.KeepReasons[bcp.Name]) > 0 {
			count++
		}
	}
	return count
}

func filterBackupsByProfile(backups []backup.BackupMeta, profile string) []backup.BackupMeta {
	filtered := make([]backup.BackupMeta, 0, len(backups))
	for _, bcp := range backups {
		if bcp.Store.Name != profile {
			continue
		}

		filtered = append(filtered, bcp)
	}

	return filtered
}

// EvaluateProfile loads lifecycle configuration and backup metadata for one
// profile, then evaluates the policy at the provided time.
func EvaluateProfile(
	ctx context.Context,
	conn connect.Client,
	profile string,
	dryRun bool,
	now time.Time,
) (*Report, error) {
	var cfg *config.Config
	var err error
	if profile == "" {
		cfg, err = config.GetConfig(ctx, conn)
		if err != nil {
			return nil, errors.Wrap(err, "get config")
		}
	} else {
		cfg, err = config.GetProfile(ctx, conn, profile)
		if err != nil {
			return nil, errors.Wrap(err, "get profile config")
		}
	}

	allBackups, err := backup.BackupsList(ctx, conn, 0)
	if err != nil {
		return nil, errors.Wrap(err, "fetch backups")
	}

	selectedBackups := filterBackupsByProfile(allBackups, profile)
	report := evaluate(*cfg.Lifecycle, selectedBackups, allBackups, dryRun, now)

	pitrEnabled, oplogOnly := cfg.PITR.Enabled, cfg.PITR.OplogOnly
	if profile != "" {
		pitrEnabled, oplogOnly, err = config.IsPITREnabled(ctx, conn)
		if err != nil {
			return nil, errors.Wrap(err, "get PITR status")
		}
	}

	if pitrEnabled && !oplogOnly && len(report.DeleteTargets) != 0 {
		err = report.applyPITRDeleteChecks(ctx, conn, selectedBackups, allBackups)
		if err != nil {
			return nil, errors.Wrap(err, "apply PITR deletion checks")
		}

		if profile == "" && len(report.DeleteTargets) != 0 {
			err = report.applyPITRProtection(ctx, conn, selectedBackups, allBackups)
			if err != nil {
				return nil, errors.Wrap(err, "apply PITR protection")
			}
		}
	}

	report.sortDeleteTargets(selectedBackups)
	return report, nil
}

// buildIncrementalChains groups incremental metadata by its resolved base and
// marks chains unsafe when they cross profiles or data-owning storage boundaries.
func buildIncrementalChains(
	allBackups []backup.BackupMeta,
	selectedNames map[string]struct{},
) []incrementalChain {
	byName := make(map[string]backup.BackupMeta, len(allBackups))
	for _, bcp := range allBackups {
		if bcp.Type != defs.IncrementalBackup {
			continue
		}
		byName[bcp.Name] = bcp
	}

	byBase := make(map[string][]backup.BackupMeta)
	var bases []string
	var invalid []incrementalChain
	for _, bcp := range allBackups {
		if bcp.Type != defs.IncrementalBackup {
			continue
		}

		base, ok := findIncrementalBase(bcp, byName)
		if !ok {
			invalid = append(invalid, incrementalChain{members: []backup.BackupMeta{bcp}})
			continue
		}
		if _, ok := byBase[base.Name]; !ok {
			bases = append(bases, base.Name)
		}
		byBase[base.Name] = append(byBase[base.Name], bcp)
	}

	chains := make([]incrementalChain, 0, len(bases)+len(invalid))
	for _, baseName := range bases {
		base := byName[baseName]
		chain := incrementalChain{members: byBase[baseName], valid: true}
		for _, member := range chain.members {
			if _, ok := selectedNames[member.Name]; !ok {
				chain.valid = false
				break
			}
			if member.Status == defs.StatusDone &&
				!member.Store.IsSameStorage(&base.Store.StorageConf) {
				chain.valid = false
				break
			}
		}
		chains = append(chains, chain)
	}

	return append(chains, invalid...)
}

// findIncrementalBase follows source links to the base. Unresolvable links fail
// closed so lifecycle cleanup does not emit a non-base deletion target.
func findIncrementalBase(
	bcp backup.BackupMeta,
	byName map[string]backup.BackupMeta,
) (backup.BackupMeta, bool) {
	seen := make(map[string]struct{})
	for bcp.SrcBackup != "" {
		if _, ok := seen[bcp.Name]; ok {
			return backup.BackupMeta{}, false
		}
		seen[bcp.Name] = struct{}{}

		parent, ok := byName[bcp.SrcBackup]
		if !ok {
			return backup.BackupMeta{}, false
		}
		bcp = parent
	}

	return bcp, bcp.Name != ""
}

// applyIncrementalChainRules expands keep decisions to complete incremental
// chains and protects chains that cannot be deleted within the selected profile.
func (r *Report) applyIncrementalChainRules(selectedBackups, allBackups []backup.BackupMeta) {
	selectedNames := make(map[string]struct{}, len(selectedBackups))
	for _, bcp := range selectedBackups {
		selectedNames[bcp.Name] = struct{}{}
	}

	chains := buildIncrementalChains(allBackups, selectedNames)
	for _, chain := range chains {
		if !chain.valid {
			for _, member := range chain.members {
				if _, ok := selectedNames[member.Name]; ok && !member.Status.IsRunning() {
					r.addKeepReason(member.Name, invalidIncrementalChainReason)
				}
			}
			continue
		}

		retain := false
		for _, member := range chain.members {
			if len(r.KeepReasons[member.Name]) > 0 || member.Status.IsRunning() {
				retain = true
				break
			}
		}
		if !retain {
			continue
		}

		for _, member := range chain.members {
			if _, ok := selectedNames[member.Name]; !ok || member.Status.IsRunning() {
				continue
			}
			if len(r.KeepReasons[member.Name]) == 0 {
				r.addKeepReason(member.Name, incrementalChainReason)
			}
		}
	}
}

// isMainPITRBaseSnapshot mirrors automatic PITR base selection. A profile
// backup may still be selected explicitly for restore with --base-snapshot.
func isMainPITRBaseSnapshot(bcp backup.BackupMeta) bool {
	return !bcp.Store.IsProfile &&
		bcp.Status == defs.StatusDone &&
		bcp.Type != defs.ExternalBackup &&
		!util.IsSelective(bcp.Namespaces)
}

// applyPITRDeleteChecks ensures lifecycle never proposes a deletion that the
// checked delete-backup path would reject for PITR safety.
func (r *Report) applyPITRDeleteChecks(
	ctx context.Context,
	conn connect.Client,
	selectedBackups, allBackups []backup.BackupMeta,
) error {
	byName := make(map[string]backup.BackupMeta, len(selectedBackups))
	for _, bcp := range selectedBackups {
		byName[bcp.Name] = bcp
	}

	protected := make(map[string]struct{})
	for _, name := range r.DeleteTargets {
		bcp, ok := byName[name]
		if !ok {
			return errors.Errorf("PITR deletion target %q not found", name)
		}

		anchor := bcp.Name
		var err error
		if bcp.Type == defs.IncrementalBackup {
			var increments [][]*backup.BackupMeta
			increments, err = backup.FetchAllIncrements(ctx, conn, &bcp)
			if err == nil {
				err = backup.CanDeleteIncrementalChain(ctx, conn, &bcp, increments)
			}
			for _, attempts := range increments {
				for _, increment := range attempts {
					if increment.Status == defs.StatusDone {
						anchor = increment.Name
					}
				}
			}
		} else {
			err = backup.CanDeleteBackup(ctx, conn, &bcp)
		}

		if err == nil {
			continue
		}
		if !errors.Is(err, backup.ErrBaseForPITR) {
			return errors.Wrapf(err, "check whether backup %q can be deleted", name)
		}
		protected[anchor] = struct{}{}
	}

	if len(protected) == 0 {
		return nil
	}

	anchors := make([]string, 0, len(protected))
	for name := range protected {
		anchors = append(anchors, name)
	}
	slices.Sort(anchors)
	r.protectPITRAnchors(anchors, selectedBackups, allBackups)
	return nil
}

// applyPITRProtection retains the newest deletion unit or tied units when they
// are the only bases for the active main-storage PITR timeline.
func (r *Report) applyPITRProtection(
	ctx context.Context,
	conn connect.Client,
	selectedBackups, allBackups []backup.BackupMeta,
) error {
	candidates, err := r.findPITRBaseCandidates(selectedBackups)
	if err != nil || len(candidates.names) == 0 {
		return err
	}

	timelines, err := oplog.PITRTimelinesBetween(
		ctx,
		conn,
		candidates.previousRestoreTime,
		candidates.restoreTime,
	)
	if err != nil {
		return errors.Wrap(err, "get PITR timelines")
	}
	if !isRequiredPITRBase(candidates.previousRestoreTime, timelines) {
		return nil
	}

	r.protectPITRAnchors(candidates.names, selectedBackups, allBackups)
	return nil
}

func (r *Report) protectPITRAnchors(
	anchorNames []string,
	selectedBackups, allBackups []backup.BackupMeta,
) {
	for _, name := range anchorNames {
		r.addKeepReason(name, pitrBaseSnapshotReason)
	}
	// A protected incremental base retains its complete deletion unit.
	r.applyIncrementalChainRules(selectedBackups, allBackups)
	r.buildBackupLists(selectedBackups)
}

func (r *Report) findPITRBaseCandidates(backups []backup.BackupMeta) (pitrBaseCandidates, error) {
	purged := make(map[string]struct{}, len(r.BackupsPurged))
	for _, name := range r.BackupsPurged {
		purged[name] = struct{}{}
	}

	var latest []backup.BackupMeta
	var latestWrite bson.Timestamp
	for _, bcp := range backups {
		if _, ok := purged[bcp.Name]; !ok {
			continue
		}
		if !isMainPITRBaseSnapshot(bcp) {
			continue
		}

		switch bcp.LastWriteTS.Compare(latestWrite) {
		case 1:
			latestWrite = bcp.LastWriteTS
			latest = []backup.BackupMeta{bcp}
		case 0:
			latest = append(latest, bcp)
		}
	}
	if len(latest) == 0 {
		return pitrBaseCandidates{}, nil
	}

	var previousSnapshot bson.Timestamp
	for _, bcp := range backups {
		if !isMainPITRBaseSnapshot(bcp) {
			continue
		}
		if _, ok := purged[bcp.Name]; ok {
			continue
		}

		switch bcp.LastWriteTS.Compare(latestWrite) {
		case 1:
			// A newer surviving snapshot can base the active PITR timeline.
			return pitrBaseCandidates{}, nil
		case -1:
			if previousSnapshot.Compare(bcp.LastWriteTS) < 0 {
				previousSnapshot = bcp.LastWriteTS
			}
		}
	}

	byName := make(map[string]backup.BackupMeta, len(backups))
	for _, bcp := range backups {
		if bcp.Type == defs.IncrementalBackup {
			byName[bcp.Name] = bcp
		}
	}

	deleteTargets := make(map[string]struct{}, len(r.DeleteTargets))
	for _, name := range r.DeleteTargets {
		deleteTargets[name] = struct{}{}
	}

	anchorNames := make([]string, 0, len(latest))
	for _, bcp := range latest {
		target := bcp.Name
		if bcp.Type == defs.IncrementalBackup {
			base, ok := findIncrementalBase(bcp, byName)
			if !ok {
				return pitrBaseCandidates{},
					errors.Errorf("resolve PITR base for incremental backup %q", bcp.Name)
			}
			target = base.Name
		}
		if _, ok := deleteTargets[target]; !ok {
			return pitrBaseCandidates{}, errors.Errorf("PITR deletion target %q not found", target)
		}

		anchorNames = append(anchorNames, bcp.Name)
	}
	slices.Sort(anchorNames)

	return pitrBaseCandidates{
		names:               anchorNames,
		previousRestoreTime: previousSnapshot,
		restoreTime:         latestWrite,
	}, nil
}

func isRequiredPITRBase(previousRestoreTime bson.Timestamp, timelines []oplog.Timeline) bool {
	return len(timelines) == 1 && previousRestoreTime.T < timelines[0].Start
}

// sortDeleteTargets keeps report targets newest-first so the agent's reverse
// iteration executes deletion units oldest-first by their restore timestamp.
func (r *Report) sortDeleteTargets(backups []backup.BackupMeta) {
	if len(r.DeleteTargets) < 2 {
		return
	}

	targets := make(map[string]struct{}, len(r.DeleteTargets))
	for _, name := range r.DeleteTargets {
		targets[name] = struct{}{}
	}

	byName := make(map[string]backup.BackupMeta, len(backups))
	for _, bcp := range backups {
		if bcp.Type == defs.IncrementalBackup {
			byName[bcp.Name] = bcp
		}
	}

	restoreTS := make(map[string]bson.Timestamp, len(r.DeleteTargets))
	for _, bcp := range backups {
		target := bcp.Name
		if bcp.Type == defs.IncrementalBackup {
			base, ok := findIncrementalBase(bcp, byName)
			if !ok {
				continue
			}
			target = base.Name
		}
		if _, ok := targets[target]; !ok {
			continue
		}
		if bcp.Status == defs.StatusDone && restoreTS[target].Compare(bcp.LastWriteTS) < 0 {
			restoreTS[target] = bcp.LastWriteTS
		}
	}

	slices.SortFunc(r.DeleteTargets, func(a, b string) int {
		if cmp := restoreTS[a].Compare(restoreTS[b]); cmp != 0 {
			return -cmp
		}
		return strings.Compare(b, a)
	})
}

// buildBackupLists materializes the final report after all keep decisions have
// been made. Incremental deletion targets contain only chain bases.
func (r *Report) buildBackupLists(backups []backup.BackupMeta) {
	r.BackupsKept = nil
	r.BackupsPurged = nil
	r.DeleteTargets = nil

	for _, bcp := range backups {
		if bcp.Status.IsRunning() {
			continue
		}

		r.BackupTypes[bcp.Name] = string(bcp.Type)
		if len(r.KeepReasons[bcp.Name]) > 0 {
			r.BackupsKept = append(r.BackupsKept, bcp.Name)
			continue
		}

		r.BackupsPurged = append(r.BackupsPurged, bcp.Name)
		if bcp.Type != defs.IncrementalBackup || bcp.SrcBackup == "" {
			r.DeleteTargets = append(r.DeleteTargets, bcp.Name)
		}
	}
}

func (r *Report) String() string {
	strategy := strings.ToLower(r.ConfigUsed.Strategy)
	if strategy == "" {
		strategy = "rolling" // Default
	}

	weeklyStr := "Auto (Newest in bucket)"
	monthlyStr := "Auto (Newest in bucket)"

	if strategy == "calendar" {
		weeklyStr = fmt.Sprintf("Target Day: %d", r.ConfigUsed.WeeklyDay)
		monthlyStr = fmt.Sprintf("Target Date: %d", r.ConfigUsed.MonthlyDay)
	}

	minKeep := 1
	if r.ConfigUsed.MinKeep != nil {
		minKeep = *r.ConfigUsed.MinKeep
	}

	res := fmt.Sprintf("Lifecycle Report (Dry Run: %v)\n", r.DryRun)
	res += fmt.Sprintf(
		"Enabled: %v | Strategy: %s | Purge Failed: %v | Min Keep: %d\n",
		r.ConfigUsed.Enabled,
		strings.ToUpper(strategy),
		r.ConfigUsed.PurgeFailed,
		minKeep,
	)
	res += fmt.Sprintf("Daily: %d | Weekly: %d [%s] | Monthly: %d [%s]\n\n",
		r.ConfigUsed.DailyRetention,
		r.ConfigUsed.WeeklyRetention, weeklyStr,
		r.ConfigUsed.MonthlyRetention, monthlyStr)

	res += fmt.Sprintf("Backups to KEEP (%d):\n", len(r.BackupsKept))
	for _, b := range r.BackupsKept {
		reasons := strings.Join(r.KeepReasons[b], ", ")
		bType := r.BackupTypes[b] // Fetch the type
		res += fmt.Sprintf("  - %s <%s> [%s]\n", b, bType, reasons)
	}

	res += fmt.Sprintf("\nBackups to PURGE (%d):\n", len(r.BackupsPurged))
	for _, b := range r.BackupsPurged {
		bType := r.BackupTypes[b] // Fetch the type
		res += fmt.Sprintf("  - %s <%s>\n", b, bType)
	}
	return res
}

// Evaluate analyzes backups according to the lifecycle configuration and
// returns a report describing which backups should be kept or purged.
func Evaluate(cfg config.LifecycleConf, backups []backup.BackupMeta, dryRun bool, now time.Time) *Report {
	return evaluate(cfg, backups, backups, dryRun, now)
}

func evaluate(
	cfg config.LifecycleConf,
	selectedBackups []backup.BackupMeta,
	allBackups []backup.BackupMeta,
	dryRun bool,
	now time.Time,
) *Report {
	report := &Report{
		DryRun:      dryRun,
		ConfigUsed:  cfg,
		KeepReasons: make(map[string][]string),
		BackupTypes: make(map[string]string),
	}

	if !cfg.Enabled {
		for _, bcp := range selectedBackups {
			if bcp.Status.IsRunning() {
				continue
			}
			report.addKeepReason(bcp.Name, "Lifecycle Disabled")
		}
		report.buildBackupLists(selectedBackups)
		return report
	}

	isCalendar := strings.ToLower(cfg.Strategy) == "calendar"

	dailyCutoff := now.AddDate(0, 0, -cfg.DailyRetention)
	weeklyCutoff := now.AddDate(0, 0, -(cfg.WeeklyRetention * 7))
	monthlyCutoff := now.AddDate(0, -cfg.MonthlyRetention, 0)

	weeklyCandidates := make(map[string][]backup.BackupMeta)
	monthlyCandidates := make(map[string][]backup.BackupMeta)

	// 1. Bucketing phase
	for _, bcp := range selectedBackups {
		if bcp.Status.IsRunning() {
			continue
		}

		bcpTime := time.Unix(bcp.StartTS, 0).UTC()
		ageInDays := int(now.Sub(bcpTime).Hours() / 24)

		if bcp.Status == defs.StatusError || bcp.Status == defs.StatusCancelled {
			if !cfg.PurgeFailed {
				report.addKeepReason(bcp.Name, "Failed (Protected)")
			} else {
				if cfg.DailyRetention > 0 && !bcpTime.Before(dailyCutoff) {
					report.addKeepReason(bcp.Name, "Failed (Inside Daily Window)")
				}
			}
			continue
		}

		if cfg.DailyRetention > 0 && !bcpTime.Before(dailyCutoff) {
			report.addKeepReason(bcp.Name, "Daily")
			continue
		}

		// Weekly Retention Bucket
		if cfg.WeeklyRetention > 0 && !bcpTime.Before(weeklyCutoff) {
			if isCalendar {
				year, week := bcpTime.ISOWeek()
				// Append the backup Type to the bucket key
				weekKey := fmt.Sprintf("calendar-week-%d-W%02d-%s", year, week, bcp.Type)
				weeklyCandidates[weekKey] = append(weeklyCandidates[weekKey], bcp)
			} else {
				weekBucket := ageInDays / 7
				// Append the backup Type to the bucket key
				weekKey := fmt.Sprintf("rolling-week-%d-%s", weekBucket, bcp.Type)
				weeklyCandidates[weekKey] = append(weeklyCandidates[weekKey], bcp)
			}
		}

		// Monthly Retention Bucket
		if cfg.MonthlyRetention > 0 && !bcpTime.Before(monthlyCutoff) {
			if isCalendar {
				// Append the backup Type to the bucket key
				monthKey := fmt.Sprintf("%s-%s", bcpTime.Format("2006-01"), bcp.Type)
				monthlyCandidates[monthKey] = append(monthlyCandidates[monthKey], bcp)
			} else {
				monthBucket := ageInDays / 30
				// Append the backup Type to the bucket key
				monthKey := fmt.Sprintf("rolling-month-%d-%s", monthBucket, bcp.Type)
				monthlyCandidates[monthKey] = append(monthlyCandidates[monthKey], bcp)
			}
		}
	}

	for _, candidates := range weeklyCandidates {
		bestBcp := findBestCandidate(candidates, cfg.WeeklyDay, false, isCalendar)
		if bestBcp != nil {
			report.addKeepReason(bestBcp.Name, "Weekly")
		}
	}

	for _, candidates := range monthlyCandidates {
		bestBcp := findBestCandidate(candidates, cfg.MonthlyDay, true, isCalendar)
		if bestBcp != nil {
			report.addKeepReason(bestBcp.Name, "Monthly")
		}
	}

	// Account for mandatory chain retention before enforcing minKeep.
	report.applyIncrementalChainRules(selectedBackups, allBackups)

	minKeep := 1
	if cfg.MinKeep != nil {
		minKeep = *cfg.MinKeep
	}

	keptCount := report.countKeptSuccessfulBackups(selectedBackups)
	if minKeep > 0 && keptCount < minKeep {
		var rescue []backup.BackupMeta
		for _, bcp := range selectedBackups {
			if bcp.Status == defs.StatusDone && len(report.KeepReasons[bcp.Name]) == 0 {
				rescue = append(rescue, bcp)
			}
		}

		// Sort newest first to rescue the most recent ones
		slices.SortFunc(rescue, func(a, b backup.BackupMeta) int {
			if a.StartTS > b.StartTS {
				return -1
			}
			if a.StartTS < b.StartTS {
				return 1
			}
			return 0
		})

		for _, bcp := range rescue {
			if keptCount >= minKeep {
				break
			}
			report.addKeepReason(bcp.Name, "Min Keep")
			keptCount++
		}
	}

	// A backup rescued by minKeep may require the rest of its chain.
	report.applyIncrementalChainRules(selectedBackups, allBackups)
	report.buildBackupLists(selectedBackups)

	return report
}

// findBestCandidate selects the optimal backup from a bucket.
func findBestCandidate(
	candidates []backup.BackupMeta,
	targetDayInt int,
	isMonthly bool,
	isCalendar bool,
) *backup.BackupMeta {
	if len(candidates) == 0 {
		return nil
	}

	var best *backup.BackupMeta

	if !isCalendar {
		// Rolling Option: Pick the newest backup in this bucket
		for i := range candidates {
			if best == nil || candidates[i].StartTS > best.StartTS {
				best = &candidates[i]
			}
		}
		return best
	}

	// Calendar Option: Find the backup closest to the targeted day
	minDiff := 31 // Max possible difference
	for i, bcp := range candidates {
		bcpTime := time.Unix(bcp.StartTS, 0).UTC()
		diff := 0

		if isMonthly {
			diff = int(math.Abs(float64(bcpTime.Day() - targetDayInt)))
		} else {
			diff = int(math.Abs(float64(bcpTime.Weekday() - time.Weekday(targetDayInt))))
		}

		if diff < minDiff {
			minDiff = diff
			best = &candidates[i]
		}
	}

	return best
}
