package lifecycle

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	storagefs "github.com/percona/percona-backup-mongodb/pbm/storage/fs"
)

// mockBcp is a helper to generate fake backups.
// daysAgo subtracts from the baseTime (our frozen mockNow).
func mockBcp(name string, daysAgo int, baseTime time.Time, status defs.Status) backup.BackupMeta {
	bcpTime := baseTime.AddDate(0, 0, -daysAgo)
	return backup.BackupMeta{
		Name:    name,
		StartTS: bcpTime.Unix(),
		Status:  status,
	}
}

// mockTypedBcp is a helper to generate fake backups with a specific Type.
func mockTypedBcp(
	name string,
	daysAgo int,
	baseTime time.Time,
	status defs.Status,
	bcpType defs.BackupType,
) backup.BackupMeta {
	bcp := mockBcp(name, daysAgo, baseTime, status)
	bcp.Type = bcpType
	return bcp
}

func mockIncrementalBcp(
	name string,
	source string,
	daysAgo int,
	baseTime time.Time,
	status defs.Status,
) backup.BackupMeta {
	bcp := mockTypedBcp(name, daysAgo, baseTime, status, defs.IncrementalBackup)
	bcp.SrcBackup = source
	bcp.Store.StorageConf = config.StorageConf{
		Type:       storage.Filesystem,
		Filesystem: &storagefs.Config{Path: "/backups"},
	}
	return bcp
}

func TestFilterBackupsByProfile(t *testing.T) {
	backups := []backup.BackupMeta{
		{Name: "main"},
		{Name: "archive", Store: backup.Storage{Name: "archive", IsProfile: true}},
		{Name: "other", Store: backup.Storage{Name: "other", IsProfile: true}},
	}

	tests := []struct {
		name    string
		profile string
		want    []string
	}{
		{
			name: "main storage",
			want: []string{"main"},
		},
		{
			name:    "named profile",
			profile: "archive",
			want:    []string{"archive"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterBackupsByProfile(backups, tt.profile)
			gotNames := make([]string, len(got))
			for i := range got {
				gotNames[i] = got[i].Name
			}

			if !reflect.DeepEqual(gotNames, tt.want) {
				t.Errorf("filterBackupsByProfile() = %v, want %v", gotNames, tt.want)
			}
		})
	}
}

func TestEvaluate(t *testing.T) {
	// Define a few specific dates we want to test against
	standardDate := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)     // A normal Thursday
	leapYearDate := time.Date(2024, time.February, 29, 12, 0, 0, 0, time.UTC)  // Leap Day
	endOfYearDate := time.Date(2025, time.December, 31, 12, 0, 0, 0, time.UTC) // Dec 31st

	tests := []struct {
		name           string
		cfg            config.LifecycleConf
		backups        []backup.BackupMeta
		dryRun         bool
		mockNow        time.Time
		expectedKept   []string
		expectedPurged []string
	}{
		// --- STANDARD DATE SCENARIOS ---
		{
			name: "Feature Disabled (Dry run or not, it sleeps)",
			cfg: config.LifecycleConf{
				Enabled:        false,
				DailyRetention: 1,
			},
			mockNow: standardDate,
			backups: []backup.BackupMeta{
				mockBcp("bcp-today", 0, standardDate, defs.StatusDone),
				mockBcp("bcp-old", 10, standardDate, defs.StatusDone),
			},
			dryRun:         true,
			expectedKept:   []string{"bcp-today", "bcp-old"},
			expectedPurged: []string{},
		},
		{
			name: "Rolling Strategy - Basic GFS (7 Daily, 4 Weekly)",
			cfg: config.LifecycleConf{
				Enabled:         true,
				Strategy:        "rolling",
				DailyRetention:  7,
				WeeklyRetention: 4,
			},
			mockNow: standardDate,
			backups: []backup.BackupMeta{
				mockBcp("bcp-today", 0, standardDate, defs.StatusDone),
				mockBcp("bcp-7-days", 7, standardDate, defs.StatusDone),
				mockBcp("bcp-9-days", 9, standardDate, defs.StatusDone),
				mockBcp("bcp-12-days", 12, standardDate, defs.StatusDone), // Oldest in Week 1 bucket (Purged)
			},
			dryRun:         false,
			expectedKept:   []string{"bcp-today", "bcp-7-days", "bcp-9-days"},
			expectedPurged: []string{"bcp-12-days"},
		},
		{
			name: "Calendar Strategy - Exact match vs Nearest Neighbor",
			cfg: config.LifecycleConf{
				Enabled:          true,
				Strategy:         "calendar",
				DailyRetention:   0,
				MonthlyRetention: 1,
				MonthlyDay:       15, // Target the 15th
			},
			mockNow: standardDate, // March 26
			backups: []backup.BackupMeta{
				mockBcp("bcp-mar-13", 13, standardDate, defs.StatusDone), // 13 days ago (Mar 13, diff 2)
				mockBcp("bcp-mar-15", 11, standardDate, defs.StatusDone), // 11 days ago (Mar 15, Exact Match!)
			},
			dryRun:         false,
			expectedKept:   []string{"bcp-mar-15"},
			expectedPurged: []string{"bcp-mar-13"},
		},

		// --- LEAP YEAR SCENARIOS ---
		{
			name: "Leap Year - Daily Retention over Feb 29",
			cfg: config.LifecycleConf{
				Enabled:        true,
				Strategy:       "rolling",
				DailyRetention: 3,
			},
			mockNow: leapYearDate, // Feb 29, 2024
			backups: []backup.BackupMeta{
				mockBcp("bcp-feb-29", 0, leapYearDate, defs.StatusDone),
				mockBcp("bcp-feb-28", 1, leapYearDate, defs.StatusDone),
				mockBcp("bcp-feb-27", 2, leapYearDate, defs.StatusDone),
				mockBcp("bcp-feb-25", 4, leapYearDate, defs.StatusDone), // 4 days ago, Purged
			},
			dryRun:         false,
			expectedKept:   []string{"bcp-feb-29", "bcp-feb-28", "bcp-feb-27"},
			expectedPurged: []string{"bcp-feb-25"},
		},

		// --- END OF YEAR SCENARIOS ---
		{
			name: "End of Year - Monthly Nearest Neighbor across year boundary",
			cfg: config.LifecycleConf{
				Enabled:          true,
				Strategy:         "calendar",
				DailyRetention:   0,
				MonthlyRetention: 1,
				MonthlyDay:       1, // Target the 1st of the month
			},
			mockNow: endOfYearDate, // Dec 31, 2025
			backups: []backup.BackupMeta{
				mockBcp("bcp-dec-2", 29, endOfYearDate, defs.StatusDone),  // Dec 2 (Diff 1)
				mockBcp("bcp-nov-29", 32, endOfYearDate, defs.StatusDone), // Nov 29 (Diff 2)
			},
			dryRun:         false,
			expectedKept:   []string{"bcp-dec-2"},
			expectedPurged: []string{"bcp-nov-29"},
		},

		// --- STATE HANDLING SCENARIOS ---
		{
			name: "In-Progress Backups are ALWAYS protected (and MinKeep rescues the last safe base)",
			cfg: config.LifecycleConf{
				Enabled:        true,
				DailyRetention: 1,
			},
			mockNow: standardDate,
			backups: []backup.BackupMeta{
				mockBcp("bcp-running", 0, standardDate, defs.StatusRunning), // In-progress today
				mockBcp("bcp-done-old", 50, standardDate, defs.StatusDone),  // 50 days old, normally purged
			},
			dryRun: false,
			// bcp-running is implicitly protected (hidden from purge).
			// bcp-done-old is expired, but rescued because MinKeep defaults to 1 and the running backup doesn't count yet!
			expectedKept:   []string{"bcp-done-old"},
			expectedPurged: []string{},
		},
		{
			name: "Failed Backups - PurgeFailed is TRUE",
			cfg: config.LifecycleConf{
				Enabled:        true,
				PurgeFailed:    true,
				DailyRetention: 7, // Protect for 7 days
			},
			mockNow: standardDate,
			backups: []backup.BackupMeta{
				mockBcp("bcp-error-recent", 3, standardDate, defs.StatusError), // Kept
				mockBcp("bcp-error-old", 10, standardDate, defs.StatusError),   // Purged
			},
			dryRun:         false,
			expectedKept:   []string{"bcp-error-recent"},
			expectedPurged: []string{"bcp-error-old"},
		},
		{
			name: "Type-Aware Bucketing - Keeps both Physical and Logical for the same week",
			cfg: config.LifecycleConf{
				Enabled:         true,
				Strategy:        "rolling",
				DailyRetention:  0, // Disable daily to force Weekly evaluation
				WeeklyRetention: 2,
			},
			mockNow: standardDate,
			backups: []backup.BackupMeta{
				// Week 1 Bucket (7-13 days ago)
				mockTypedBcp("phys-newer", 9, standardDate, defs.StatusDone, defs.PhysicalBackup),
				mockTypedBcp("phys-older", 12, standardDate, defs.StatusDone, defs.PhysicalBackup), // Loses to phys-newer

				// Same Week 1 Bucket, but Logical
				mockTypedBcp("logical-only", 11, standardDate, defs.StatusDone, defs.LogicalBackup), // Wins its own logical bucket
			},
			dryRun:         false,
			expectedKept:   []string{"phys-newer", "logical-only"},
			expectedPurged: []string{"phys-older"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := Evaluate(tt.cfg, tt.backups, tt.dryRun, tt.mockNow)

			if report.DryRun != tt.dryRun {
				t.Errorf("Report.DryRun = %v, want %v", report.DryRun, tt.dryRun)
			}

			sort.Strings(report.BackupsKept)
			sort.Strings(tt.expectedKept)
			sort.Strings(report.BackupsPurged)
			sort.Strings(tt.expectedPurged)

			if len(report.BackupsKept) == 0 && len(tt.expectedKept) == 0 {
				// both empty, pass
			} else if !reflect.DeepEqual(report.BackupsKept, tt.expectedKept) {
				t.Errorf("BackupsKept = %v, want %v", report.BackupsKept, tt.expectedKept)
			}

			if len(report.BackupsPurged) == 0 && len(tt.expectedPurged) == 0 {
				// both empty, pass
			} else if !reflect.DeepEqual(report.BackupsPurged, tt.expectedPurged) {
				t.Errorf("BackupsPurged = %v, want %v", report.BackupsPurged, tt.expectedPurged)
			}
		})
	}
}

func TestEvaluateRetainsCompleteIncrementalChain(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 0
	report := Evaluate(config.LifecycleConf{
		Enabled:        true,
		DailyRetention: 3,
		MinKeep:        &minKeep,
	}, []backup.BackupMeta{
		mockIncrementalBcp("inc-2", "inc-1", 1, now, defs.StatusDone),
		mockIncrementalBcp("inc-1", "base", 10, now, defs.StatusDone),
		mockIncrementalBcp("base", "", 30, now, defs.StatusDone),
	}, false, now)

	wantKept := []string{"inc-2", "inc-1", "base"}
	if !reflect.DeepEqual(report.BackupsKept, wantKept) {
		t.Fatalf("BackupsKept = %v, want %v", report.BackupsKept, wantKept)
	}
	if len(report.BackupsPurged) != 0 {
		t.Fatalf("BackupsPurged = %v, want none", report.BackupsPurged)
	}
	if len(report.DeleteTargets) != 0 {
		t.Fatalf("DeleteTargets = %v, want none", report.DeleteTargets)
	}
	if !reflect.DeepEqual(report.KeepReasons["inc-2"], []string{"Daily"}) {
		t.Errorf("inc-2 reasons = %v, want Daily", report.KeepReasons["inc-2"])
	}
	for _, name := range []string{"inc-1", "base"} {
		if !reflect.DeepEqual(report.KeepReasons[name], []string{incrementalChainReason}) {
			t.Errorf("%s reasons = %v, want %q", name, report.KeepReasons[name], incrementalChainReason)
		}
	}
}

func TestEvaluateRetainedBaseKeepsNewerIncrements(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 0
	report := Evaluate(config.LifecycleConf{
		Enabled:         true,
		Strategy:        "rolling",
		WeeklyRetention: 6,
		MinKeep:         &minKeep,
	}, []backup.BackupMeta{
		mockIncrementalBcp("other-inc", "other-base", 8, now, defs.StatusDone),
		mockIncrementalBcp("inc-2", "inc-1", 9, now, defs.StatusDone),
		mockIncrementalBcp("inc-1", "base", 10, now, defs.StatusDone),
		mockIncrementalBcp("other-base", "", 20, now, defs.StatusDone),
		mockIncrementalBcp("base", "", 35, now, defs.StatusDone),
	}, false, now)

	wantKept := []string{"other-inc", "inc-2", "inc-1", "other-base", "base"}
	if !reflect.DeepEqual(report.BackupsKept, wantKept) {
		t.Fatalf("BackupsKept = %v, want %v", report.BackupsKept, wantKept)
	}
	if !reflect.DeepEqual(report.KeepReasons["base"], []string{"Weekly"}) {
		t.Errorf("base reasons = %v, want Weekly", report.KeepReasons["base"])
	}
	for _, name := range []string{"inc-1", "inc-2"} {
		if !reflect.DeepEqual(report.KeepReasons[name], []string{incrementalChainReason}) {
			t.Errorf("%s reasons = %v, want %q", name, report.KeepReasons[name], incrementalChainReason)
		}
	}
	if len(report.DeleteTargets) != 0 {
		t.Fatalf("DeleteTargets = %v, want none", report.DeleteTargets)
	}
}

func TestEvaluatePurgesIncrementalChainThroughRoot(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 0
	report := Evaluate(config.LifecycleConf{
		Enabled:     true,
		PurgeFailed: true,
		MinKeep:     &minKeep,
	}, []backup.BackupMeta{
		mockIncrementalBcp("inc-1", "base", 10, now, defs.StatusDone),
		mockIncrementalBcp("failed-attempt", "base", 12, now, defs.StatusError),
		mockTypedBcp("logical", 20, now, defs.StatusDone, defs.LogicalBackup),
		mockIncrementalBcp("base", "", 30, now, defs.StatusDone),
	}, false, now)

	wantPurged := []string{"inc-1", "failed-attempt", "logical", "base"}
	if !reflect.DeepEqual(report.BackupsPurged, wantPurged) {
		t.Fatalf("BackupsPurged = %v, want %v", report.BackupsPurged, wantPurged)
	}
	wantTargets := []string{"logical", "base"}
	if !reflect.DeepEqual(report.DeleteTargets, wantTargets) {
		t.Fatalf("DeleteTargets = %v, want %v", report.DeleteTargets, wantTargets)
	}
}

func TestEvaluateProtectedIncrementRetainsChain(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 0

	t.Run("failed attempt", func(t *testing.T) {
		report := Evaluate(config.LifecycleConf{
			Enabled: true,
			MinKeep: &minKeep,
		}, []backup.BackupMeta{
			mockIncrementalBcp("failed-attempt", "base", 10, now, defs.StatusError),
			mockIncrementalBcp("base", "", 30, now, defs.StatusDone),
		}, false, now)

		wantKept := []string{"failed-attempt", "base"}
		if !reflect.DeepEqual(report.BackupsKept, wantKept) {
			t.Fatalf("BackupsKept = %v, want %v", report.BackupsKept, wantKept)
		}
		if len(report.DeleteTargets) != 0 {
			t.Fatalf("DeleteTargets = %v, want none", report.DeleteTargets)
		}
	})

	t.Run("running increment on different storage", func(t *testing.T) {
		backups := []backup.BackupMeta{
			mockIncrementalBcp("running", "base", 0, now, defs.StatusRunning),
			mockIncrementalBcp("base", "", 30, now, defs.StatusDone),
		}
		backups[0].Store.Filesystem.Path = "/other-backups"
		backups[0].PBMVersion = "2.15.0"
		report := Evaluate(config.LifecycleConf{
			Enabled:     true,
			PurgeFailed: true,
			MinKeep:     &minKeep,
		}, backups, false, now)

		if !reflect.DeepEqual(report.BackupsKept, []string{"base"}) {
			t.Fatalf("BackupsKept = %v, want base", report.BackupsKept)
		}
		if len(report.DeleteTargets) != 0 {
			t.Fatalf("DeleteTargets = %v, want none", report.DeleteTargets)
		}
		if _, ok := report.KeepReasons["running"]; ok {
			t.Errorf("running backup should not have a keep reason")
		}
		if _, ok := report.BackupTypes["running"]; ok {
			t.Errorf("running backup should not have a reported type")
		}
	})
}

func TestEvaluateMinKeepRetainsCompleteIncrementalChain(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	report := Evaluate(config.LifecycleConf{Enabled: true}, []backup.BackupMeta{
		mockIncrementalBcp("inc-1", "base", 10, now, defs.StatusDone),
		mockIncrementalBcp("base", "", 30, now, defs.StatusDone),
	}, false, now)

	if !reflect.DeepEqual(report.BackupsKept, []string{"inc-1", "base"}) {
		t.Fatalf("BackupsKept = %v, want complete chain", report.BackupsKept)
	}
	if !reflect.DeepEqual(report.KeepReasons["inc-1"], []string{"Min Keep"}) {
		t.Errorf("inc-1 reasons = %v, want Min Keep", report.KeepReasons["inc-1"])
	}
	if !reflect.DeepEqual(report.KeepReasons["base"], []string{incrementalChainReason}) {
		t.Errorf("base reasons = %v, want %q", report.KeepReasons["base"], incrementalChainReason)
	}
	if len(report.DeleteTargets) != 0 {
		t.Fatalf("DeleteTargets = %v, want none", report.DeleteTargets)
	}

}

func TestEvaluateMinKeepCountsOnlySuccessfulBackups(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 2
	report := Evaluate(config.LifecycleConf{
		Enabled: true,
		MinKeep: &minKeep,
	}, []backup.BackupMeta{
		mockTypedBcp("failed", 1, now, defs.StatusError, defs.LogicalBackup),
		mockTypedBcp("canceled", 2, now, defs.StatusCancelled, defs.LogicalBackup),
		mockTypedBcp("success-new", 10, now, defs.StatusDone, defs.LogicalBackup),
		mockTypedBcp("success-old", 20, now, defs.StatusDone, defs.LogicalBackup),
	}, false, now)

	wantKept := []string{"failed", "canceled", "success-new", "success-old"}
	if !reflect.DeepEqual(report.BackupsKept, wantKept) {
		t.Fatalf("BackupsKept = %v, want %v", report.BackupsKept, wantKept)
	}
	for _, name := range []string{"success-new", "success-old"} {
		if !reflect.DeepEqual(report.KeepReasons[name], []string{"Min Keep"}) {
			t.Errorf("%s reasons = %v, want Min Keep", name, report.KeepReasons[name])
		}
	}
	if len(report.BackupsPurged) != 0 || len(report.DeleteTargets) != 0 {
		t.Fatalf("minKeep should preserve all successful backups: %+v", report)
	}
}

func TestEvaluateMinKeepCountsMandatoryChainRetention(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 1
	report := Evaluate(config.LifecycleConf{
		Enabled: true,
		MinKeep: &minKeep,
	}, []backup.BackupMeta{
		mockTypedBcp("standalone", 5, now, defs.StatusDone, defs.LogicalBackup),
		mockIncrementalBcp("failed-attempt", "base", 10, now, defs.StatusError),
		mockIncrementalBcp("base", "", 30, now, defs.StatusDone),
	}, false, now)

	wantKept := []string{"failed-attempt", "base"}
	if !reflect.DeepEqual(report.BackupsKept, wantKept) {
		t.Fatalf("BackupsKept = %v, want %v", report.BackupsKept, wantKept)
	}
	if !reflect.DeepEqual(report.BackupsPurged, []string{"standalone"}) {
		t.Fatalf("BackupsPurged = %v, want standalone", report.BackupsPurged)
	}
	if !reflect.DeepEqual(report.DeleteTargets, []string{"standalone"}) {
		t.Fatalf("DeleteTargets = %v, want standalone", report.DeleteTargets)
	}
	if !reflect.DeepEqual(report.KeepReasons["base"], []string{incrementalChainReason}) {
		t.Errorf("base reasons = %v, want %q", report.KeepReasons["base"], incrementalChainReason)
	}
}

func TestEvaluateProtectsInvalidIncrementalChain(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 0
	tests := []struct {
		name        string
		backups     []backup.BackupMeta
		wantKept    []string
		wantReasons map[string][]string
	}{
		{
			name: "missing parent",
			backups: []backup.BackupMeta{
				mockIncrementalBcp("orphan", "missing", 1, now, defs.StatusDone),
			},
			wantKept: []string{"orphan"},
			wantReasons: map[string][]string{
				"orphan": {"Daily", invalidIncrementalChainReason},
			},
		},
		{
			name: "cycle",
			backups: []backup.BackupMeta{
				mockIncrementalBcp("inc-b", "inc-a", 10, now, defs.StatusDone),
				mockIncrementalBcp("inc-a", "inc-b", 11, now, defs.StatusDone),
			},
			wantKept: []string{"inc-b", "inc-a"},
			wantReasons: map[string][]string{
				"inc-b": {invalidIncrementalChainReason},
				"inc-a": {invalidIncrementalChainReason},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := Evaluate(config.LifecycleConf{
				Enabled:        true,
				DailyRetention: 3,
				MinKeep:        &minKeep,
			}, tt.backups, false, now)

			if !reflect.DeepEqual(report.BackupsKept, tt.wantKept) {
				t.Fatalf("BackupsKept = %v, want %v", report.BackupsKept, tt.wantKept)
			}
			for name, want := range tt.wantReasons {
				if !reflect.DeepEqual(report.KeepReasons[name], want) {
					t.Errorf("%s reasons = %v, want %v", name, report.KeepReasons[name], want)
				}
			}
			if len(report.DeleteTargets) != 0 {
				t.Fatalf("DeleteTargets = %v, want none", report.DeleteTargets)
			}
		})
	}
}

func TestEvaluateProtectsChainSplitAcrossProfiles(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 0
	allBackups := []backup.BackupMeta{
		mockIncrementalBcp("archive-inc", "base", 10, now, defs.StatusDone),
		mockIncrementalBcp("base", "", 30, now, defs.StatusDone),
	}
	allBackups[0].Store.Name = "archive"
	allBackups[0].Store.IsProfile = true
	mainBackups := filterBackupsByProfile(allBackups, "")
	report := evaluate(config.LifecycleConf{
		Enabled: true,
		MinKeep: &minKeep,
	}, mainBackups, allBackups, false, now)

	if !reflect.DeepEqual(report.BackupsKept, []string{"base"}) {
		t.Fatalf("BackupsKept = %v, want base", report.BackupsKept)
	}
	if !reflect.DeepEqual(report.KeepReasons["base"], []string{invalidIncrementalChainReason}) {
		t.Errorf("base reasons = %v, want %q", report.KeepReasons["base"], invalidIncrementalChainReason)
	}
	if len(report.DeleteTargets) != 0 {
		t.Fatalf("DeleteTargets = %v, want none", report.DeleteTargets)
	}

	archiveBackups := filterBackupsByProfile(allBackups, "archive")
	archiveReport := evaluate(config.LifecycleConf{
		Enabled: true,
		MinKeep: &minKeep,
	}, archiveBackups, allBackups, false, now)
	if !reflect.DeepEqual(archiveReport.BackupsKept, []string{"archive-inc"}) {
		t.Fatalf("archive BackupsKept = %v, want archive-inc", archiveReport.BackupsKept)
	}
	if !reflect.DeepEqual(
		archiveReport.KeepReasons["archive-inc"],
		[]string{invalidIncrementalChainReason},
	) {
		t.Errorf(
			"archive-inc reasons = %v, want %q",
			archiveReport.KeepReasons["archive-inc"],
			invalidIncrementalChainReason,
		)
	}
	if len(archiveReport.DeleteTargets) != 0 {
		t.Fatalf("archive DeleteTargets = %v, want none", archiveReport.DeleteTargets)
	}
}

func TestEvaluateProtectsSuccessfulChainSplitAcrossStorageLocations(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 0
	backups := []backup.BackupMeta{
		mockIncrementalBcp("inc-1", "base", 10, now, defs.StatusDone),
		mockIncrementalBcp("base", "", 30, now, defs.StatusDone),
	}
	backups[0].Store.Filesystem.Path = "/other-backups"

	report := Evaluate(config.LifecycleConf{
		Enabled: true,
		MinKeep: &minKeep,
	}, backups, false, now)

	wantKept := []string{"inc-1", "base"}
	if !reflect.DeepEqual(report.BackupsKept, wantKept) {
		t.Fatalf("BackupsKept = %v, want %v", report.BackupsKept, wantKept)
	}
	for _, name := range wantKept {
		if !reflect.DeepEqual(report.KeepReasons[name], []string{invalidIncrementalChainReason}) {
			t.Errorf("%s reasons = %v, want %q", name, report.KeepReasons[name], invalidIncrementalChainReason)
		}
	}
	if len(report.DeleteTargets) != 0 {
		t.Fatalf("DeleteTargets = %v, want none", report.DeleteTargets)
	}
}

func TestEvaluateAllowsFailedAttemptOnDifferentStorage(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 0
	backups := []backup.BackupMeta{
		mockIncrementalBcp("failed-attempt", "base", 10, now, defs.StatusError),
		mockIncrementalBcp("base", "", 30, now, defs.StatusDone),
	}
	backups[0].Store.Filesystem.Path = "/other-backups"
	backups[0].PBMVersion = "2.15.0"

	report := Evaluate(config.LifecycleConf{
		Enabled:     true,
		PurgeFailed: true,
		MinKeep:     &minKeep,
	}, backups, false, now)

	wantPurged := []string{"failed-attempt", "base"}
	if !reflect.DeepEqual(report.BackupsPurged, wantPurged) {
		t.Fatalf("BackupsPurged = %v, want %v", report.BackupsPurged, wantPurged)
	}
	if !reflect.DeepEqual(report.DeleteTargets, []string{"base"}) {
		t.Fatalf("DeleteTargets = %v, want base", report.DeleteTargets)
	}
	if len(report.KeepReasons) != 0 {
		t.Fatalf("KeepReasons = %v, want none", report.KeepReasons)
	}
}

func TestEvaluateProtectsLegacyFailedAttemptOnDifferentStorage(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 0
	backups := []backup.BackupMeta{
		mockIncrementalBcp("failed-attempt", "base", 10, now, defs.StatusError),
		mockIncrementalBcp("base", "", 30, now, defs.StatusDone),
	}
	backups[0].Store.Filesystem.Path = "/other-backups"
	backups[0].PBMVersion = "2.5.0"

	report := Evaluate(config.LifecycleConf{
		Enabled:     true,
		PurgeFailed: true,
		MinKeep:     &minKeep,
	}, backups, false, now)

	wantKept := []string{"failed-attempt", "base"}
	if !reflect.DeepEqual(report.BackupsKept, wantKept) {
		t.Fatalf("BackupsKept = %v, want %v", report.BackupsKept, wantKept)
	}
	if len(report.BackupsPurged) != 0 || len(report.DeleteTargets) != 0 {
		t.Fatalf("legacy cross-storage chain should be protected: %+v", report)
	}
}

func TestEvaluateDisabledIncrementalChain(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	report := Evaluate(config.LifecycleConf{}, []backup.BackupMeta{
		mockIncrementalBcp("inc-1", "base", 10, now, defs.StatusDone),
		mockIncrementalBcp("base", "", 30, now, defs.StatusDone),
	}, false, now)

	wantKept := []string{"inc-1", "base"}
	if !reflect.DeepEqual(report.BackupsKept, wantKept) {
		t.Fatalf("BackupsKept = %v, want %v", report.BackupsKept, wantKept)
	}
	if len(report.BackupsPurged) != 0 || len(report.DeleteTargets) != 0 {
		t.Fatalf("disabled lifecycle should not purge backups: %+v", report)
	}
	for _, name := range wantKept {
		if !reflect.DeepEqual(report.KeepReasons[name], []string{"Lifecycle Disabled"}) {
			t.Errorf("%s reasons = %v, want Lifecycle Disabled", name, report.KeepReasons[name])
		}
	}
}

func TestReportHidesDeleteTargetsFromJSON(t *testing.T) {
	b, err := json.Marshal(Report{
		BackupsPurged: []string{"base", "inc-1"},
		DeleteTargets: []string{"base"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "DeleteTargets") || strings.Contains(string(b), "deleteTargets") {
		t.Fatalf("internal deletion targets leaked into JSON: %s", b)
	}
}
