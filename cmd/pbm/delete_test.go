package main

import (
	"testing"

	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/lifecycle"
	"github.com/stretchr/testify/assert"
)

func TestValidateCleanupMode(t *testing.T) {
	tests := []struct {
		name       string
		olderThan  string
		lifecycle  bool
		wantErrMsg string
	}{
		{
			name:       "mode missing",
			wantErrMsg: "either --older-than or --lifecycle should be set",
		},
		{
			name:      "older than",
			olderThan: "30d",
		},
		{
			name:      "lifecycle",
			lifecycle: true,
		},
		{
			name:       "modes conflict",
			olderThan:  "30d",
			lifecycle:  true,
			wantErrMsg: "cannot use --older-than and --lifecycle at the same command",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCleanupMode(tt.olderThan, tt.lifecycle)
			if tt.wantErrMsg == "" {
				assert.NoError(t, err)
				return
			}

			assert.EqualError(t, err, tt.wantErrMsg)
		})
	}
}

func TestLifecycleConfirmationQuestion(t *testing.T) {
	prompt := false
	minKeep := 2
	report := &lifecycle.Report{
		ConfigUsed: config.LifecycleConf{
			Prompt:  &prompt,
			MinKeep: &minKeep,
		},
		BackupsKept: []string{"backup-1", "backup-2"},
	}

	assert.Equal(t,
		"Are you sure you want to permanently delete the purged backups?",
		lifecycleConfirmationQuestion(report, false),
		"lifecycle.prompt must not suppress cleanup confirmation",
	)
	assert.Empty(t, lifecycleConfirmationQuestion(report, true), "--yes must suppress confirmation")

	report.BackupsKept = report.BackupsKept[:1]
	assert.Equal(t,
		"This rotation would leave 1 backup(s), below minKeep 2. Force deletion?",
		lifecycleConfirmationQuestion(report, false),
	)
}
