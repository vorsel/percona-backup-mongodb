package backup

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/percona/percona-backup-mongodb/x/pbm/config"
	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
	"github.com/percona/percona-backup-mongodb/x/pbm/storage/factory"
)

// StorageResyncer rebuilds and persists the backup metadata list from a storage.
type StorageResyncer struct {
	ccDB *clientv3.Client
}

// NewStorageResyncer creates a resyncer.
func NewStorageResyncer(ccDB *clientv3.Client) *StorageResyncer {
	return &StorageResyncer{ccDB: ccDB}
}

// Resync builds the storage described by config and rebuilds its backup metadata
// list in etcd.
func (r *StorageResyncer) Resync(ctx context.Context, stg *config.StorageConf) error {
	newStg, err := factory.Create(stg)
	if err != nil {
		return errors.Wrap(err, "create storage")
	}

	repo := New(r.ccDB, newStg)
	return repo.SyncBackupList(ctx)
}
