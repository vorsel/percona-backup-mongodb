package backup

import (
	"context"
	"encoding/json"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/percona/percona-backup-mongodb/x/pbm/defs"
	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
	"github.com/percona/percona-backup-mongodb/x/pbm/storage"
)

const keyPrefix = "/pbm/backups/"

var (
	ErrNotFound      = errors.New("backup not found")
	ErrAlreadyExists = errors.New("backup already exists")
	ErrNoName        = errors.New("backup name is empty")
)

// Repo is the backup repository.
// It manages backup metadata documents persisted in etcd as part of PBM's
// control-collection state. Each document is stored under
// "/pbm/backups/{name}" with a JSON-encoded BackupMeta value.
type Repo struct {
	ccDB *clientv3.Client
	stg  storage.Storage
}

// New creates a backup repository. ccDB persists the metadata; stg is the
// backup storage used to (re)sync the metadata list from.
func New(ccDB *clientv3.Client, stg storage.Storage) *Repo {
	return &Repo{
		ccDB: ccDB,
		stg:  stg,
	}
}

// Get returns the backup metadata under the given name.
func (r *Repo) Get(ctx context.Context, name string) (*BackupMeta, error) {
	if name == "" {
		return nil, ErrNoName
	}

	resp, err := r.ccDB.Get(ctx, key(name))
	if err != nil {
		return nil, errors.Wrap(err, "get backup meta")
	}
	if len(resp.Kvs) == 0 {
		return nil, ErrNotFound
	}

	meta := &BackupMeta{}
	if err := json.Unmarshal(resp.Kvs[0].Value, meta); err != nil {
		return nil, errors.Wrap(err, "unmarshal backup meta")
	}

	return meta, nil
}

// GetAll returns every stored backup metadata, ordered by name ascending.
// As backup names are timestamps, this yields chronological order.
// It returns an empty slice when no backups exist.
func (r *Repo) GetAll(ctx context.Context) ([]*BackupMeta, error) {
	resp, err := r.ccDB.Get(ctx, keyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Wrap(err, "get backups")
	}

	out := make([]*BackupMeta, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		meta := &BackupMeta{}
		if err := json.Unmarshal(kv.Value, meta); err != nil {
			return nil, errors.Wrapf(err, "unmarshal backup meta %s", string(kv.Key))
		}
		out = append(out, meta)
	}

	return out, nil
}

// Insert stores a new backup metadata document.
// It returns ErrAlreadyExists if a backup with that name is already present.
func (r *Repo) Insert(ctx context.Context, meta *BackupMeta) error {
	if meta.Name == "" {
		return ErrNoName
	}

	data, err := json.Marshal(meta)
	if err != nil {
		return errors.Wrap(err, "marshal backup")
	}

	k := key(meta.Name)
	resp, err := r.ccDB.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(k), "=", 0)).
		Then(clientv3.OpPut(k, string(data))).
		Commit()
	if err != nil {
		return errors.Wrap(err, "insert backup meta")
	}
	if !resp.Succeeded {
		return ErrAlreadyExists
	}

	return nil
}

// Update replaces the backup metadata document identified by meta.Name.
// It returns ErrNotFound if no such backup exists.
func (r *Repo) Update(ctx context.Context, meta *BackupMeta) error {
	if meta.Name == "" {
		return ErrNoName
	}

	data, err := json.Marshal(meta)
	if err != nil {
		return errors.Wrap(err, "marshal backup")
	}

	k := key(meta.Name)
	resp, err := r.ccDB.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(k), ">", 0)).
		Then(clientv3.OpPut(k, string(data))).
		Commit()
	if err != nil {
		return errors.Wrap(err, "put backup")
	}
	if !resp.Succeeded {
		return ErrNotFound
	}

	return nil
}

// Delete removes the backup metadata document.
// It returns ErrNotFound if no such backup exists.
func (r *Repo) Delete(ctx context.Context, name string) error {
	if name == "" {
		return ErrNoName
	}

	resp, err := r.ccDB.Delete(ctx, key(name))
	if err != nil {
		return errors.Wrap(err, "delete backup")
	}
	if resp.Deleted == 0 {
		return ErrNotFound
	}

	return nil
}

// DeleteAll removes every stored backup metadata document and returns the
// number of backups deleted.
func (r *Repo) DeleteAll(ctx context.Context) (int64, error) {
	resp, err := r.ccDB.Delete(ctx, keyPrefix, clientv3.WithPrefix())
	if err != nil {
		return 0, errors.Wrap(err, "delete backups")
	}

	return resp.Deleted, nil
}

// key resolves the backup name to its etcd key.
func key(name string) string {
	return keyPrefix + name
}

// SyncBackupList rebuilds the metadata list in etcd from the backup storage:
// it drops all stored metadata and re-inserts one document per backup found.
func (r *Repo) SyncBackupList(ctx context.Context) error {
	// todo: add events:
	// 	l.Info("syncing backup list for main storage")
	// 	l.Info("syncing backup list for profile %q", profile)

	cntDeleted, err := r.DeleteAll(ctx)
	if err != nil {
		return errors.Wrapf(err, "clear backup list")
	}
	fmt.Printf("deleted %d backup metadata docs\n", cntDeleted)

	backupList, err := r.getAllBackupMetaFromStorage()
	if err != nil {
		return errors.Wrap(err, "get all backups meta from the storage")
	}

	fmt.Printf("got backups list: %d\n", len(backupList))

	if len(backupList) == 0 {
		return nil
	}

	for _, backupMeta := range backupList {
		fmt.Printf("backup: %s, size=%d\n", backupMeta.Name, backupMeta.Size)
		err = r.Insert(ctx, backupMeta)
		if err != nil {
			return errors.Wrapf(err, "insert backup meta %q", backupMeta.Name)
		}
	}

	return nil
}

// getAllBackupMetaFromStorage reads every backup metadata document from the
// storage. Unreadable metadata files are skipped.
func (r *Repo) getAllBackupMetaFromStorage() ([]*BackupMeta, error) {
	backupFiles, err := r.stg.List("", defs.MetadataFileSuffix)
	if err != nil {
		return nil, errors.Wrap(err, "get a backups list from the storage")
	}

	backupMeta := make([]*BackupMeta, 0, len(backupFiles))
	for _, b := range backupFiles {
		meta, err := r.ReadMetadata(b.Name)
		if err != nil {
			fmt.Printf("read metadata of backup %s: %v\n", b.Name, err)
			continue
		}

		// todo: add file checks
		// err = backup.CheckBackupDataFiles(ctx, stg, meta)
		if err != nil {
			fmt.Printf("skip snapshot %s: %v\n", meta.Name, err)
			meta.Status = defs.StatusError
			meta.Err = err.Error()
		}

		backupMeta = append(backupMeta, meta)
	}

	return backupMeta, nil
}

// ReadMetadata reads and decodes a single backup metadata file from the storage.
func (r *Repo) ReadMetadata(fname string) (*BackupMeta, error) {
	rdr, err := r.stg.SourceReader(fname)
	if err != nil {
		return nil, errors.Wrap(err, "open")
	}
	defer rdr.Close()

	var meta BackupMeta
	err = json.NewDecoder(rdr).Decode(&meta)
	if err != nil {
		return nil, errors.Wrap(err, "decode")
	}

	return &meta, nil
}
