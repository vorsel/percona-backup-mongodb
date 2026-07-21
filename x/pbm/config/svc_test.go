// PBM 2.x package
package config

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	tcetcd "github.com/testcontainers/testcontainers-go/modules/etcd"

	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
	"github.com/percona/percona-backup-mongodb/x/pbm/storage"
	"github.com/percona/percona-backup-mongodb/x/pbm/storage/fs"
)

const etcdImage = "gcr.io/etcd-development/etcd:v3.6.12"

var testEndpoints []string

func TestMain(m *testing.M) {
	ctx := context.Background()

	ctr, err := tcetcd.Run(ctx, etcdImage)
	if err != nil {
		log.Fatalf("start etcd container: %v", err)
	}

	testEndpoints, err = ctr.ClientEndpoints(ctx)
	if err != nil {
		_ = ctr.Terminate(ctx)
		log.Fatalf("get etcd client endpoints: %v", err)
	}

	code := m.Run()

	if err := ctr.Terminate(ctx); err != nil {
		log.Printf("terminate etcd container: %v", err)
	}

	os.Exit(code)
}

func newTestSvc(t *testing.T) *Svc {
	t.Helper()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   testEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("dial etcd client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	if _, err := cli.Delete(t.Context(), keyPrefix, clientv3.WithPrefix()); err != nil {
		t.Fatalf("reset config keys: %v", err)
	}

	return New(cli)
}

func testConfig(name, path string) *Config {
	return &Config{
		Name: name,
		Storage: StorageConf{
			Type:       storage.Filesystem,
			Filesystem: &fs.Config{Path: path},
		},
	}
}

func TestSave(t *testing.T) {
	ctx := context.Background()

	t.Run("creates new document", func(t *testing.T) {
		svc := newTestSvc(t)

		if err := svc.Save(ctx, testConfig("primary", "/backups/primary")); err != nil {
			t.Fatalf("Save: %v", err)
		}

		got, err := svc.Get(ctx, "primary")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Name != "primary" {
			t.Errorf("Name = %q, want %q", got.Name, "primary")
		}
		if got.Storage.Type != storage.Filesystem {
			t.Errorf("Storage.Type = %q, want %q", got.Storage.Type, storage.Filesystem)
		}
		if got.Storage.Filesystem == nil || got.Storage.Filesystem.Path != "/backups/primary" {
			t.Errorf("Storage.Filesystem = %+v, want Path %q", got.Storage.Filesystem, "/backups/primary")
		}
	})

	t.Run("replaces existing document", func(t *testing.T) {
		svc := newTestSvc(t)

		if err := svc.Save(ctx, testConfig("dup", "/a")); err != nil {
			t.Fatalf("first Save: %v", err)
		}

		// A second Save replaces the whole document (create-or-replace).
		if err := svc.Save(ctx, testConfig("dup", "/b")); err != nil {
			t.Fatalf("second Save: %v", err)
		}

		got, err := svc.Get(ctx, "dup")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Storage.Filesystem == nil || got.Storage.Filesystem.Path != "/b" {
			t.Errorf("Storage.Filesystem = %+v, want Path %q", got.Storage.Filesystem, "/b")
		}
	})

	t.Run("defaults empty name to main", func(t *testing.T) {
		svc := newTestSvc(t)

		if err := svc.Save(ctx, testConfig("", "/default")); err != nil {
			t.Fatalf("Save: %v", err)
		}

		got, err := svc.Get(ctx, "main")
		if err != nil {
			t.Fatalf("Get main: %v", err)
		}
		if got.Name != DefaultConfigName {
			t.Errorf("Name = %q, want %q", got.Name, DefaultConfigName)
		}
	})
}

func TestGet(t *testing.T) {
	ctx := context.Background()

	t.Run("missing returns ErrNotFound", func(t *testing.T) {
		svc := newTestSvc(t)

		_, err := svc.Get(ctx, "ghost")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get missing: got %v, want ErrNotFound", err)
		}
	})

	t.Run("empty name resolves to main", func(t *testing.T) {
		svc := newTestSvc(t)

		if err := svc.Save(ctx, testConfig("main", "/default")); err != nil {
			t.Fatalf("Save: %v", err)
		}

		got, err := svc.Get(ctx, "")
		if err != nil {
			t.Fatalf("Get empty name: %v", err)
		}
		if got.Name != DefaultConfigName {
			t.Errorf("Name = %q, want %q", got.Name, DefaultConfigName)
		}
	})
}

func TestGetAll(t *testing.T) {
	ctx := context.Background()

	t.Run("empty store returns empty slice", func(t *testing.T) {
		svc := newTestSvc(t)

		all, err := svc.GetAll(ctx)
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		if len(all) != 0 {
			t.Fatalf("GetAll: got %d configs, want 0", len(all))
		}
	})

	t.Run("returns all ordered by name", func(t *testing.T) {
		svc := newTestSvc(t)

		for _, name := range []string{"c", "a", "b"} {
			if err := svc.Save(ctx, testConfig(name, "/"+name)); err != nil {
				t.Fatalf("Save %q: %v", name, err)
			}
		}

		all, err := svc.GetAll(ctx)
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}

		got := make([]string, len(all))
		for i, cfg := range all {
			got[i] = cfg.Name
		}
		want := []string{"a", "b", "c"}
		if len(got) != len(want) {
			t.Fatalf("GetAll: got %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("GetAll order: got %v, want %v", got, want)
			}
		}
	})
}

func TestDelete(t *testing.T) {
	ctx := context.Background()

	t.Run("removes existing document", func(t *testing.T) {
		svc := newTestSvc(t)

		if err := svc.Save(ctx, testConfig("tmp", "/a")); err != nil {
			t.Fatalf("Save: %v", err)
		}

		if err := svc.Delete(ctx, "tmp"); err != nil {
			t.Fatalf("Delete: %v", err)
		}

		if _, err := svc.Get(ctx, "tmp"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get after Delete: got %v, want ErrNotFound", err)
		}
	})

	t.Run("missing returns ErrNotFound", func(t *testing.T) {
		svc := newTestSvc(t)

		err := svc.Delete(ctx, "ghost")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("Delete missing: got %v, want ErrNotFound", err)
		}
	})
}
