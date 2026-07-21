package config

import (
	"context"
	"encoding/json"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
)

const (
	DefaultConfigName = "main"
	keyPrefix         = "/pbm/config/"
)

var ErrNotFound = errors.New("config not found")

// Svc is the configuration service.
// It manages PBM's configuration documents persisted in etcd.
type Svc struct {
	ccDB *clientv3.Client
}

// New creates a config service instance.
func New(ccDB *clientv3.Client) *Svc {
	return &Svc{ccDB: ccDB}
}

// Get returns the config document under the given name, defaulting to
// 'main' when empty. It returns ErrNotFound if no such document
// exists.
func (s *Svc) Get(ctx context.Context, name string) (*Config, error) {
	resp, err := s.ccDB.Get(ctx, key(name))
	if err != nil {
		return nil, errors.Wrap(err, "get config")
	}
	if len(resp.Kvs) == 0 {
		return nil, ErrNotFound
	}

	cfg := &Config{}
	if err := json.Unmarshal(resp.Kvs[0].Value, cfg); err != nil {
		return nil, errors.Wrap(err, "unmarshal config")
	}

	return cfg, nil
}

// GetAll returns every stored config document, ordered by name ascending.
// It returns an empty slice when no config documents exist.
func (s *Svc) GetAll(ctx context.Context) ([]*Config, error) {
	resp, err := s.ccDB.Get(ctx, keyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Wrap(err, "get configs")
	}

	out := make([]*Config, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		cfg := &Config{}
		if err := json.Unmarshal(kv.Value, cfg); err != nil {
			return nil, errors.Wrapf(err, "unmarshal config %q", string(kv.Key))
		}
		out = append(out, cfg)
	}

	return out, nil
}

// Save stores a config document, creating it when absent and replacing it in
// full when present. The document is identified by cfg.Name, defaulting to
// DefaultConfigName when empty.
func (s *Svc) Save(ctx context.Context, cfg *Config) error {
	if cfg.Name == "" {
		cfg.Name = DefaultConfigName
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		return errors.Wrap(err, "marshal config")
	}

	if _, err := s.ccDB.Put(ctx, key(cfg.Name), string(data)); err != nil {
		return errors.Wrap(err, "put config")
	}

	return nil
}

// Delete removes the config document under the given name, defaulting to
// DefaultConfigName when empty.
func (s *Svc) Delete(ctx context.Context, name string) error {
	resp, err := s.ccDB.Delete(ctx, key(name))
	if err != nil {
		return errors.Wrap(err, "delete config")
	}
	if resp.Deleted == 0 {
		return ErrNotFound
	}

	return nil
}

// key resolves the config name to its etcd key.
func key(name string) string {
	if name == "" {
		name = DefaultConfigName
	}
	return keyPrefix + name
}
