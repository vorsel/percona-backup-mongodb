package gcs

import (
	"context"
	"io"
	"path"
	"strings"
	"time"

	storagegcs "cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

const (
	defaultChunkSize               = 10 * 1024 * 1024 // 10MiB
	defaultParallelUploadChunkSize = 16 * 1024 * 1024 // 16MiB, matches Google SDK PCU default
	defaultMaxObjSizeGB            = 5018             // 4.9 TB

	defaultMaxAttempts        = 5
	defaultBackoffInitial     = time.Second
	defaultBackoffMax         = 30 * time.Second
	defaultBackoffMultiplier  = 2
	defaultChunkRetryDeadline = 32 * time.Second
)

type ServiceAccountCredentials struct {
	Type                string `json:"type"`
	PrivateKey          string `json:"private_key"`
	ClientEmail         string `json:"client_email"`
	AuthURI             string `json:"auth_uri"`
	TokenURI            string `json:"token_uri"`
	UniverseDomain      string `json:"universe_domain"`
	AuthProviderCertURL string `json:"auth_provider_x509_cert_url"`
	ClientCertURL       string `json:"client_x509_cert_url"`
}

type GCS struct {
	cfg *Config
	log log.LogEvent

	client       *storagegcs.Client
	bucketHandle *storagegcs.BucketHandle
	d            *Download
}

func New(cfg *Config, node string, l log.LogEvent) (storage.Storage, error) {
	if err := cfg.Cast(); err != nil {
		return nil, errors.Wrap(err, "set defaults")
	}

	g := &GCS{
		cfg: cfg,
		log: l,
	}

	if err := g.initClient(); err != nil {
		return nil, errors.Wrap(err, "new google client")
	}

	g.d = &Download{
		arenas:   []*storage.Arena{storage.NewArena(storage.DownloadChuckSizeDefault, storage.DownloadChuckSizeDefault)},
		spanSize: storage.DownloadChuckSizeDefault,
		cc:       1,
	}

	return storage.NewSplitMergeMW(g, cfg.GetMaxObjSizeGB()), nil
}

func NewWithDownloader(
	opts *Config,
	node string,
	l log.LogEvent,
	cc, bufSizeMb, spanSizeMb int,
) (storage.Storage, error) {
	if err := opts.Cast(); err != nil {
		return nil, errors.Wrap(err, "set defaults")
	}

	if l == nil {
		l = log.DiscardEvent
	}

	g := &GCS{
		cfg: opts,
		log: l,
	}

	if err := g.initClient(); err != nil {
		return nil, errors.Wrap(err, "new google client")
	}

	arenaSize, spanSize, cc := storage.DownloadOpts(cc, bufSizeMb, spanSizeMb)
	g.log.Debug("download max buf %d (arena %d, span %d, concurrency %d)", arenaSize*cc, arenaSize, spanSize, cc)

	var arenas []*storage.Arena
	for i := 0; i < cc; i++ {
		arenas = append(arenas, storage.NewArena(arenaSize, spanSize))
	}

	g.d = &Download{
		arenas:   arenas,
		spanSize: spanSize,
		cc:       cc,
		stat:     storage.NewDownloadStat(cc, arenaSize, spanSize),
	}

	return storage.NewSplitMergeMW(g, opts.GetMaxObjSizeGB()), nil
}

func (*GCS) Type() storage.Type {
	return storage.GCS
}

func (g *GCS) Save(name string, data io.Reader, options ...storage.Option) error {
	opts := storage.GetDefaultOpts()
	for _, opt := range options {
		if err := opt(opts); err != nil {
			return errors.Wrap(err, "processing options for save")
		}
	}

	ctx := context.Background()
	w := g.bucketHandle.Object(path.Join(g.cfg.Prefix, name)).NewWriter(ctx)
	if g.cfg.parallelUploadEnabled() {
		if g.log != nil && opts.UseLogger {
			g.log.Debug(`uploading %q [size hint: %v (%v); parallel upload part size: %v (%v); concurrency: %d]`,
				name,
				opts.Size, storage.PrettySize(opts.Size),
				g.cfg.ChunkSize, storage.PrettySize(int64(g.cfg.ChunkSize)),
				g.cfg.ParallelUploadConcurrency)
		}

		w.EnableParallelUpload = true
		w.ParallelUploadConfig = storagegcs.ParallelUploadConfig{
			PartSize:       g.cfg.ChunkSize,
			MaxConcurrency: g.cfg.ParallelUploadConcurrency,
		}
	} else {
		const align int64 = 256 << 10 // 256 KiB (both min size and alignment)

		partSize := storage.ComputePartSize(
			opts.Size,
			defaultChunkSize,
			align,
			10_000,
			int64(g.cfg.ChunkSize),
		)

		if rem := partSize % align; rem != 0 {
			partSize += align - rem
		}

		if g.log != nil && opts.UseLogger {
			g.log.Debug(`uploading %q [size hint: %v (%v); part size: %v (%v)]`,
				name,
				opts.Size, storage.PrettySize(opts.Size),
				partSize, storage.PrettySize(partSize))
		}

		w.ChunkSize = int(partSize)
		w.ChunkRetryDeadline = g.cfg.Retryer.ChunkRetryDeadline
	}
	if g.log != nil && opts.UseLogger {
		w.ProgressFunc = func(written int64) {
			if opts.Size > 0 {
				g.log.Debug("uploaded %v / %v (%.1f%%)",
					written, opts.Size,
					float64(written)*100/float64(opts.Size))
			} else {
				g.log.Debug("uploaded %v (total unknown)", written)
			}
		}
	}

	if _, err := io.Copy(w, data); err != nil {
		return errors.Wrap(err, "save data")
	}

	if err := w.Close(); err != nil {
		return errors.Wrap(err, "writer close")
	}

	return nil
}

func (g *GCS) FileStat(name string) (storage.FileInfo, error) {
	ctx := context.Background()

	attrs, err := g.bucketHandle.Object(path.Join(g.cfg.Prefix, name)).Attrs(ctx)
	if err != nil {
		if errors.Is(err, storagegcs.ErrObjectNotExist) {
			return storage.FileInfo{}, storage.ErrNotExist
		}

		return storage.FileInfo{}, errors.Wrap(err, "get properties")
	}

	inf := storage.FileInfo{
		Name: attrs.Name,
		Size: attrs.Size,
	}

	if inf.Size == 0 {
		return inf, storage.ErrEmpty
	}

	return inf, nil
}

func (g *GCS) List(prefix, suffix string) ([]storage.FileInfo, error) {
	prefix = path.Join(g.cfg.Prefix, prefix)
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	ctx := context.Background()

	var files []storage.FileInfo
	it := g.bucketHandle.Objects(ctx, &storagegcs.Query{Prefix: prefix})

	for {
		attrs, err := it.Next()

		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return nil, errors.Wrap(err, "list objects")
		}

		name := attrs.Name
		name = strings.TrimPrefix(name, prefix)
		if len(name) == 0 {
			continue
		}
		if name[0] == '/' {
			name = name[1:]
		}

		if suffix != "" && !strings.HasSuffix(name, suffix) {
			continue
		}

		files = append(files, storage.FileInfo{
			Name: name,
			Size: attrs.Size,
		})
	}

	return files, nil
}

func (g *GCS) Delete(name string) error {
	ctx := context.Background()

	err := g.bucketHandle.Object(path.Join(g.cfg.Prefix, name)).Delete(ctx)
	if err != nil {
		if errors.Is(err, storagegcs.ErrObjectNotExist) {
			return storage.ErrNotExist
		}
		return errors.Wrap(err, "delete object")
	}

	return nil
}

func (g *GCS) Copy(src, dst string) error {
	ctx := context.Background()

	srcObj := g.bucketHandle.Object(path.Join(g.cfg.Prefix, src))
	dstObj := g.bucketHandle.Object(path.Join(g.cfg.Prefix, dst))

	_, err := g.FileStat(src)
	if err == storage.ErrNotExist {
		return err
	}

	_, err = dstObj.CopierFrom(srcObj).Run(ctx)
	return err
}

func (g *GCS) Close() error {
	if g == nil || g.client == nil {
		return nil
	}

	if g.cfg.parallelUploadEnabled() {
		// Google SDK parallel upload starts temporary object cleanup asynchronously
		// after Writer.Close returns. Give it a short window before closing the client.
		time.Sleep(2 * time.Second)
	}

	return g.client.Close()
}
