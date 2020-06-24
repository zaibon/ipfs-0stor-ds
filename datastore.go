package zstords

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"strings"

	"github.com/ipfs/go-datastore"
	ds "github.com/ipfs/go-datastore"
	"github.com/jbenet/goprocess"

	dsq "github.com/ipfs/go-datastore/query"
	"github.com/threefoldtech/0-stor/client"
	datastor "github.com/threefoldtech/0-stor/client/datastor"
	"github.com/threefoldtech/0-stor/client/datastor/pipeline"
	"github.com/threefoldtech/0-stor/client/datastor/pipeline/storage"
	"github.com/threefoldtech/0-stor/client/datastor/zerodb"
	"github.com/threefoldtech/0-stor/client/metastor"
	"github.com/threefoldtech/0-stor/client/metastor/db/badger"
)

var ErrClosed = errors.New("datastore closed")

type Datastore struct {
	data *client.Client
	meta *metastor.Client
}

// Options are the 0-stor options, reexported here for convenience.
type Options struct {
	// directory where metadata is stored
	MetaPath string

	// JobCount controls the concurency of the datastor pipeline
	JobCount int

	client.Config
}

var _ ds.Datastore = (*Datastore)(nil)

// var _ ds.TxnDatastore = (*Datastore)(nil)
// var _ ds.TTLDatastore = (*Datastore)(nil)
// var _ ds.GCDatastore = (*Datastore)(nil)

// NewDatastore creates a new badger datastore.
//
// DO NOT set the Dir and/or ValuePath fields of opt, they will be set for you.
func NewDatastore(options *Options) (*Datastore, error) {
	if options == nil {
		return nil, fmt.Errorf("options cannot be nil")
	}

	if options.JobCount < 1 {
		options.JobCount = runtime.NumCPU()
	}

	// create our badger-backed metastor database
	metaDB, err := badger.New(options.MetaPath, options.MetaPath)
	if err != nil {
		return nil, err
	}

	// create the metastor client,
	// with encryption enabled and our created badger DB backend
	metaClient, err := metastor.NewClientFromConfig([]byte(options.Config.Namespace), metastor.Config{
		Database: metaDB,
	})

	// create a datastor cluster, using our predefined addresses and namespace,
	// which will be used to store the actual data
	datastorCluster, err := zerodb.NewCluster(options.DataStor.Shards, options.Password, options.Namespace, nil, datastor.SpreadingTypeRandom)
	if err != nil {
		return nil, err
	}

	// create our pipeline which will be used to process or data prior to storage,
	// and process it once again upon reading it back from storage
	pipeline, err := pipeline.NewPipeline(options.DataStor.Pipeline, datastorCluster, -1)
	if err != nil {
		return nil, err
	}

	// create our custom client
	ds := &Datastore{
		data: client.NewClient(metaClient, pipeline),
		meta: metaClient,
	}

	return ds, nil
}

func (d *Datastore) Put(key ds.Key, value []byte) error {

	meta, err := d.data.Write(key.Bytes(), bytes.NewReader(value))
	if err != nil {
		return err
	}

	return d.meta.SetMetadata(*meta)
}

func (d *Datastore) Sync(prefix ds.Key) error {
	// 0-stor is always sync
	return nil
}

func (d *Datastore) Get(key ds.Key) (value []byte, err error) {
	meta, err := d.meta.GetMetadata(key.Bytes())
	if err != nil {
		return nil, err
	}

	b := bytes.Buffer{}
	if err := d.data.Read(*meta, &b); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (d *Datastore) Has(key ds.Key) (bool, error) {
	_, err := d.meta.GetMetadata(key.Bytes())
	if err == nil {
		return true, nil
	}

	if errors.Is(err, metastor.ErrNotFound) {
		return false, nil
	}

	return false, err
}

func (d *Datastore) GetSize(key ds.Key) (size int, err error) {
	meta, err := d.meta.GetMetadata(key.Bytes())
	if err != nil {
		return 0, err
	}
	return int(meta.Size), nil
}

func (d *Datastore) Delete(key ds.Key) error {
	meta, err := d.meta.GetMetadata(key.Bytes())
	if err != nil {
		return err
	}

	return d.data.Delete(*meta)
}

func (d *Datastore) Query(q dsq.Query) (dsq.Results, error) {
	qrb := dsq.NewResultBuilder(q)
	qrb.Process.Go(func(worker goprocess.Process) {
		var prefix []byte
		if q.Prefix != "" {
			prefix = []byte(q.Prefix)
		}

		b := bytes.Buffer{}
		_ = d.meta.ListKeys(func(key []byte) error {

			if prefix != nil && bytes.HasPrefix(key, prefix) {
				return nil
			}

			e := dsq.Entry{Key: string(key)}
			var result dsq.Result

			meta, err := d.meta.GetMetadata(key)
			if err != nil {
				result = dsq.Result{Error: err}
			} else {
				if !q.KeysOnly {
					b.Reset()
					if err := d.data.Read(*meta, &b); err != nil {
						result = dsq.Result{Error: err}
					} else {
						e.Value = b.Bytes()
						e.Size = len(e.Value)
						result = dsq.Result{Entry: e}
					}
				} else {
					e.Size = int(meta.Size)
				}

				// Finally, filter it (unless we're dealing with an error).
				if result.Error == nil && filter(q.Filters, e) {
					return nil
				}
			}

			select {
			case qrb.Output <- result:
			case <-worker.Closing(): // client told us to close early
				return nil
			}

			return nil
		})
	})

	go qrb.Process.CloseAfterChildren() //nolint

	return qrb.Results(), nil
}

// DiskUsage implements the PersistentDatastore interface.
// It returns the sum of lsm and value log files sizes in bytes.
func (d *Datastore) DiskUsage() (uint64, error) {
	var total uint64 = 0

	err := d.meta.ListKeys(func(key []byte) error {
		meta, err := d.meta.GetMetadata(key)
		if err != nil {
			return err
		}
		total += uint64(meta.StorageSize)
		return nil
	})

	return total, err
}

func (d *Datastore) Check() error {
	errorKeys := []string{}

	err := d.meta.ListKeys(func(key []byte) error {
		meta, err := d.meta.GetMetadata(key)
		if err != nil {
			return err
		}
		status, err := d.data.Check(*meta, true)
		if err != nil {
			return err
		}
		if status == storage.CheckStatusInvalid {
			errorKeys = append(errorKeys, fmt.Sprintf("%x", key))
		}
		return nil
	})
	if err != nil {
		return err
	}

	if len(errorKeys) > 0 {
		return fmt.Errorf("error found for keys %s", strings.Join(errorKeys, ", "))
	}

	return nil
}

func (d *Datastore) Scrub() error {
	err := d.meta.ListKeys(func(key []byte) error {
		meta, err := d.meta.GetMetadata(key)
		if err != nil {
			return err
		}

		status, err := d.data.Check(*meta, true)
		if err != nil {
			return err
		}

		if status == storage.CheckStatusOptimal {
			return nil
		}

		newMeta, err := d.data.Repair(*meta)
		if err != nil {
			return err
		}

		return d.meta.SetMetadata(*newMeta)
	})
	return err
}

func (d *Datastore) Close() error {
	if err := d.meta.Close(); err != nil {
		return err
	}
	return d.data.Close()
}

type batch struct {
	puts    map[datastore.Key][]byte
	deletes map[datastore.Key]struct{}

	ds *Datastore
}

func (d *Datastore) Batch() (datastore.Batch, error) {
	return &batch{
		puts:    make(map[datastore.Key][]byte),
		deletes: make(map[datastore.Key]struct{}),
		ds:      d,
	}, nil
}

func (bt *batch) Put(key datastore.Key, val []byte) error {
	bt.puts[key] = val
	return nil
}

func (bt *batch) Delete(key datastore.Key) error {
	bt.deletes[key] = struct{}{}
	return nil
}

func (bt *batch) Commit() error {
	for k, v := range bt.puts {
		if err := bt.ds.Put(k, v); err != nil {
			return err
		}
	}

	for k := range bt.deletes {
		if err := bt.ds.Delete(k); err != nil {
			return err
		}
	}

	return nil
}

// filter returns _true_ if we should filter (skip) the entry
func filter(filters []dsq.Filter, entry dsq.Entry) bool {
	for _, f := range filters {
		if !f.Filter(entry) {
			return true
		}
	}
	return false
}
