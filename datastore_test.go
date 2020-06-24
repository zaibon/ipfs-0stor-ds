package zstords

import (
	"fmt"
	"testing"

	ipfsds "github.com/ipfs/go-datastore"

	dsq "github.com/ipfs/go-datastore/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	datastor "github.com/threefoldtech/0-stor/client/datastor"
)

func TestWriteRead(t *testing.T) {
	opts := Options{}

	opts.JobCount = 1
	opts.Config.DataStor.Shards = []datastor.ShardConfig{
		{
			Address:   "localhost:9001",
			Namespace: "default",
			Password:  "",
		},
	}
	opts.Config.DataStor.Pipeline.BlockSize = 1024 * 1024 // 1MiB
	opts.MetaPath = "/home/zaibon/.ipfs-meta"

	ds, err := NewDatastore(&opts)
	require.NoError(t, err)
	defer ds.Close()

	key := ipfsds.NewKey("hello")
	val := []byte("world")
	err = ds.Put(key, val)
	require.NoError(t, err)

	recv, err := ds.Get(key)
	require.NoError(t, err)
	assert.Equal(t, val, recv)
}

func TestQuery(t *testing.T) {
	opts := Options{}

	opts.JobCount = 1
	opts.Config.DataStor.Shards = []datastor.ShardConfig{
		{
			Address:   "localhost:9001",
			Namespace: "default",
			Password:  "",
		},
	}
	opts.Config.DataStor.Pipeline.BlockSize = 1024 * 1024 // 1MiB
	opts.MetaPath = "/home/zaibon/.ipfs-meta"

	ds, err := NewDatastore(&opts)
	require.NoError(t, err)
	defer ds.Close()

	val := []byte("world")

	for i := 0; i < 500; i++ {
		key := ipfsds.NewKey(fmt.Sprintf("hello-%d", i))
		err = ds.Put(key, val)
		require.NoError(t, err)
	}

	res, err := ds.Query(dsq.Query{
		KeysOnly:     false,
		ReturnsSizes: true,
	})
	require.NoError(t, err)

	entries, err := res.Rest()
	require.NoError(t, err)
	for _, e := range entries {
		t.Logf("%+v\n", e)
	}
	assert.Equal(t, 500, len(entries))
}
