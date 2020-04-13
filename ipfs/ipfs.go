package ipfsblob

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	clusterapi "github.com/ipfs/ipfs-cluster/api"
	cluster "github.com/ipfs/ipfs-cluster/api/rest/client"
)

type Config struct {
	DefaultTimeout time.Duration
	TempDir        string

	// -1: all, 0: default, >0: that #
	ReplFactorMin int
	ReplFactorMax int
	Local         bool
}

func (cfg *Config) timeoutCtx() (context.Context, func()) {
	if cfg.DefaultTimeout == 0 {
		return context.Background(), func() {}
	}
	return context.WithTimeout(context.Background(), cfg.DefaultTimeout)
}

type ipfsBlob struct {
	cfg *Config
	c   cluster.Client
}

var _ Blob = (*ipfsBlob)(nil)

func new(ccfg *cluster.Config, cfg *Config) (*ipfsBlob, error) {
	c, err := cluster.NewDefaultClient(ccfg)
	if err != nil {
		return nil, err
	}

	// Ensure the cluster is accessible.
	ctx, cancel := cfg.timeoutCtx()
	defer cancel()
	_, err = c.ID(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to fetch cluster id: %v", err)
	}

	return &ipfsBlob{
		cfg: cfg,
		c:   c,
	}, nil
}

func (s *ipfsBlob) Put(data []byte) ([]byte, error) {
	// IPFS cluster doesn't support adding stuff not on a file. :(
	//
	// So, add it to a temp file and pass it along. Ideally the temp dir
	// should be a tmpfs so we don't actually hit the disk.
	//
	// TODO: decide when to remove this file.
	fname := filepath.Join(s.cfg.TempDir, uuid.New().String())
	err := ioutil.WriteFile(fname, data, 0644)
	if err != nil {
		return nil, fmt.Errorf("error writing temp file: %v", err)
	}
	fnames := []string{fname}

	addOpts := &clusterapi.AddParams{
		PinOptions: clusterapi.PinOptions{
			ReplicationFactorMin: s.cfg.ReplFactorMin,
			ReplicationFactorMax: s.cfg.ReplFactorMax,
			Name:                 fname,
		},
		Local: s.cfg.Local,
	}

	ctx, cancel := s.cfg.timeoutCtx()
	defer cancel()

	addedChan := make(chan *clusterapi.AddedOutput)
	gotCid := make(chan struct{})
	var id *cid.Cid
	go func() {
		// addedChan should only be sent to once but we recv in a loop
		// to prevent multiple sends from deadlocking us.
		for {
			select {
			case added := <-addedChan:
				if added == nil {
					continue
				}

				id = &added.Cid
				close(gotCid)
			case <-ctx.Done():
				break
			}
		}
	}()

	err = s.c.Add(ctx, fnames, addOpts, addedChan)
	if err != nil {
		return nil, err
	}
	select {
	case <-gotCid:
	case <-ctx.Done():
		return nil, fmt.Errorf("cid not returned before ctx timeout")
	}

	return []byte(id.String()), nil
}

func (s *ipfsBlob) Get(scid []byte) ([]byte, error) {
	ctx, cancel := s.cfg.timeoutCtx()
	defer cancel()

	key := string(scid)
	ipfss := s.c.IPFS(ctx)
	rc, err := ipfss.Cat(key)
	if err != nil {
		return nil, fmt.Errorf("unable to fetch cid %s: %v", key, err)
	}
	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("error reading cid data %s: %v", key, err)
	}
	rc.Close()

	return data, err
}

func (s *ipfsBlob) Del([]byte) error {
	return fmt.Errorf("not yet")
}
