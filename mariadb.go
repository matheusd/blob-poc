package ipfsblob

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"
)

const sqlTableKV = `
create table kv (
	k BIGINT not null primary key,
	v LONGBLOB 
)
`

type Config struct {
	DefaultTimeout time.Duration
	DB             *sql.DB
	ServerExecID   uint32 // server ID + start ts
}

func (cfg *Config) timeoutCtx() (context.Context, func()) {
	if cfg.DefaultTimeout == 0 {
		return context.Background(), func() {}
	}
	return context.WithTimeout(context.Background(), cfg.DefaultTimeout)
}

type sqlBlob struct {
	cfg    *Config
	lastID uint32 // atomic
}

var _ Blob = (*sqlBlob)(nil)

func new(cfg *Config) (*sqlBlob, error) {
	return &sqlBlob{
		cfg: cfg,
	}, nil
}

func (s *sqlBlob) Put(data []byte) ([]byte, error) {
	sql := "insert into kv (k, v) values (?, ?);"

	// TODO: improve dis :P
	id := atomic.AddUint32(&s.lastID, 1)
	keyBytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(keyBytes, s.cfg.ServerExecID)
	binary.LittleEndian.PutUint32(keyBytes[4:], id)
	key := binary.LittleEndian.Uint64(keyBytes)

	ctx, cancel := s.cfg.timeoutCtx()
	defer cancel()
	_, err := s.cfg.DB.ExecContext(ctx, sql, key, data)
	if err != nil {
		return nil, err
	}

	return keyBytes, nil
}

func (s *sqlBlob) Get(scid []byte) ([]byte, error) {
	sql := "select v from kv where k = ?"

	key := binary.LittleEndian.Uint64(scid)

	ctx, cancel := s.cfg.timeoutCtx()
	defer cancel()
	res, err := s.cfg.DB.QueryContext(ctx, sql, key)
	if err != nil {
		return nil, err
	}

	if !res.Next() {
		err := res.Err()
		switch {
		case err != nil:
			return nil, err
		default:
			return nil, fmt.Errorf("no results returned")
		}
	}

	var data []byte
	err = res.Scan(&data)
	if err != nil {
		return nil, err
	}
	err = res.Close()
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *sqlBlob) Del([]byte) error {
	return fmt.Errorf("not yet")
}
