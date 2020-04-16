package ipfsblob

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"strings"
	"time"
)

const sqlTableKV = `
create table kv (
	k BIGINT auto_increment not null primary key,
	v LONGBLOB 
)
`

// ErrNotFound is returned when a Get() operation fails due to the key not
// existing.
type ErrNotFound struct {
	K []byte
}

// Error is part of the error interface.
func (e ErrNotFound) Error() string {
	return fmt.Sprintf("key '%x' not found", e.K)
}

// Is returns trus if the provided target is an ErrNotFound (not necessarily
// with the same key).
func (e ErrNotFound) Is(target error) bool {
	_, ok := target.(ErrNotFound)
	return ok
}

// Config specifies the configuration needed when initializing the blob
// backend.
type Config struct {
	// DefaultTimeout is the maximum time allowed for execution of Put, Get
	// and Del operations after which it fails. If zero it means the
	// operations never timeout.
	DefaultTimeout time.Duration

	// DB is the connection to the database. It should be open.
	DB *sql.DB
}

func nop() {}

func (cfg *Config) timeoutCtx() (context.Context, func()) {
	if cfg.DefaultTimeout == 0 {
		return context.Background(), nop
	}
	return context.WithTimeout(context.Background(), cfg.DefaultTimeout)
}

// SetupDB sets up the DB in the given configuration for the blob backend. It
// creates the necessary tables for operation of the kv store.
//
// It errors if the db is already setup.
func SetupDB(cfg *Config) error {
	ctx, cancel := cfg.timeoutCtx()
	defer cancel()

	_, err := cfg.DB.QueryContext(ctx, "select k from kv limit 1")
	if err == nil {
		return fmt.Errorf("table kv already exists (db is setup)")
	}

	if strings.Index(err.Error(), "Error 1146") == -1 {
		return fmt.Errorf("unexpected error querying kv table: %v", err)
	}

	_, err = cfg.DB.ExecContext(ctx, sqlTableKV)
	if err != nil {
		return fmt.Errorf("unable to create kv table: %v\n", err)
	}

	return nil
}

type sqlBlob struct {
	cfg *Config

	stInsert *sql.Stmt
	stGet    *sql.Stmt
	stDel    *sql.Stmt
}

var _ Blob = (*sqlBlob)(nil)

func new(cfg *Config) (*sqlBlob, error) {
	ctx, cancel := cfg.timeoutCtx()
	defer cancel()
	stInsert, err := cfg.DB.PrepareContext(ctx, "insert into kv (v) values (?);")
	if err != nil {
		return nil, fmt.Errorf("unable to prepare put() statement: %v", err)
	}

	stGet, err := cfg.DB.PrepareContext(ctx, "select v from kv where k = ?")
	if err != nil {
		return nil, fmt.Errorf("unable to prepare get() statement: %v", err)
	}

	stDel, err := cfg.DB.PrepareContext(ctx, "delete from kv where k = ?")
	if err != nil {
		return nil, fmt.Errorf("unable to prepare del() statement: %v", err)
	}

	return &sqlBlob{
		cfg:      cfg,
		stInsert: stInsert,
		stGet:    stGet,
		stDel:    stDel,
	}, nil
}

// Put the given data into the kv store. It returns the key with which to query
// the data in the future.
//
// This is part of the Blob interface.
func (s *sqlBlob) Put(data []byte) ([]byte, error) {
	ctx, cancel := s.cfg.timeoutCtx()
	defer cancel()
	res, err := s.stInsert.ExecContext(ctx, data)
	if err != nil {
		return nil, err
	}

	lastId, err := res.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("unable to fetch lastInsertId: %v", err)
	}

	keyBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(keyBytes, uint64(lastId))

	return keyBytes, nil
}

// Get returns the data for the given key. If the key does not exist in the
// store, an ErrNotFound is returned.
//
// This is part of the Blob interface.
func (s *sqlBlob) Get(k []byte) ([]byte, error) {
	key := int64(binary.LittleEndian.Uint64(k))

	ctx, cancel := s.cfg.timeoutCtx()
	defer cancel()
	res, err := s.stGet.QueryContext(ctx, key)
	if err != nil {
		return nil, err
	}

	if !res.Next() {
		err := res.Err()
		switch {
		case err != nil:
			return nil, err
		default:
			return nil, ErrNotFound{K: k}
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

// Del removes the given key from the store.
//
// This is part of the Blob interface.
func (s *sqlBlob) Del(k []byte) error {
	key := int64(binary.LittleEndian.Uint64(k))

	ctx, cancel := s.cfg.timeoutCtx()
	defer cancel()

	_, err := s.stDel.ExecContext(ctx, key)
	if err != nil {
		return err
	}

	return nil
}
