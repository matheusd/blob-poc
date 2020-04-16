package ipfsblob

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/loov/hrtime"
)

func crandInt64() int64 {
	var b [8]byte
	_, err := crand.Read(b[:])
	if err != nil {
		panic(err)
	}
	return int64(binary.LittleEndian.Uint64(b[:]))
}

const (
	defaultTimeout = time.Second * 30
)

var (
	testDB           *sql.DB
	lastServerExecId uint32
)

type testCtx struct {
	s       *sqlBlob
	cancels []func()
	testing.TB
}

func newTestCtx(t testing.TB) (*testCtx, func()) {
	t.Helper()

	testDir, err := ioutil.TempDir("", "ipfsblob-test")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		DefaultTimeout: 30 * time.Second,
		DB:             testDB,
	}
	s, err := new(cfg)
	if err != nil {
		t.Fatalf("unable to create ipfsBlob: %v", err)
	}

	tc := &testCtx{
		TB: t,
		s:  s,
	}
	tearDown := func() {
		for _, f := range tc.cancels {
			f()
		}
		switch t.Failed() {
		case true:
			t.Logf("Test dir: %s", testDir)
		default:
			os.RemoveAll(testDir)
		}
	}
	return tc, tearDown
}

// TestSetupDB tests that the datbase can be setup.
func TestSetupDB(t *testing.T) {
	// Manually drop the existing table.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	testDB.ExecContext(ctx, "drop table kv")

	cfg := &Config{
		DB:             testDB,
		DefaultTimeout: 10 * time.Second,
	}
	err := SetupDB(cfg)
	if err != nil {
		t.Fatalf("unexpected error setting up db: %v", err)
	}

	// Trying to set it up a second time should fail.
	err = SetupDB(cfg)
	if err == nil {
		t.Fatalf("unexpected success when setting up db for the second time")
	}
}

// TestPutGet tests and times a simple put/get roundtrip for a random 32 byte
// string.
func TestPutGet(t *testing.T) {
	tc, doneTc := newTestCtx(t)
	defer doneTc()

	data := make([]byte, 32)
	_, err := crand.Read(data)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	name, err := tc.s.Put(data)
	if err != nil {
		t.Fatalf("unable to Put: %v", err)
	}
	putTime := time.Now()

	newData, err := tc.s.Get(name)
	if err != nil {
		t.Fatalf("unable to Get: %v", err)
	}
	getTime := time.Now()

	us := time.Microsecond
	putDelta := putTime.Sub(start).Truncate(us)
	getDelta := getTime.Sub(putTime).Truncate(us)
	totDelta := getTime.Sub(start).Truncate(us)
	t.Logf("Timings for put: %s, get: %s, total: %s", putDelta, getDelta, totDelta)

	if !bytes.Equal(newData, data) {
		t.Fatalf("data doesn't match (name=%x). want=%x got=%x", name, data, newData)
	}
}

// TestBenchPut benchmarks several different put scenarios.
func TestBenchPut(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Put bench")
	}
	var rnd *rand.Rand
	readRnd := func(sz int) []byte {
		b := make([]byte, sz)
		_, err := rnd.Read(b)
		if err != nil {
			panic(err)
		}
		return b
	}

	type testCase struct {
		name string
		data func(int) []byte
		N    int
	}

	testCases := []testCase{
		{
			name: "small data",
			data: func(i int) []byte { return readRnd(32) },
			N:    100,
		},
		{
			name: "large data",
			data: func(i int) []byte { return readRnd(1000 * 1000) },
			N:    50,
		},
	}

	// This is the actual code for the benchmark.
	benchCase := func(t *testing.T, c *testCase) {
		seed := crandInt64()
		defer func() {
			if t.Failed() {
				t.Logf("Seed: %d", seed)
			}
		}()
		rnd = rand.New(rand.NewSource(seed))

		tc, doneTc := newTestCtx(t)
		defer doneTc()

		// Modify the running config of the blob.

		bench := hrtime.NewBenchmark(c.N)
		var i int
		for bench.Next() {
			_, err := tc.s.Put(c.data(i))
			if err != nil {
				t.Fatalf("failed to put at %d: %v", i, err)
			}
			i++
		}
		t.Log(bench.Histogram(10))
	}

	for _, c := range testCases {
		c := c
		ok := t.Run(c.name, func(t *testing.T) {
			benchCase(t, &c)
		})
		if !ok {
			break
		}
	}
}

// TestDel tests that the del() operation works.
func TestDel(t *testing.T) {
	tc, doneTc := newTestCtx(t)
	defer doneTc()

	data := make([]byte, 32)
	_, err := crand.Read(data)
	if err != nil {
		t.Fatal(err)
	}

	// Put a random element
	name, err := tc.s.Put(data)
	if err != nil {
		t.Fatalf("unable to Put: %v", err)
	}

	// Get should return it.
	newData, err := tc.s.Get(name)
	if err != nil {
		t.Fatalf("unable to Get: %v", err)
	}
	if !bytes.Equal(newData, data) {
		t.Fatalf("data doesn't match (name=%x). want=%x got=%x", name, data, newData)
	}

	// Delete it.
	err = tc.s.Del(name)
	if err != nil {
		t.Fatalf("unable to Del: %v", err)
	}

	// Get should return an error. The error should fulfill
	// errors.Is/errors.As.
	_, err = tc.s.Get(name)
	if !errors.Is(err, ErrNotFound{}) {
		t.Fatalf("unexpected error after del. want=%v got=%v", ErrNotFound{}, err)
	}

	var notFoundErr ErrNotFound
	if !errors.As(err, &notFoundErr) {
		t.Fatalf("returned error does not fulfill As for ErrNotFound")
	}

	if !bytes.Equal(notFoundErr.K, name) {
		t.Fatalf("returned error data not the same as the queried one")
	}
}

// TestMain starts up the tests. To run the tests you need a database named
// 'test' already setup, accessible by the current user without password and
// with full permissions on the test database.
func TestMain(m *testing.M) {
	var err error
	testDB, err = sql.Open("mysql", "/test")
	if err != nil {
		fmt.Printf("unable to connect to DB: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err = testDB.QueryContext(ctx, "select * from kv limit 1")
	if err != nil {
		if strings.Index(err.Error(), "Error 1146") > -1 {
			_, err := testDB.ExecContext(ctx, sqlTableKV)
			if err != nil {
				fmt.Printf("unable to create kv table: %v\n", err)
				os.Exit(1)
			}
		} else {
			fmt.Printf("unable to query kv table: %v\n", err)
			os.Exit(1)
		}
	}

	_, err = testDB.QueryContext(ctx, "delete from kv")
	if err != nil {
		fmt.Printf("unable to clean up kv table: %v\n", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}
