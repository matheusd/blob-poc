package ipfsblob

import (
	"bytes"
	crand "crypto/rand"
	"encoding/binary"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	cluster "github.com/ipfs/ipfs-cluster/api/rest/client"
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

type testCtx struct {
	ipfs    *ipfsBlob
	cancels []func()
	testing.TB
}

func newTestCtx(t testing.TB) (*testCtx, func()) {
	t.Helper()

	testDir := path.Join(os.TempDir(), fmt.Sprintf("%d", crandInt64()&!(1<<63)))
	err := os.Mkdir(testDir)
	if err != nil {
		t.Fatal(err)
	}
	ccfg := &cluster.Config{}
	cfg := &Config{
		DefaultTimeout: 30 * time.Second,
		TempDir:        testDir,
	}
	ipfs, err := new(ccfg, cfg)
	if err != nil {
		t.Fatalf("unable to create ipfsBlob: %v", err)
	}

	tc := &testCtx{
		TB:   t,
		ipfs: ipfs,
	}
	tearDown := func() {
		for _, f := range tc.cancels {
			f()
		}
		os.RemoveAll(testDir)
	}
	return tc, tearDown
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
	name, err := tc.ipfs.Put(data)
	if err != nil {
		t.Fatalf("unable to Put: %v", err)
	}
	putTime := time.Now()

	newData, err := tc.ipfs.Get(name)
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
		name          string
		data          func(int) []byte
		replFactorMin int
		replFactorMax int
		local         bool
		N             int
	}

	testCases := []testCase{
		{
			name:          "full replication small data",
			data:          func(i int) []byte { return readRnd(32) },
			replFactorMin: -1,
			replFactorMax: -1,
			N:             30,
		},
		{
			name:          "default replication small data",
			data:          func(i int) []byte { return readRnd(32) },
			replFactorMin: 0,
			replFactorMax: 0,
			N:             30,
		},
		{
			name:          "default replication small data local",
			data:          func(i int) []byte { return readRnd(32) },
			replFactorMin: 0,
			replFactorMax: 0,
			local:         true,
			N:             30,
		},
		{
			name:          "single peer small data",
			data:          func(i int) []byte { return readRnd(32) },
			replFactorMin: 1,
			replFactorMax: 1,
			N:             30,
		},
		{
			name:          "single peer small data local",
			data:          func(i int) []byte { return readRnd(32) },
			replFactorMin: 1,
			replFactorMax: 1,
			local:         true,
			N:             30,
		},
		{
			name:          "full replication large data",
			data:          func(i int) []byte { return readRnd(1000 * 1000) },
			replFactorMin: -1,
			replFactorMax: -1,
			N:             10,
		},
		{
			name:          "default replication large data",
			data:          func(i int) []byte { return readRnd(1000 * 1000) },
			replFactorMin: 0,
			replFactorMax: 0,
			N:             10,
		},
		{
			name:          "single peer large data",
			data:          func(i int) []byte { return readRnd(1000 * 1000) },
			replFactorMin: 1,
			replFactorMax: 1,
			N:             10,
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
		tc.ipfs.cfg.ReplFactorMin = c.replFactorMin
		tc.ipfs.cfg.ReplFactorMax = c.replFactorMax
		tc.ipfs.cfg.Local = c.local

		start := time.Now()
		for i := 0; i < c.N; i++ {
			_, err := tc.ipfs.Put(c.data(i))
			if err != nil {
				t.Fatalf("failed to put at %d: %v", i, err)
			}
		}
		delta := time.Now().Sub(start)
		avg := delta / time.Duration(c.N)
		t.Logf("Average Put() time: %s/op", avg.Truncate(time.Millisecond))
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
