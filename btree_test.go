package btree_test

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/alexanderbez/btree"
	"github.com/stretchr/testify/require"
)

var (
	rng *rand.Rand

	seed = flag.Int64("seed", -1, "Override source for random byte generation")
)

func init() {
	s := *seed
	if s < 0 {
		s = time.Now().UnixNano()
	}

	rng = rand.New(rand.NewSource(s))
}

var _ btree.Entry = (*testEntry)(nil)

type testEntry struct {
	key   uint64
	value uint64
}

func (te testEntry) Compare(other btree.Entry) int {
	teOther := other.(testEntry)

	switch {
	case te.key < teOther.key:
		return -1

	case te.key > teOther.key:
		return 1

	default:
		return 0
	}
}

func TestBTree(t *testing.T) {
	for _, minDegree := range []int{2, 4, 11, 17, 24, 48, 67, 99, 500} {
		t.Run(fmt.Sprintf("minimum degree %d", minDegree), func(t *testing.T) {
			bt, err := btree.New(minDegree)
			require.NoError(t, err)
			require.NotNil(t, bt)

			for i := 0; i < 500000; i++ {
				k := make([]byte, 32)
				rng.Read(k)

				v := make([]byte, 32)
				rng.Read(v)

				e := testEntry{binary.BigEndian.Uint64(k), binary.BigEndian.Uint64(v)}
				bt.Insert(e)
				require.Equal(t, i+1, bt.Size())
				require.Equal(t, e, bt.Search(e), i)
			}
		})
	}
}

func benchmarkInsert(b *testing.B, minDegree int) {
	bt, err := btree.New(minDegree)
	require.NoError(b, err)
	require.NotNil(b, bt)

	b.ResetTimer()

	b.Run(fmt.Sprintf("minimum degree %d", minDegree), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()

			k := make([]byte, 32)
			rng.Read(k)

			v := make([]byte, 32)
			rng.Read(v)

			b.StartTimer()
			bt.Insert(testEntry{binary.BigEndian.Uint64(k), binary.BigEndian.Uint64(v)})
		}
	})
}

func BenchmarkInsert17(b *testing.B) {
	benchmarkInsert(b, 17)
	benchmarkInsert(b, 24)
	benchmarkInsert(b, 48)
}
