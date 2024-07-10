package uuid

import (
	"encoding/binary"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/oklog/ulid"
)

const JobIDVersion = "1"

// Use pool to avoid concurrent access for rand.Source
var entropyPool = sync.Pool{
	New: func() interface{} {
		return rand.New(rand.NewSource(time.Now().UnixNano()))
	},
}

// Generate Unique ID
// Currently using ULID, this maybe conflict with other process with very low possibility
func GenUniqueID() string {
	entropy := entropyPool.Get().(*rand.Rand)
	defer entropyPool.Put(entropy)
	id := ulid.MustNew(ulid.Now(), entropy)
	return id.String()
}

// Use the last four bytes of the 16-byte's ULID to store the delaySecond.
// The last fours bytes was some random value in ULID, so changing that value won't
// affect anything except randomness.
func GenUniqueJobIDWithDelay(delaySecond uint32) string {
	entropy := entropyPool.Get().(*rand.Rand)
	defer entropyPool.Put(entropy)
	id := ulid.MustNew(ulid.Now(), entropy)
	// Encode the delayHour in littleEndian and store at the last four bytes
	binary.LittleEndian.PutUint32(id[len(id)-4:], delaySecond)
	// Add a version prefix to identify the jobID format
	return JobIDVersion + id.String()
}

func ElapsedMilliSecondFromUniqueID(s string) (int64, error) {
	s, _ = ExtractJobID(s)
	id, err := ulid.Parse(s)
	if err != nil {
		return 0, err
	}
	t := id.Time()
	now := ulid.Now()
	if t <= now {
		return int64(now - t), nil
	} else {
		return 0, errors.New("id has a future timestamp")
	}
}

func ExtractDelaySecondFromUniqueID(s string) (uint32, error) {
	s, _ = ExtractJobID(s)
	id, err := ulid.Parse(s)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(id[len(id)-4:]), nil
}

func ExtractJobID(s string) (string, int) {
	if len(s) == ulid.EncodedSize+1 {
		return s[1:], int(s[0] - '0')
	}
	return s, 0
}
