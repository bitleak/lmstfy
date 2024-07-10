package uuid

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/oklog/ulid"
)

const JobIDV1 = 1

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

// GenJobIDWithVersion generates a job ID with version prefix and delaySecond.
// For the legacy version 0 job ID, the version prefix is not included,
// we use the version prefix to distinguish different job payload format.
//
// Use the last four bytes of the 16-byte's ULID to store the delaySecond.
// The last fours bytes was some random value in ULID, so changing that value won't
// affect anything except randomness.
func GenJobIDWithVersion(version int, delaySecond uint32) string {
	entropy := entropyPool.Get().(*rand.Rand)
	defer entropyPool.Put(entropy)
	id := ulid.MustNew(ulid.Now(), entropy)
	// Encode the delayHour in littleEndian and store at the last four bytes
	binary.LittleEndian.PutUint32(id[len(id)-4:], delaySecond)
	// legacy version is 0, it doesn't include version prefix in the id
	if version == 0 {
		return id.String()
	}
	if version < 0 || version > 9 {
		version = JobIDV1
	}
	return fmt.Sprintf("%d%s", version, id.String())
}

func ElapsedMilliSecondFromUniqueID(s string) (int64, error) {
	s, _ = extractJobID(s)
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
	s, _ = extractJobID(s)
	id, err := ulid.Parse(s)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(id[len(id)-4:]), nil
}

func extractJobID(s string) (string, int) {
	if len(s) <= ulid.EncodedSize {
		return s, 0
	}
	return s[1:], int(s[0] - '0')
}

func ExtractJobIDVersion(s string) int {
	if len(s) == ulid.EncodedSize {
		return 0
	}
	return int(s[0] - '0')
}
