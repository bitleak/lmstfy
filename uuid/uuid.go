package uuid

import (
	"encoding/binary"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/oklog/ulid"
)

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
	return id.String()
}

func UniqueIDToBinary(id string) [16]byte {
	return ulid.MustParse(id)
}

func BinaryToUniqueID(bin [16]byte) string {
	return ulid.ULID(bin).String()
}

func ElapsedMilliSecondFromUniqueID(s string) (int64, error) {
	id, err := ulid.Parse(s)
	if err != nil {
		return 0, err
	}
	t := id.Time()
	now := ulid.Now()
	if t < now {
		return int64(now - t), nil
	} else {
		return 0, errors.New("id has a future timestamp")
	}
}

func ExtractDelaySecondFromUniqueID(s string) (uint32, error) {
	id, err := ulid.Parse(s)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(id[len(id)-4:]), nil
}
