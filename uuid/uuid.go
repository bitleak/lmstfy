package uuid

import (
	"encoding/binary"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/oklog/ulid"
)

const (
	delaySecondMask = uint32(0xFFFF)
	priorityMask = uint32(0xFF0000)
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

// Use the last four bytes of the 16-byte's ULID to store the delaySecond,
// and 1-byte to store the priority. ID format like below:
//
// <-           12bytes           -><- 1byte -><- 1byte -><-     2bytes    ->
// +--------------------------------+----------+----------+-----------------+
// |    random bytes                | reserved | priority | delay second    |
// +--------------------------------+----------+----------+-----------------+
//
// The last fours bytes was some random value in ULID, so changing that value won't
// affect anything except randomness.
func GenUniqueJobID(delaySecond uint32, priority uint8) string {
	entropy := entropyPool.Get().(*rand.Rand)
	defer entropyPool.Put(entropy)
	id := ulid.MustNew(ulid.Now(), entropy)
	// Encode the delay second and priority in littleEndian and store at the last four bytes
	customBits := (uint32(priority)<<16) | (delaySecond & delaySecondMask)
	binary.LittleEndian.PutUint32(id[len(id)-4:], customBits)
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
	return binary.LittleEndian.Uint32(id[len(id)-4:]) & delaySecondMask, nil
}

func ExtractPriorityFromUniqueID(s string) (uint8, error) {
	id, err := ulid.Parse(s)
	if err != nil {
		return 0, err
	}
	return uint8((binary.LittleEndian.Uint32(id[len(id)-4:]) & priorityMask)>>16), nil
}