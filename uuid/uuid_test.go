package uuid

import (
	"testing"
	"time"

	"github.com/oklog/ulid"
	"github.com/stretchr/testify/require"
)

func TestJobID(t *testing.T) {
	jobID := GenJobIDWithVersion(JobIDV1, 10)

	id, version := extractJobID(jobID)
	require.Equal(t, 1, version)
	require.Equal(t, ulid.EncodedSize, len(id))

	delay, err := ExtractDelaySecondFromUniqueID(jobID)
	require.NoError(t, err)
	require.Equal(t, uint32(10), delay)

	// Test elapsed time
	time.Sleep(10 * time.Millisecond)
	delayMilliseconds, err := ElapsedMilliSecondFromUniqueID(jobID)
	require.NoError(t, err)
	require.InDelta(t, 10, delayMilliseconds, 2)
}
