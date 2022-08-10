package redis

import (
	"encoding/binary"
	"errors"
	"strings"
)

func join(args ...string) string {
	return strings.Join(args, "/")
}

func splits(n int, s string) []string {
	return strings.SplitN(s, "/", n)
}

func isLuaScriptGone(err error) bool {
	return strings.HasPrefix(err.Error(), "NOSCRIPT")
}

// StructPack packs (tries, jobID) into lua struct pack of format "HHHc0", in lua this can be done:
//   ```local data = struct.pack("HHc0", tries, #job_id, job_id)```
func structPack(tries uint16, jobID string) (data string) {
	buf := make([]byte, 2+2+len(jobID))
	binary.LittleEndian.PutUint16(buf[0:], tries)
	binary.LittleEndian.PutUint16(buf[2:], uint16(len(jobID)))
	copy(buf[4:], jobID)
	return string(buf)
}

// StructUnpack unpacks the "HHc0" lua struct format, in lua this can be done:
//   ```local tries, job_id = struct.unpack("HHc0", data)```
func structUnpack(data string) (tries uint16, jobID string, err error) {
	buf := []byte(data)
	h1 := binary.LittleEndian.Uint16(buf[0:])
	h2 := binary.LittleEndian.Uint16(buf[2:])
	jobID = string(buf[4:])
	tries = h1
	if len(jobID) != int(h2) {
		err = errors.New("corrupted data")
	}
	return
}

// ConstructDelayedJobContent struct-pack the data in the format `Hc0Hc0HHc0`:
// {namespace len}{namespace}{queue len}{queue}{tries}{jobID len}{jobID}
// length are 2-byte uint16 in little-endian
func constructDelayedJobContent(namespace, queue, jobID string, tries uint16) []byte {
	namespaceLen := len(namespace)
	queueLen := len(queue)
	jobIDLen := len(jobID)
	buf := make([]byte, 2+namespaceLen+2+queueLen+2+2+jobIDLen)
	binary.LittleEndian.PutUint16(buf[0:], uint16(namespaceLen))
	copy(buf[2:], namespace)
	binary.LittleEndian.PutUint16(buf[2+namespaceLen:], uint16(queueLen))
	copy(buf[2+namespaceLen+2:], queue)
	binary.LittleEndian.PutUint16(buf[2+namespaceLen+2+queueLen:], tries)
	binary.LittleEndian.PutUint16(buf[2+namespaceLen+2+queueLen+2:], uint16(jobIDLen))
	copy(buf[2+namespaceLen+2+queueLen+2+2:], jobID)
	return buf
}

func getJobStreamIDKey(jobID string) string {
	return join(StreamIDPrefix, jobID)
}

func getQueueStreamName(namespace, queue string) string {
	return join(QueuePrefix, namespace, queue)
}

func getTimersetKey(namespace, queue string) string {
	return join(TimerSetPrefix, namespace, queue)
}
