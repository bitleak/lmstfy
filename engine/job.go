package engine

import (
	"encoding"
	"encoding/binary"
	"errors"

	"github.com/meitu/lmstfy/uuid"
)

type Job interface {
	Namespace() string
	Queue() string
	ID() string
	Body() []byte
	TTL() uint32
	Delay() uint32
	Tries() uint16
	ElapsedMS() int64
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

type jobImpl struct {
	namespace string
	queue     string
	id        string
	body      []byte
	ttl       uint32
	delay     uint32
	tries     uint16

	_elapsedMS int64
}

// NOTE: there is a trick in this factory, the delay is embedded in the jobID.
// By doing this we can delete the job that's located in hourly AOF, by placing
// a tombstone record in that AOF.
func NewJob(namespace, queue string, body []byte, ttl, delay uint32, tries uint16) Job {
	id := uuid.GenUniqueJobIDWithDelay(delay)
	return &jobImpl{
		namespace: namespace,
		queue:     queue,
		id:        id,
		body:      body,
		ttl:       ttl,
		delay:     delay,
		tries:     tries,
	}
}

func NewJobWithID(namespace, queue string, body []byte, ttl uint32, tries uint16, jobID string) Job {
	delay, _ := uuid.ExtractDelaySecondFromUniqueID(jobID)
	return &jobImpl{
		namespace: namespace,
		queue:     queue,
		id:        jobID,
		body:      body,
		ttl:       ttl,
		delay:     delay,
		tries:     tries,
	}
}

func (j *jobImpl) Namespace() string {
	return j.namespace
}

func (j *jobImpl) Queue() string {
	return j.queue
}

func (j *jobImpl) ID() string {
	return j.id
}

func (j *jobImpl) Body() []byte {
	return j.body
}

func (j *jobImpl) TTL() uint32 {
	return j.ttl
}

func (j *jobImpl) Delay() uint32 {
	return j.delay
}

func (j *jobImpl) Tries() uint16 {
	return j.tries
}

func (j *jobImpl) ElapsedMS() int64 {
	if j._elapsedMS != 0 {
		return j._elapsedMS
	}
	ms, _ := uuid.ElapsedMilliSecondFromUniqueID(j.id)
	j._elapsedMS = ms
	return ms
}

// Marshal into binary of the format:
// {total len: 4 bytes}{ns len: 1 byte}{ns}{queue len: 1 byte}{queue}{id: 16 bytes}{ttl: 4 bytes}{tries: 2 byte}{job data}
func (j *jobImpl) MarshalBinary() (data []byte, err error) {
	nsLen := len(j.namespace)
	qLen := len(j.queue)
	bodyLen := len(j.body)
	totalSize := 1 + nsLen + 1 + qLen + 16 + 4 + 2 + bodyLen
	buf := make([]byte, totalSize+4)
	binary.LittleEndian.PutUint32(buf, uint32(totalSize))

	nsOffset := 4 + 1
	qOffset := nsOffset + nsLen + 1
	idOffset := qOffset + qLen
	ttlOffset := idOffset + 16
	triesOffset := ttlOffset + 4
	jobOffset := triesOffset + 2

	buf[4] = uint8(nsLen)
	copy(buf[nsOffset:], j.namespace)
	buf[qOffset-1] = uint8(qLen)
	copy(buf[qOffset:], j.queue)
	binID := uuid.UniqueIDToBinary(j.id)
	copy(buf[idOffset:], binID[:]) // binary ID is 16 byte-long
	binary.LittleEndian.PutUint32(buf[ttlOffset:], j.ttl)
	binary.LittleEndian.PutUint16(buf[triesOffset:], j.tries)
	copy(buf[jobOffset:], j.body)
	return buf, nil
}

func (j *jobImpl) UnmarshalBinary(data []byte) error {
	if len(data) <= 4 {
		return errors.New("data too small")
	}
	totalSize := binary.LittleEndian.Uint32(data[0:])
	if len(data) != int(totalSize)+4 {
		return errors.New("corrupted data")
	}

	nsLen := int(data[4])
	nsOffset := 4 + 1
	j.namespace = string(data[nsOffset : nsOffset+nsLen])
	qOffset := nsOffset + nsLen + 1
	qLen := int(data[qOffset-1])
	j.queue = string(data[qOffset : qOffset+qLen])
	idOffset := qOffset + qLen
	var binaryID [16]byte
	copy(binaryID[:], data[idOffset:idOffset+16])
	j.id = uuid.BinaryToUniqueID(binaryID)
	ttlOffset := idOffset + 16
	j.ttl = binary.LittleEndian.Uint32(data[ttlOffset:])
	triesOffset := ttlOffset + 4
	j.tries = binary.LittleEndian.Uint16(data[triesOffset:])
	jobOffset := triesOffset + 2
	j.body = make([]byte, len(data)-jobOffset)
	copy(j.body, data[jobOffset:])

	delay, err := uuid.ExtractDelaySecondFromUniqueID(j.id)
	if err != nil {
		return err
	}
	j.delay = delay
	return nil
}

func (j *jobImpl) GetDelayHour() uint16 {
	return 0
}
