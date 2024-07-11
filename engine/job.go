package engine

import (
	"encoding"
	"encoding/json"

	"github.com/bitleak/lmstfy/uuid"
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
	Attributes() map[string]string

	encoding.TextMarshaler
}

type jobImpl struct {
	namespace  string
	queue      string
	id         string
	body       []byte
	ttl        uint32
	delay      uint32
	tries      uint16
	attributes map[string]string

	_elapsedMS int64
}

// NOTE: there is a trick in this factory, the delay is embedded in the jobID.
// By doing this we can delete the job that's located in hourly AOF, by placing
// a tombstone record in that AOF.
func NewJob(namespace, queue string, body []byte, attributes map[string]string, ttl, delay uint32, tries uint16, jobID string) Job {
	if jobID == "" {
		jobID = uuid.GenJobIDWithVersion(0, delay)
	}
	return &jobImpl{
		namespace:  namespace,
		queue:      queue,
		id:         jobID,
		body:       body,
		ttl:        ttl,
		delay:      delay,
		tries:      tries,
		attributes: attributes,
	}
}

func NewJobWithID(namespace, queue string, body []byte, attributes map[string]string, ttl uint32, tries uint16, jobID string) Job {
	delay, _ := uuid.ExtractDelaySecondFromUniqueID(jobID)
	return &jobImpl{
		namespace:  namespace,
		queue:      queue,
		id:         jobID,
		body:       body,
		ttl:        ttl,
		delay:      delay,
		tries:      tries,
		attributes: attributes,
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

func (j *jobImpl) Attributes() map[string]string {
	return j.attributes
}

func (j *jobImpl) MarshalText() (text []byte, err error) {
	var job struct {
		Namespace  string            `json:"namespace"`
		Queue      string            `json:"queue"`
		ID         string            `json:"id"`
		TTL        uint32            `json:"ttl"`
		ElapsedMS  int64             `json:"elapsed_ms"`
		Body       []byte            `json:"body"`
		Attributes map[string]string `json:"attributes,omitempty"`
	}
	job.Namespace = j.namespace
	job.Queue = j.queue
	job.ID = j.id
	job.TTL = j.ttl
	job.ElapsedMS = j._elapsedMS
	job.Body = j.body
	job.Attributes = j.attributes
	return json.Marshal(job)
}

func (j *jobImpl) GetDelayHour() uint16 {
	return 0
}
