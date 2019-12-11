package engine

import "errors"

var (
	ErrNotFound   = errors.New("job not found")
	ErrEmptyQueue = errors.New("the queue is empty")
	ErrWrongQueue = errors.New("wrong queue for the job")
)