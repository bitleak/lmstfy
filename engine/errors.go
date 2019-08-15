package engine

import "errors"

var (
	ErrNotFound   = errors.New("job not found")
	ErrWrongQueue = errors.New("wrong queue for the job")
)
