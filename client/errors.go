package client

import "fmt"

// API error type. implements the Stringer interface.
type ErrType int

const (
	RequestErr ErrType = iota + 1
	ResponseErr
)

func (t ErrType) String() string {
	switch t {
	case RequestErr:
		return "req"
	case ResponseErr:
		return "resp"
	}
	return ""
}

// API error. implements the error interface.
type APIError struct {
	Type      ErrType
	Reason    string
	JobID     string
	RequestID string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("t:%s; m:%s; j:%s; r:%s", e.Type, e.Reason, e.JobID, e.RequestID)
}
