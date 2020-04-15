package topic

import "strconv"

// ErrUnknownID is returned by Get() when a topic id is requested, that is lower
// then the lowest id in the topic.
type ErrUnknownID struct {
	// FirstID is the lowest id in the topic.
	FirstID uint64

	// ID is the id that was requested.
	ID uint64
}

func (e ErrUnknownID) Error() string {
	return "id " + strconv.FormatUint(e.ID, 10) + " is unknown in topic. Lowest id is " + strconv.FormatUint(e.FirstID, 10)
}
