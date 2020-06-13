package topic

import "strconv"

// UnknownIDError is returned by Receive() when a topic id is requested, that is
// lower then the lowest id in the topic.
type UnknownIDError struct {
	// FirstID is the lowest id in the topic.
	FirstID uint64

	// ID is the id that was requested.
	ID uint64
}

func (e UnknownIDError) Error() string {
	return "id " + strconv.FormatUint(e.ID, 10) + " is unknown in the topic. Lowest id is " + strconv.FormatUint(e.FirstID, 10)
}

type closingError struct{}

func (e closingError) Error() string { return "topic was closed" }
func (e closingError) Closing()      {}
