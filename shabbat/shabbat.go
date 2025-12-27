package shabbat

import (
	"time"
)

// Clock defines an interface for getting the current time, allowing for dependency injection in tests.
type Clock interface {
	Now() time.Time
}

// RealClock is a concrete implementation of Clock that returns the actual current time.
type RealClock struct{}

// Now returns the current time.
func (c *RealClock) Now() time.Time {
	return time.Now()
}

// ShabbatClock determines if it is currently Shabbat.
type ShabbatClock struct {
	clock Clock
}

// NewShabbatClock creates a new ShabbatClock with the given Clock.
func NewShabbatClock(clock Clock) *ShabbatClock {
	return &ShabbatClock{clock: clock}
}

// IsShabbat checks if the current time is during Shabbat.
// Shabbat is considered to be from Friday 5 PM to Saturday 8 PM.
func (sc *ShabbatClock) IsShabbat() bool {
	now := sc.clock.Now()
	weekday := now.Weekday()
	hour := now.Hour()

	// Friday
	if weekday == time.Friday && hour >= 17 {
		return true
	}

	// Saturday
	if weekday == time.Saturday && hour < 20 {
		return true
	}

	return false
}
