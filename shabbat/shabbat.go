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

// FixedClock returns a fixed time, useful for tests.
type FixedClock struct {
	Time time.Time
}

// Now returns the fixed time.
func (c *FixedClock) Now() time.Time {
	return c.Time
}

// ShabbatClock determines if it is currently Shabbat.
type ShabbatClock struct {
	clock Clock
}

// NewShabbatClock creates a new ShabbatClock with the given Clock.
func NewShabbatClock(clock Clock) *ShabbatClock {
	return &ShabbatClock{clock: clock}
}

const (
	// StartHour is the hour on Friday when Shabbat begins.
	StartHour = 17
	// EndHour is the hour on Saturday when Shabbat ends.
	EndHour = 20
)

// IsShabbat checks if the current time is during Shabbat.
// Shabbat is considered to be from Friday 5 PM to Saturday 8 PM.
func (sc *ShabbatClock) IsShabbat() bool {
	return isShabbatAt(sc.clock.Now())
}

// NextTransition returns whether it is currently Shabbat and the time at
// which that state next changes: the end of Shabbat while it is observed,
// otherwise the start of the next Shabbat.
func (sc *ShabbatClock) NextTransition() (bool, time.Time) {
	return nextTransitionAt(sc.clock.Now())
}

func nextTransitionAt(now time.Time) (bool, time.Time) {
	year, month, day := now.Date()
	loc := now.Location()

	switch {
	case now.Weekday() == time.Friday && now.Hour() >= StartHour:
		return true, time.Date(year, month, day+1, EndHour, 0, 0, 0, loc)
	case now.Weekday() == time.Saturday && now.Hour() < EndHour:
		return true, time.Date(year, month, day, EndHour, 0, 0, 0, loc)
	}

	daysUntilFriday := (int(time.Friday) - int(now.Weekday()) + 7) % 7
	if daysUntilFriday == 0 && now.Hour() >= StartHour {
		daysUntilFriday = 7
	}
	return false, time.Date(year, month, day+daysUntilFriday, StartHour, 0, 0, 0, loc)
}

// Status returns whether it is currently Shabbat and how long until that
// state next changes.
func (sc *ShabbatClock) Status() (bool, time.Duration) {
	now := sc.clock.Now()
	isShabbat, next := nextTransitionAt(now)
	return isShabbat, next.Sub(now)
}

func isShabbatAt(now time.Time) bool {
	weekday := now.Weekday()
	hour := now.Hour()

	// Friday
	if weekday == time.Friday && hour >= StartHour {
		return true
	}

	// Saturday
	if weekday == time.Saturday && hour < EndHour {
		return true
	}

	return false
}
