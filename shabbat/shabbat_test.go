package shabbat

import (
	"testing"
	"time"
)

// mockClock is a mock implementation of the Clock interface for testing.
type mockClock struct {
	currentTime time.Time
}

// Now returns the mock time.
func (mc *mockClock) Now() time.Time {
	return mc.currentTime
}

// setTime sets the current time of the mock clock.
func (mc *mockClock) setTime(t time.Time) {
	mc.currentTime = t
}

func TestIsShabbat(t *testing.T) {
	mockClock := &mockClock{}
	shabbatClock := NewShabbatClock(mockClock)

	testCases := []struct {
		name           string
		time           time.Time
		expectedShabbat bool
	}{
		{
			name:           "Friday before Shabbat",
			time:           time.Date(2024, 5, 10, 16, 59, 0, 0, time.UTC), // Friday 4:59 PM
			expectedShabbat: false,
		},
		{
			name:           "Friday at Shabbat start",
			time:           time.Date(2024, 5, 10, 17, 0, 0, 0, time.UTC), // Friday 5:00 PM
			expectedShabbat: true,
		},
		{
			name:           "Friday after Shabbat start",
			time:           time.Date(2024, 5, 10, 18, 0, 0, 0, time.UTC), // Friday 6:00 PM
			expectedShabbat: true,
		},
		{
			name:           "Saturday during Shabbat",
			time:           time.Date(2024, 5, 11, 10, 0, 0, 0, time.UTC), // Saturday 10:00 AM
			expectedShabbat: true,
		},
		{
			name:           "Saturday before Shabbat ends",
			time:           time.Date(2024, 5, 11, 19, 59, 0, 0, time.UTC), // Saturday 7:59 PM
			expectedShabbat: true,
		},
		{
			name:           "Saturday at Shabbat end",
			time:           time.Date(2024, 5, 11, 20, 0, 0, 0, time.UTC), // Saturday 8:00 PM
			expectedShabbat: false,
		},
		{
			name:           "Saturday after Shabbat ends",
			time:           time.Date(2024, 5, 11, 21, 0, 0, 0, time.UTC), // Saturday 9:00 PM
			expectedShabbat: false,
		},
		{
			name:           "Normal weekday",
			time:           time.Date(2024, 5, 13, 10, 0, 0, 0, time.UTC), // Monday 10:00 AM
			expectedShabbat: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockClock.setTime(tc.time)
			isShabbat := shabbatClock.IsShabbat()
			if isShabbat != tc.expectedShabbat {
				t.Errorf("Expected IsShabbat to be %v, but got %v", tc.expectedShabbat, isShabbat)
			}
		})
	}
}
