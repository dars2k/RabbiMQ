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
		name            string
		time            time.Time
		expectedShabbat bool
	}{
		{
			name:            "Friday before Shabbat",
			time:            time.Date(2024, 5, 10, 16, 59, 0, 0, time.UTC), // Friday 4:59 PM
			expectedShabbat: false,
		},
		{
			name:            "Friday at Shabbat start",
			time:            time.Date(2024, 5, 10, 17, 0, 0, 0, time.UTC), // Friday 5:00 PM
			expectedShabbat: true,
		},
		{
			name:            "Friday after Shabbat start",
			time:            time.Date(2024, 5, 10, 18, 0, 0, 0, time.UTC), // Friday 6:00 PM
			expectedShabbat: true,
		},
		{
			name:            "Saturday during Shabbat",
			time:            time.Date(2024, 5, 11, 10, 0, 0, 0, time.UTC), // Saturday 10:00 AM
			expectedShabbat: true,
		},
		{
			name:            "Saturday before Shabbat ends",
			time:            time.Date(2024, 5, 11, 19, 59, 0, 0, time.UTC), // Saturday 7:59 PM
			expectedShabbat: true,
		},
		{
			name:            "Saturday at Shabbat end",
			time:            time.Date(2024, 5, 11, 20, 0, 0, 0, time.UTC), // Saturday 8:00 PM
			expectedShabbat: false,
		},
		{
			name:            "Saturday after Shabbat ends",
			time:            time.Date(2024, 5, 11, 21, 0, 0, 0, time.UTC), // Saturday 9:00 PM
			expectedShabbat: false,
		},
		{
			name:            "Normal weekday",
			time:            time.Date(2024, 5, 13, 10, 0, 0, 0, time.UTC), // Monday 10:00 AM
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

func TestNextTransition(t *testing.T) {
	tests := []struct {
		name       string
		now        time.Time
		wantShabat bool
		want       time.Time
	}{
		{
			name: "Monday waits for Friday",
			now:  time.Date(2024, 5, 13, 10, 0, 0, 0, time.UTC),
			want: time.Date(2024, 5, 17, 17, 0, 0, 0, time.UTC),
		},
		{
			name: "Friday morning waits for same evening",
			now:  time.Date(2024, 5, 10, 9, 0, 0, 0, time.UTC),
			want: time.Date(2024, 5, 10, 17, 0, 0, 0, time.UTC),
		},
		{
			name:       "Friday night ends Saturday evening",
			now:        time.Date(2024, 5, 10, 18, 0, 0, 0, time.UTC),
			wantShabat: true,
			want:       time.Date(2024, 5, 11, 20, 0, 0, 0, time.UTC),
		},
		{
			name:       "Saturday afternoon ends same evening",
			now:        time.Date(2024, 5, 11, 15, 0, 0, 0, time.UTC),
			wantShabat: true,
			want:       time.Date(2024, 5, 11, 20, 0, 0, 0, time.UTC),
		},
		{
			name: "Saturday night waits for next Friday",
			now:  time.Date(2024, 5, 11, 21, 0, 0, 0, time.UTC),
			want: time.Date(2024, 5, 17, 17, 0, 0, 0, time.UTC),
		},
		{
			name: "Month boundary",
			now:  time.Date(2024, 5, 30, 12, 0, 0, 0, time.UTC),
			want: time.Date(2024, 5, 31, 17, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sc := NewShabbatClock(&FixedClock{Time: tc.now})
			gotShabbat, got := sc.NextTransition()
			if gotShabbat != tc.wantShabat {
				t.Fatalf("shabbat = %v, want %v", gotShabbat, tc.wantShabat)
			}
			if !got.Equal(tc.want) {
				t.Fatalf("next transition = %v, want %v", got, tc.want)
			}
			if sc.IsShabbat() != gotShabbat {
				t.Fatal("NextTransition disagrees with IsShabbat")
			}
			statusShabbat, remaining := sc.Status()
			if want := tc.want.Sub(tc.now); statusShabbat != gotShabbat || remaining != want {
				t.Fatalf("Status = %v %v, want %v %v", statusShabbat, remaining, gotShabbat, want)
			}
		})
	}
}

func TestRealClock(t *testing.T) {
	before := time.Now()
	got := (&RealClock{}).Now()
	if got.Before(before) {
		t.Fatalf("real clock went backwards: %v < %v", got, before)
	}
}
