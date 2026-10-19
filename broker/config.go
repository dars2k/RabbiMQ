package broker

import "fmt"

// DefaultMaxMessageBytes is the message size limit used when none is configured.
const DefaultMaxMessageBytes = 1 << 20

// Config defines broker limits.
type Config struct {
	// MaxQueueDepth limits total pending messages across all channels. 0 means unlimited.
	MaxQueueDepth int `json:"maxQueueDepth" yaml:"maxQueueDepth"`
	// MaxChannelDepth limits pending messages per channel. 0 means unlimited.
	MaxChannelDepth int `json:"maxChannelDepth" yaml:"maxChannelDepth"`
	// MaxChannels limits how many channels can exist, including the default channel. 0 means unlimited.
	MaxChannels int `json:"maxChannels" yaml:"maxChannels"`
	// MaxConsumers limits concurrent consumers. 0 means unlimited.
	MaxConsumers int `json:"maxConsumers" yaml:"maxConsumers"`
	// MaxMessageBytes limits the size of a message in bytes. 0 means DefaultMaxMessageBytes.
	MaxMessageBytes int `json:"maxMessageBytes" yaml:"maxMessageBytes"`
}

// Validate reports configuration values that cannot be honored.
func (c Config) Validate() error {
	limits := []struct {
		name  string
		value int
	}{
		{"maxQueueDepth", c.MaxQueueDepth},
		{"maxChannelDepth", c.MaxChannelDepth},
		{"maxChannels", c.MaxChannels},
		{"maxConsumers", c.MaxConsumers},
		{"maxMessageBytes", c.MaxMessageBytes},
	}
	for _, limit := range limits {
		if limit.value < 0 {
			return fmt.Errorf("%s must not be negative, got %d", limit.name, limit.value)
		}
	}
	return nil
}

// Normalize returns a copy of the config with defaults filled in.
func (c Config) Normalize() Config {
	if c.MaxMessageBytes == 0 {
		c.MaxMessageBytes = DefaultMaxMessageBytes
	}
	return c
}
