package broker

// Config defines broker limits.
type Config struct {
	// MaxQueueDepth limits total pending messages across all channels. 0 means unlimited.
	MaxQueueDepth int `json:"maxQueueDepth" yaml:"maxQueueDepth"`
	// MaxChannelDepth limits pending messages per channel. 0 means unlimited.
	MaxChannelDepth int `json:"maxChannelDepth" yaml:"maxChannelDepth"`
	// MaxChannels limits how many channels can be created. 0 means unlimited.
	MaxChannels int `json:"maxChannels" yaml:"maxChannels"`
	// MaxConsumers limits concurrent consumers. 0 means unlimited.
	MaxConsumers int `json:"maxConsumers" yaml:"maxConsumers"`
	// MaxMessageBytes limits the size of a message in bytes. 0 means default.
	MaxMessageBytes int `json:"maxMessageBytes" yaml:"maxMessageBytes"`
}

// normalize fills defaults.
func (c Config) normalize() Config {
	if c.MaxMessageBytes == 0 {
		c.MaxMessageBytes = 1 << 20
	}
	return c
}
