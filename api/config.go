package api

import "RabbiMQ/broker"

// Config defines API broker configuration.
type Config struct {
	Limits broker.Config `json:"limits"`
}
