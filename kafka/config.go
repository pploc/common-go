package kafka

import (
	"fmt"
	"strings"
	"time"
)

// TransportConfig configures a franz-go transport without exposing client types.
// Registry registration is deliberately absent: production must use schemas that
// were registered before the library starts.
type TransportConfig struct {
	Brokers        []string
	Topics         []string
	ConsumerGroup  string
	PublishTimeout time.Duration
}

// Validate checks configuration shared by the acknowledged producer and manual
// commit consumer.
func (c TransportConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("kafka: at least one broker is required")
	}
	for _, broker := range c.Brokers {
		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf("kafka: broker addresses must not be blank")
		}
	}
	if c.PublishTimeout <= 0 {
		return fmt.Errorf("kafka: publish timeout must be positive")
	}
	for _, topic := range c.Topics {
		if strings.TrimSpace(topic) == "" {
			return fmt.Errorf("kafka: topics must not be blank")
		}
	}
	return nil
}

// ValidateConsumer checks configuration that is mandatory for manual commits.
func (c TransportConfig) ValidateConsumer() error {
	if err := c.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(c.ConsumerGroup) == "" {
		return fmt.Errorf("kafka: consumer group is required")
	}
	if len(c.Topics) == 0 {
		return fmt.Errorf("kafka: at least one consumed topic is required")
	}
	return nil
}
