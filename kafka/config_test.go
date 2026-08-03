package kafka

import (
	"testing"
	"time"
)

func TestGivenTransportConfiguration_WhenValidating_ThenRejectsMissingRequiredValues(t *testing.T) {
	tests := []struct {
		name   string
		config TransportConfig
		check  func(TransportConfig) error
	}{
		{name: "no brokers", config: TransportConfig{PublishTimeout: time.Second}, check: TransportConfig.Validate},
		{name: "blank broker", config: TransportConfig{Brokers: []string{" "}, PublishTimeout: time.Second}, check: TransportConfig.Validate},
		{name: "missing timeout", config: TransportConfig{Brokers: []string{"broker:9092"}}, check: TransportConfig.Validate},
		{name: "missing consumer group", config: TransportConfig{Brokers: []string{"broker:9092"}, Topics: []string{"identity.user.registered.v1"}, PublishTimeout: time.Second}, check: TransportConfig.ValidateConsumer},
		{name: "missing consumed topic", config: TransportConfig{Brokers: []string{"broker:9092"}, ConsumerGroup: "consumer", PublishTimeout: time.Second}, check: TransportConfig.ValidateConsumer},
		{name: "blank consumed topic", config: TransportConfig{Brokers: []string{"broker:9092"}, ConsumerGroup: "consumer", Topics: []string{" "}, PublishTimeout: time.Second}, check: TransportConfig.ValidateConsumer},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Given
			config := test.config

			// When
			err := test.check(config)

			// Then
			if err == nil {
				t.Fatal("configuration was accepted")
			}
		})
	}
}

func TestGivenCompleteTransportConfiguration_WhenValidatingConsumer_ThenAcceptsIt(t *testing.T) {
	// Given
	config := TransportConfig{
		Brokers:        []string{"broker-1:9092", "broker-2:9092"},
		Topics:         []string{"identity.user.registered.v1"},
		ConsumerGroup:  "ms-gym-member",
		PublishTimeout: time.Second,
	}

	// When
	err := config.ValidateConsumer()

	// Then
	if err != nil {
		t.Fatalf("validate consumer configuration: %v", err)
	}
}
