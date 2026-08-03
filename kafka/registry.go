package kafka

import (
	"errors"
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rest"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	confluentprotobuf "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	eventsv1 "github.com/pploc/proto-go/events/v1"
	"google.golang.org/protobuf/proto"
)

// RegistryConfig identifies a Confluent Schema Registry. Schema registration is
// intentionally not configurable because production is lookup-only.
type RegistryConfig struct {
	URL string
}

// ConfluentProtobufRegistry uses Confluent's official Go Schema Registry serde
// for complete frame encoding and decoding. It uses TopicNameStrategy and never
// auto-registers schemas.
type ConfluentProtobufRegistry struct {
	client       schemaregistry.Client
	serializer   *confluentprotobuf.Serializer
	deserializer *confluentprotobuf.Deserializer
}

// NewConfluentProtobufRegistry creates a bounded-cache official serde adapter.
// The caller must pre-register all frozen schemas at BACKWARD compatibility. Encoding
// uses the subject's latest registered schema, avoiding generator-specific descriptor
// formatting differences while preserving lookup-only production behavior.
func NewConfluentProtobufRegistry(config RegistryConfig) (*ConfluentProtobufRegistry, error) {
	if strings.TrimSpace(config.URL) == "" {
		return nil, fmt.Errorf("kafka: Schema Registry URL is required")
	}
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(config.URL))
	if err != nil {
		return nil, fmt.Errorf("create Schema Registry client: %w", err)
	}
	serializerConfig := confluentprotobuf.NewSerializerConfig()
	serializerConfig.AutoRegisterSchemas = false
	serializerConfig.CacheSchemas = true
	serializerConfig.UseLatestVersion = true
	serializerConfig.SubjectNameStrategyType = serde.TopicNameStrategyType
	serializer, err := confluentprotobuf.NewSerializer(client, serde.ValueSerde, serializerConfig)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("create Protobuf serializer: %w", err)
	}
	deserializerConfig := confluentprotobuf.NewDeserializerConfig()
	deserializerConfig.SubjectNameStrategyType = serde.TopicNameStrategyType
	deserializer, err := confluentprotobuf.NewDeserializer(client, serde.ValueSerde, deserializerConfig)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("create Protobuf deserializer: %w", err)
	}
	for _, message := range frozenMessages() {
		if err := deserializer.ProtoRegistry.RegisterMessage(message.ProtoReflect().Type()); err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("register frozen Protobuf descriptor: %w", err)
		}
	}
	return &ConfluentProtobufRegistry{client: client, serializer: serializer, deserializer: deserializer}, nil
}

// Encode emits the official complete Confluent Protobuf value with TopicNameStrategy.
func (r *ConfluentProtobufRegistry) Encode(topic string, message proto.Message) ([]byte, error) {
	if err := validateFrozenPair(topic, message); err != nil {
		return nil, err
	}
	frame, err := r.serializer.Serialize(topic, message)
	if err != nil {
		return nil, classifyRegistryError("serialize registered Protobuf schema", err)
	}
	return frame, nil
}

// Resolve supplies a concrete generated message for structural validation. The
// official DecodeFrame method remains the authority for Registry resolution.
func (r *ConfluentProtobufRegistry) Resolve(topic string, _ int, _ []int) (proto.Message, error) {
	message, ok := frozenMessageForTopic(topic)
	if !ok {
		return nil, Permanent{Err: fmt.Errorf("kafka: topic %q is not part of the frozen v1 contract", topic)}
	}
	return message, nil
}

// DecodeFrame delegates Schema Registry ID and message-index resolution to the
// official Confluent deserializer.
func (r *ConfluentProtobufRegistry) DecodeFrame(topic string, frame []byte) (proto.Message, error) {
	decoded, err := r.deserializer.Deserialize(topic, frame)
	if err != nil {
		return nil, classifyRegistryError("deserialize Confluent Protobuf frame", err)
	}
	message, ok := decoded.(proto.Message)
	if !ok || message == nil {
		return nil, Permanent{Err: fmt.Errorf("kafka: Schema Registry returned a non-Protobuf message")}
	}
	return message, nil
}

// Close releases Schema Registry client resources.
func (r *ConfluentProtobufRegistry) Close() error {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.Close()
}

func classifyRegistryError(operation string, err error) error {
	var registryError *rest.Error
	if errors.As(err, &registryError) && registryError.Code >= 400 && registryError.Code < 500 {
		return Permanent{Err: fmt.Errorf("kafka: %s: %w", operation, err)}
	}
	return fmt.Errorf("kafka: %s: %w", operation, err)
}

func frozenMessageForTopic(topic string) (proto.Message, bool) {
	for _, message := range frozenMessages() {
		if expectedType, ok := frozenTopicTypes[topic]; ok && string(message.ProtoReflect().Descriptor().FullName()) == expectedType {
			return message.ProtoReflect().New().Interface(), true
		}
	}
	return nil, false
}

func frozenMessages() []proto.Message {
	return []proto.Message{
		&eventsv1.UserRegisteredEvent{},
		&eventsv1.UserSuspendedEvent{},
		&eventsv1.UserRoleChangedEvent{},
		&eventsv1.PaymentCompletedEvent{},
		&eventsv1.MembershipActivatedEvent{},
		&eventsv1.MembershipPausedEvent{},
		&eventsv1.MembershipResumedEvent{},
		&eventsv1.MembershipExpiringSoonEvent{},
		&eventsv1.MembershipExpiredEvent{},
	}
}
