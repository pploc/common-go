package kafka

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const confluentMagicByte byte = 0

// Decode validates frozen metadata and the Confluent Protobuf envelope while
// preserving the raw input for a later DLQ decision.
func Decode(record RawRecord, resolver SchemaResolver) (DecodedRecord, error) {
	if resolver == nil {
		return DecodedRecord{}, Permanent{Err: fmt.Errorf("kafka: schema resolver is required")}
	}
	if err := ValidateCanonicalHeaders(record.Headers); err != nil {
		return DecodedRecord{}, Permanent{Err: err}
	}
	if err := validateHeaderValues(record.Headers); err != nil {
		return DecodedRecord{}, Permanent{Err: err}
	}
	schemaID, indexes, payload, err := decodeConfluentFrame(record.Value)
	if err != nil {
		return DecodedRecord{}, Permanent{Err: err}
	}
	message, err := resolver.Resolve(record.Topic, schemaID, indexes)
	if err != nil {
		return DecodedRecord{}, err
	}
	if framedResolver, ok := resolver.(FramedSchemaResolver); ok {
		message, err = framedResolver.DecodeFrame(record.Topic, record.Value)
		if err != nil {
			return DecodedRecord{}, err
		}
	} else if err := proto.Unmarshal(payload, message); err != nil {
		return DecodedRecord{}, Permanent{Err: fmt.Errorf("kafka: decode protobuf payload: %w", err)}
	}
	if err := validateFrozenPair(record.Topic, message); err != nil {
		return DecodedRecord{}, Permanent{Err: err}
	}
	if err := validateMessage(message); err != nil {
		return DecodedRecord{}, Permanent{Err: err}
	}
	eventType, _ := headerString(record.Headers, HeaderEventType)
	if eventType != string(message.ProtoReflect().Descriptor().FullName()) {
		return DecodedRecord{}, Permanent{Err: fmt.Errorf("kafka: event-type does not match decoded protobuf descriptor")}
	}
	return DecodedRecord{RawRecord: cloneRawRecord(record), Message: message}, nil
}

func decodeConfluentFrame(frame []byte) (schemaID int, indexes []int, payload []byte, err error) {
	if len(frame) < 6 {
		return 0, nil, nil, fmt.Errorf("kafka: truncated Confluent protobuf frame")
	}
	if frame[0] != confluentMagicByte {
		return 0, nil, nil, fmt.Errorf("kafka: invalid Confluent frame magic byte")
	}
	schemaID = int(binary.BigEndian.Uint32(frame[1:5]))
	if schemaID <= 0 {
		return 0, nil, nil, fmt.Errorf("kafka: invalid Schema Registry ID")
	}
	cursor := 5
	encodedCount, next, err := consumeUnsignedVarint(frame, cursor)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("kafka: decode message index count: %w", err)
	}
	cursor = next
	if encodedCount == 0 {
		return schemaID, []int{0}, append([]byte(nil), frame[cursor:]...), nil
	}
	count := protowire.DecodeZigZag(encodedCount)
	if count < 0 || count > 128 {
		return 0, nil, nil, fmt.Errorf("kafka: invalid protobuf message index count")
	}
	indexes = make([]int, 0, count)
	for range count {
		encodedIndex, next, err := consumeUnsignedVarint(frame, cursor)
		if err != nil {
			return 0, nil, nil, fmt.Errorf("kafka: decode message indexes: %w", err)
		}
		index := protowire.DecodeZigZag(encodedIndex)
		if index < 0 {
			return 0, nil, nil, fmt.Errorf("kafka: protobuf message index must not be negative")
		}
		indexes = append(indexes, int(index))
		cursor = next
	}
	return schemaID, indexes, append([]byte(nil), frame[cursor:]...), nil
}

func consumeUnsignedVarint(bytes []byte, offset int) (uint64, int, error) {
	if offset >= len(bytes) {
		return 0, offset, fmt.Errorf("truncated varint")
	}
	value, size := protowire.ConsumeVarint(bytes[offset:])
	if size < 0 {
		return 0, offset, protowire.ParseError(size)
	}
	return value, offset + size, nil
}

func validateHeaderValues(headers []Header) error {
	eventType, _ := headerString(headers, HeaderEventType)
	if strings.TrimSpace(eventType) == "" {
		return fmt.Errorf("kafka: event-type is blank")
	}
	timestamp, _ := headerString(headers, HeaderTimestamp)
	if value, err := strconv.ParseInt(timestamp, 10, 64); err != nil || value < 0 {
		return fmt.Errorf("kafka: timestamp must be a decimal Unix epoch millisecond value")
	}
	traceparent, _ := headerString(headers, HeaderTraceParent)
	if !isValidTraceparent(traceparent) {
		return fmt.Errorf("kafka: traceparent is invalid")
	}
	return nil
}

func isValidTraceparent(value string) bool {
	parts := strings.Split(value, "-")
	if len(parts) != 4 || len(parts[0]) != 2 || len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return false
	}
	for _, part := range parts {
		for _, r := range part {
			if !strings.ContainsRune("0123456789abcdef", r) {
				return false
			}
		}
	}
	return true
}

func cloneRawRecord(record RawRecord) RawRecord {
	headers := make([]Header, len(record.Headers))
	for index, header := range record.Headers {
		headers[index] = Header{Key: header.Key, Value: append([]byte(nil), header.Value...)}
	}
	return RawRecord{
		Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
		Key: append([]byte(nil), record.Key...), Value: append([]byte(nil), record.Value...), Headers: headers,
	}
}
