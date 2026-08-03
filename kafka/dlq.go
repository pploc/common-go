package kafka

import (
	"context"
	"strconv"
	"strings"
	"time"
)

const (
	HeaderOriginalTopic    = "x-original-topic"
	HeaderExceptionMessage = "x-exception-message"
	HeaderFailedAt         = "x-failed-at"
	HeaderRetryCount       = "x-retry-count"
)

// RawPublisher publishes records without reserializing their key or framed value.
type RawPublisher interface {
	PublishRaw(context.Context, RawRecord) error
}

// DLQRecord creates the frozen DLQ representation. Original header values are
// copied byte-for-byte; only the four contract-defined headers are replaced.
func DLQRecord(record RawRecord, diagnostic string, attempts int, now time.Time) RawRecord {
	headers := make([]Header, 0, len(record.Headers)+4)
	for _, header := range record.Headers {
		switch strings.ToLower(header.Key) {
		case HeaderOriginalTopic, HeaderExceptionMessage, HeaderFailedAt, HeaderRetryCount:
			continue
		default:
			headers = append(headers, Header{Key: header.Key, Value: append([]byte(nil), header.Value...)})
		}
	}
	headers = append(headers,
		Header{Key: HeaderOriginalTopic, Value: []byte(record.Topic)},
		Header{Key: HeaderExceptionMessage, Value: []byte(diagnostic)},
		Header{Key: HeaderFailedAt, Value: []byte(strconv.FormatInt(now.UTC().UnixMilli(), 10))},
		Header{Key: HeaderRetryCount, Value: []byte(strconv.Itoa(attempts))},
	)
	return RawRecord{
		Topic: record.Topic + ".DLQ", Partition: record.Partition, Offset: record.Offset,
		Key: append([]byte(nil), record.Key...), Value: append([]byte(nil), record.Value...), Headers: headers,
	}
}
