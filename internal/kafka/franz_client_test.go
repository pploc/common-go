package kafka

import "testing"

func TestGivenMutablePrivateHeaders_WhenConvertingToAndFromFranz_ThenCopiesHeaderBytes(t *testing.T) {
	// Given
	headers := []Header{{Key: "event-id", Value: []byte("event-1")}}

	// When
	franzHeaders := toFranzHeaders(headers)
	headers[0].Value[0] = 'X'
	if got := string(franzHeaders[0].Value); got != "event-1" {
		t.Fatalf("franz header = %q, want defensive copy", got)
	}
	roundTripped := fromFranzHeaders(franzHeaders)
	franzHeaders[0].Value[0] = 'Y'

	// Then
	if got := string(roundTripped[0].Value); got != "event-1" {
		t.Fatalf("round-tripped header = %q, want defensive copy", got)
	}
}
