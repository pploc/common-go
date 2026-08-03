package logging

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/pploc/common-go/auth"
)

func TestWithClaimsOmitsSensitiveFields(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	WithClaims(logger, auth.Claims{UserID: "secret-user", Role: auth.RoleAdmin, GymID: "gym-1", TraceID: "trace"}).Info("request")
	line := output.String()
	if !strings.Contains(line, "role=ADMIN") {
		t.Fatalf("missing role field: %s", line)
	}
	if strings.Contains(line, "secret-user") || strings.Contains(line, "trace") || strings.Contains(line, "gym-1") {
		t.Fatalf("logged sensitive fields: %s", line)
	}
}

func TestFromContextFallsBackToDefault(t *testing.T) {
	if FromContext(context.Background()) == nil {
		t.Fatal("expected default logger")
	}
}
