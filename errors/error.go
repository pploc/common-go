package errors

import "fmt"

// Error is a categorized domain failure. Code is a stable, service-defined
// application code emitted through the x-error-code trailer.
type Error struct {
	Category Category
	Code     string
	Message  string
	cause    error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message != "" {
		return e.Message
	}
	if e.cause != nil {
		return e.cause.Error()
	}
	return string(e.Category)
}

// Unwrap retains the original cause for standard errors.Is and errors.As.
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// New creates a categorized domain error.
func New(category Category, code, message string) *Error {
	return &Error{Category: category, Code: code, Message: message}
}

// Wrap attaches category and code to a cause without losing its identity.
func Wrap(cause error, category Category, code, message string) *Error {
	if cause == nil {
		return New(category, code, message)
	}
	if message == "" {
		message = fmt.Sprintf("%s", cause)
	}
	return &Error{Category: category, Code: code, Message: message, cause: cause}
}
