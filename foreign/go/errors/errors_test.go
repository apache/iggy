package ierror

import (
	"testing"
)

func TestIggyError_Error(t *testing.T) {
	iggyErr := &IggyError{
		Code:    42,
		Message: "test_error",
	}

	expectedErrorString := "42: 'test_error'"
	actualErrorString := iggyErr.Error()

	if expectedErrorString != actualErrorString {
		t.Errorf("Error() method mismatch, expected: %s, got: %s", expectedErrorString, actualErrorString)
	}
}
