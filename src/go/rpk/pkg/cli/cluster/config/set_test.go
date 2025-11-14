package config

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestParseArgs(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		expected  []string
		expectErr bool
	}{
		{
			name:      "valid key=value format",
			args:      []string{"key=value"},
			expected:  []string{"key=value"},
			expectErr: false,
		},
		{
			name:      "valid key=value key2=value2 format",
			args:      []string{"key=value", "key2=value2"},
			expected:  []string{"key=value", "key2=value2"},
			expectErr: false,
		},
		{
			name:      "valid key and value as separate arguments",
			args:      []string{"key", "value"},
			expected:  []string{"key=value"},
			expectErr: false,
		},
		{
			name:      "invalid format without '='",
			args:      []string{"key", "value1", "value2"},
			expected:  nil,
			expectErr: true,
		},
		{
			name:      "invalid single argument without '='",
			args:      []string{"key"},
			expected:  nil,
			expectErr: true,
		},
		{
			name:      "invalid multiple arguments without '='",
			args:      []string{"key", "value1", "key", "value2"},
			expected:  nil,
			expectErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := parseArgs(test.args)
			if test.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, result)
			}
		})
	}
}

func TestPollOperationStatus(t *testing.T) {
	tests := []struct {
		name               string
		initialState       controlplanev1.Operation_State
		transitionToState  controlplanev1.Operation_State
		transitionAfter    int // number of polls before transition
		expectCompleted    bool
		expectTimeout      bool
		expectedFinalState controlplanev1.Operation_State
	}{
		{
			name:               "operation completes immediately",
			initialState:       controlplanev1.Operation_STATE_COMPLETED,
			expectCompleted:    true,
			expectTimeout:      false,
			expectedFinalState: controlplanev1.Operation_STATE_COMPLETED,
		},
		{
			name:               "operation fails immediately",
			initialState:       controlplanev1.Operation_STATE_FAILED,
			expectCompleted:    true,
			expectTimeout:      false,
			expectedFinalState: controlplanev1.Operation_STATE_FAILED,
		},
		{
			name:               "operation transitions from in_progress to completed",
			initialState:       controlplanev1.Operation_STATE_IN_PROGRESS,
			transitionToState:  controlplanev1.Operation_STATE_COMPLETED,
			transitionAfter:    2, // transition after 2 polls
			expectCompleted:    true,
			expectTimeout:      false,
			expectedFinalState: controlplanev1.Operation_STATE_COMPLETED,
		},
		{
			name:               "operation transitions from in_progress to failed",
			initialState:       controlplanev1.Operation_STATE_IN_PROGRESS,
			transitionToState:  controlplanev1.Operation_STATE_FAILED,
			transitionAfter:    3,
			expectCompleted:    true,
			expectTimeout:      false,
			expectedFinalState: controlplanev1.Operation_STATE_FAILED,
		},
		{
			name:               "operation stays in_progress and times out",
			initialState:       controlplanev1.Operation_STATE_IN_PROGRESS,
			transitionToState:  controlplanev1.Operation_STATE_IN_PROGRESS,
			transitionAfter:    100, // never transitions
			expectCompleted:    false,
			expectTimeout:      true,
			expectedFinalState: controlplanev1.Operation_STATE_IN_PROGRESS,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var pollCount atomic.Int32
			operationID := "test-operation-id"

			// Create a mock server that simulates the operation status API
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Check that it's a GetOperation request
				require.Contains(t, r.URL.Path, "redpanda.api.controlplane.v1.OperationService/GetOperation")

				count := pollCount.Add(1)
				state := tt.initialState

				// Transition to the target state after the specified number of polls
				if tt.transitionAfter > 0 && int(count) >= tt.transitionAfter {
					state = tt.transitionToState
				}

				response := &controlplanev1.GetOperationResponse{
					Operation: &controlplanev1.Operation{
						Id:    operationID,
						State: state,
					},
				}

				// Use application/proto content type for Connect RPC
				w.Header().Set("Content-Type", "application/proto")
				w.WriteHeader(http.StatusOK)

				// Marshal the response using proto binary format
				marshaled, err := proto.Marshal(response)
				require.NoError(t, err)
				_, _ = w.Write(marshaled)
			}))
			defer server.Close()

			// Create a cloud client pointing to our mock server
			cloudClient := publicapi.NewCloudClientSet(server.URL, "test-token")

			ctx := context.Background()
			startTime := time.Now()

			// Call the function under test
			operation, completedInTime, err := pollOperationStatus(ctx, cloudClient, operationID)

			// Verify no error occurred
			require.NoError(t, err)
			require.NotNil(t, operation)

			// Verify the completion status
			require.Equal(t, tt.expectCompleted, completedInTime, "completedInTime mismatch")

			// Verify the final state
			require.Equal(t, tt.expectedFinalState, operation.GetState(), "final state mismatch")

			// Verify operation ID is preserved
			require.Equal(t, operationID, operation.Id)

			// Additional time-based checks
			elapsed := time.Since(startTime)
			if tt.expectTimeout {
				// Should take at least 10 seconds when timing out
				require.GreaterOrEqual(t, elapsed, 10*time.Second, "should have waited full timeout period")
			} else {
				// Should complete before the 10 second timeout
				require.Less(t, elapsed, 10*time.Second, "should complete before timeout")
			}

			// Verify polling happened the expected number of times
			if tt.expectTimeout {
				// Should poll multiple times (approximately 10 times for 10 seconds with 1 second interval)
				require.GreaterOrEqual(t, pollCount.Load(), int32(9), "should have polled multiple times during timeout")
			} else if tt.transitionAfter > 0 {
				// Should have polled at least until the transition
				require.GreaterOrEqual(t, pollCount.Load(), int32(tt.transitionAfter), "should have polled until transition")
			}
		})
	}
}

func TestPollOperationStatus_ContextCancellation(t *testing.T) {
	operationID := "test-operation-id"

	// Create a mock server that always returns in_progress
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := &controlplanev1.GetOperationResponse{
			Operation: &controlplanev1.Operation{
				Id:    operationID,
				State: controlplanev1.Operation_STATE_IN_PROGRESS,
			},
		}

		w.Header().Set("Content-Type", "application/proto")
		w.WriteHeader(http.StatusOK)

		marshaled, err := proto.Marshal(response)
		require.NoError(t, err)
		_, _ = w.Write(marshaled)
	}))
	defer server.Close()

	cloudClient := publicapi.NewCloudClientSet(server.URL, "test-token")

	// Create a context that we'll cancel after 500ms (during the first sleep)
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel the context after 500ms
	go func() {
		time.Sleep(500 * time.Millisecond)
		cancel()
	}()

	startTime := time.Now()
	_, _, err := pollOperationStatus(ctx, cloudClient, operationID)
	elapsed := time.Since(startTime)

	// Should get a context cancellation error
	require.Error(t, err)
	require.Contains(t, err.Error(), "context cancelled while polling operation status")

	// Should return quickly after cancellation (within ~600ms), not wait for full polling interval
	// Give some buffer for test execution, but it should be much less than 2 seconds
	require.Less(t, elapsed, 1500*time.Millisecond, "should have been cancelled immediately, not waited for next poll")
	require.GreaterOrEqual(t, elapsed, 500*time.Millisecond, "should have waited at least until cancellation")
}

func TestPollOperationStatus_APIError(t *testing.T) {
	operationID := "test-operation-id"

	// Create a mock server that returns an error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"code":"internal","message":"internal server error"}`))
	}))
	defer server.Close()

	cloudClient := publicapi.NewCloudClientSet(server.URL, "test-token")

	ctx := context.Background()
	_, _, err := pollOperationStatus(ctx, cloudClient, operationID)

	// Should return an error
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to get operation status")
}
