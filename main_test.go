/*
Copyright 2026 Southern Cross Solutions (Pty) Ltd

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bufio"
	"bytes"
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

// TestWriter captures bytes and sends them to t.Log
type TestWriter struct {
	t *testing.T
}

func (tw TestWriter) Write(p []byte) (n int, err error) {
	// Strip trailing newline if you don't want empty lines in your test output
	tw.t.Log(string(p))
	return len(p), nil
}

// TestLoadConfig verifies JSON parsing behavior
func TestLoadConfig(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "config-*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	content := `{"redis_address": "10.0.0.1:6379", "redis_password": "test-password", "http_port": 9000}`
	if _, err := tmpFile.Write([]byte(content)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	defer func() { _ = tmpFile.Close() }()

	cfg, err := loadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("loadConfig failed: %v", err)
	}

	if cfg.RedisAddress != "10.0.0.1:6379" || cfg.RedisPassword != "test-password" || cfg.HTTPPort != 9000 {
		t.Errorf("Unexpected config values parsed: %+v", cfg)
	}
}

// TestSendCommand verifies low-level Redis command formatting and response processing
func TestSendCommand(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	tests := []struct {
		name         string
		redisInput   string
		command      string
		expectedResp string
		expectErr    bool
	}{
		{
			name:         "Inline Simple String Status OK",
			redisInput:   "+OK\r\n",
			command:      "AUTH secret",
			expectedResp: "+OK",
			expectErr:    false,
		},
		{
			name:         "Inline Simple String Pong",
			redisInput:   "+PONG\r\n",
			command:      "PING",
			expectedResp: "+PONG",
			expectErr:    false,
		},
		{
			name:         "Bulk String Info Replication - Single line",
			redisInput:   "$11\r\nrole:master\r\n",
			command:      "INFO replication",
			expectedResp: "role:master",
			expectErr:    false,
		},
		// {
		// 	name: "Bulk String Info Replication - Multi line with comment",
		// 	redisInput: "$349\r\n# Replication\r\nrole:master\r\nconnected_slaves:0\r\n" +
		// 		"master_failover_state:no-failover\r\n" +
		// 		"master_replid:628aa9e80558abae3e2f2d88f989763eb8774941\r\n" +
		// 		"master_replid2:0000000000000000000000000000000000000000\r\n" +
		// 		"master_repl_offset:0\r\nsecond_repl_offset:-1\r\n" +
		// 		"repl_backlog_active:0\r\nrepl_backlog_size:1048576" +
		// 		"repl_backlog_first_byte_offset:0\r\nrepl_backlog_histlen:0\r\n",
		// 	command:      "INFO replication",
		// 	expectedResp: "role:master",
		// 	expectErr:    false,
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var inBuf bytes.Buffer
			var outBuf bytes.Buffer
			inBuf.WriteString(tt.redisInput)

			rw := bufio.NewReadWriter(bufio.NewReader(&inBuf), bufio.NewWriter(&outBuf))
			resp, err := sendCommand(rw, tt.command, logger)

			if (err != nil) != tt.expectErr {
				t.Fatalf("Expected error: %v, got: %v", tt.expectErr, err)
			}
			if resp != tt.expectedResp {
				t.Errorf("Expected response %q, got %q", tt.expectedResp, resp)
			}
			if !bytes.Contains(outBuf.Bytes(), []byte(tt.command+"\r\n")) {
				t.Errorf("Expected command %q to be sent, found buffer: %q", tt.command, outBuf.String())
			}
		})
	}
}

// TestHealthCheckHandlerWithMockServer uses a real TCP loopback listener to simulate various Redis behaviors
func TestHealthCheckHandlerWithMockServer(t *testing.T) {
	// Initialize default logger to silence outputs during testing
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	// UNcomment below to enable actual logging
	// logger := slog.New(slog.NewTextHandler(TestWriter{t: t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	// slog.SetDefault(logger)

	tests := []struct {
		name           string
		redisResponses []string
		expectedStatus int
	}{
		{
			name: "Successful Master Validation - Single line",
			redisResponses: []string{
				"+OK\r\n",                // AUTH response
				"+PONG\r\n",              // PING response
				"$11\r\nrole:master\r\n", // INFO replication response
				"+OK\r\n",                // QUIT response
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "Successful Master Validation - Multi line",
			redisResponses: []string{
				"+OK\r\n",   // AUTH response
				"+PONG\r\n", // PING response
				"$349\r\n# Replication\r\nrole:master\r\nconnected_slaves:0\r\n" +
					"master_failover_state:no-failover\r\n" +
					"master_replid:628aa9e80558abae3e2f2d88f989763eb8774941\r\n" +
					"master_replid2:0000000000000000000000000000000000000000\r\n" +
					"master_repl_offset:0\r\nsecond_repl_offset:-1\r\n" +
					"repl_backlog_active:0\r\nrepl_backlog_size:1048576\r\n" +
					"repl_backlog_first_byte_offset:0\r\nrepl_backlog_histlen:0\r\n" +
					"\r\n",
				// INFO replication response
				"+OK\r\n", // QUIT response
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "Failed Master Validation because node is a Replica",
			redisResponses: []string{
				"+OK\r\n",
				"+PONG\r\n",
				"$12\r\nrole:slave\r\n\r\n", // Node indicates it's a replica/slave
			},
			expectedStatus: http.StatusServiceUnavailable,
		},
		{
			name: "Auth Rejection from Redis",
			redisResponses: []string{
				"-ERR invalid password\r\n",
			},
			expectedStatus: http.StatusServiceUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Start a local fake Redis server mock socket
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("Failed to start fake redis listener: %v", err)
			}
			defer func() { _ = listener.Close() }()

			// Wire up the mock server behavior asynchronously
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func(ctx context.Context) {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer func() { _ = conn.Close() }()

				reader := bufio.NewReader(conn)
				for _, mockResp := range tt.redisResponses {
					// Read what our client app sidecar sends (discarding just to satisfy pipe)
					_, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					// Write back our specific scripted test scenario string
					_, _ = conn.Write([]byte(mockResp))
				}
			}(ctx)

			// Point our global application config target directly to this localized temp port
			globalConfig = Config{
				RedisAddress:  listener.Addr().String(),
				RedisPassword: "test-password",
				HTTPPort:      8000,
			}

			// Execute the HTTP Server Request against the implementation handler
			req := httptest.NewRequest("GET", "/master", nil)
			rr := httptest.NewRecorder()

			masterHandler(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Errorf("Handler returned incorrect status code: got %v want %v", rr.Code, tt.expectedStatus)
			}
		})
	}
}
