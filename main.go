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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds the structure for our external JSON file
type Config struct {
	RedisAddress  string `json:"redis_address"`
	RedisHostname string `json:"redis_hostname"`
	RedisPassword string `json:"redis_password"`
	HTTPPort      int    `json:"http_port"`
	TLSEnabled    bool   `json:"tls_enabled"`
	TLSCACert     string `json:"tls_ca_cert"`
	TLSCert       string `json:"tls_cert"`
	TLSKey        string `json:"tls_key"`
}

var globalConfig Config

const timeout = 2 * time.Second

func loadConfig(path string) (Config, error) {
	var cfg Config
	file, err := os.Open(path)
	if err != nil {
		return cfg, err
	}

	defer func() {
		if err := file.Close(); err != nil {
			slog.Default().Warn("Failed to close file", slog.Any("error", err))
		}
	}()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&cfg)
	return cfg, err
}

// Helper function to send commands to and read responses from Redis
func sendCommand(rw *bufio.ReadWriter, command string, logger *slog.Logger) (string, error) {
	logCmd := command
	if strings.HasPrefix(command, "AUTH") {
		logCmd = "AUTH *****"
	}

	logger.Debug("Sending command to Redis", "command", logCmd)

	_, err := rw.WriteString(command + "\r\n")
	if err != nil {
		return "", err
	}
	if err := rw.Flush(); err != nil {
		return "", err
	}

	line, err := rw.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSpace(line)

	if strings.HasPrefix(line, "$") {
		logger.Debug("Redis returned bulk string length indicator", "line", line)
		lengthStr := strings.TrimSpace(line)[1:]
		logger.Debug("Redis returned bulk string length indicator", "lengthStr", lengthStr)
		length, err := strconv.Atoi(lengthStr)
		if err != nil {
			return "", fmt.Errorf("Failed to parse bulk string length: %w", err)
		}

		if length == -1 {
			return "", nil // In Go, nil or empty string denotes the null/empty condition
		}
		if length < 0 {
			return "", fmt.Errorf("Invalid negative bulk string length: %d", length)
		}

		payloadBuffer := make([]byte, length)
		_, err = io.ReadFull(rw, payloadBuffer)
		if err != nil {
			return "", fmt.Errorf("Failed to read bulk string payload: %w", err)
		}

		// Consume the trailing \r\n from the reader
		if _, err := rw.Discard(2); err != nil {
			return "", fmt.Errorf("Failed to discard trailing CRLF: %w", err)
		}

		payload := string(payloadBuffer)
		logger.Debug("Redis bulk string payload received", "bytes", len(payload))
		return payload, nil
	}

	logger.Debug("Redis inline response received", "response", line)
	return line, nil
}

func masterHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	reqLogger := slog.With("client_ip", r.RemoteAddr, "path", r.URL.Path)
	reqLogger.Debug("Received /master request")

	var conn net.Conn
	var err error

	dialer := &net.Dialer{Timeout: timeout}
	if globalConfig.TLSEnabled {
		// Load Client Certificate and Key
		cert, err := tls.LoadX509KeyPair(globalConfig.TLSCert, globalConfig.TLSKey)
		if err != nil {
			reqLogger.Error("Failed to load client certificate/key", "error", err)
			reqLogger.Debug("TLSCert=", "debug", globalConfig.TLSCert)
			reqLogger.Debug("TLSKey=", "debug", globalConfig.TLSKey)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		// Load CA Certificate to verify Redis Server
		caCert, err := os.ReadFile(globalConfig.TLSCACert)
		if err != nil {
			reqLogger.Error("Failed to read CA certificate", "error", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		// Create TLS Configuration
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
			ServerName:   globalConfig.RedisHostname,
		}

		conn, err = tls.DialWithDialer(dialer, "tcp", globalConfig.RedisAddress, tlsConfig)
	} else {
		conn, err = dialer.Dial("tcp", globalConfig.RedisAddress)
	}

	if err != nil {
		reqLogger.Error("Master-check failed: cannot connect to Redis port", "error", err)
		http.Error(w, "Redis Down", http.StatusServiceUnavailable)
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			slog.Default().Warn("Failed to close connection", slog.Any("error", err))
		}
	}()

	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	// 2. AUTH using config-defined password
	res, err := sendCommand(rw, fmt.Sprintf("AUTH %s", globalConfig.RedisPassword), reqLogger)
	if err != nil || !strings.Contains(res, "+OK") {
		reqLogger.Error("Master-check failed: AUTH rejected", "response", res, "error", err)
		http.Error(w, "Unauthorized", http.StatusServiceUnavailable)
		return
	}

	// 3. PING
	res, err = sendCommand(rw, "PING", reqLogger)
	if err != nil || !strings.Contains(res, "+PONG") {
		reqLogger.Error("Matser-check failed: PING failed", "response", res, "error", err)
		http.Error(w, "PING Failed", http.StatusServiceUnavailable)
		return
	}

	// 4. INFO REPLICATION
	res, err = sendCommand(rw, "INFO replication", reqLogger)
	if err != nil {
		reqLogger.Error("Master-check failed: INFO replication failed", "error", err)
		http.Error(w, "INFO Failed", http.StatusServiceUnavailable)
		return
	}

	// 5. Parse output for "role:master"
	if !strings.Contains(res, "role:master") {
		reqLogger.Info("Master-check completed: Node is healthy but it is a REPLICA", "duration_ms", time.Since(start).Milliseconds())
		http.Error(w, "Not Master", http.StatusServiceUnavailable)
		return
	}

	// 6. QUIT safely
	_, _ = sendCommand(rw, "QUIT", reqLogger)

	reqLogger.Info("Master-check completed: Node is healthy and MASTER", "duration_ms", time.Since(start).Milliseconds())
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK - Node is Master"))
}

func main() {
	// Parse CLI flags (allows changing config path on the fly)
	configPath := flag.String("config", "/etc/redis-hc-sidecar.json", "Path to configuration file")
	flag.Parse()

	// Handle Log levels via ENV
	logLevel := slog.LevelInfo
	if envLogLevel := os.Getenv("LOG_LEVEL"); envLogLevel != "" {
		var parsedLevel slog.Level

		err := parsedLevel.UnmarshalText([]byte(strings.ToUpper(envLogLevel)))
		if err == nil {
			logLevel = parsedLevel
		} else {
			slog.Warn("Invalid LOG_LEVEL missing or malformed, defaulting to INFO", "err", err)
		}
	}
	if strings.ToLower(os.Getenv("LOG_LEVEL")) == "debug" {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	// Load configuration file
	var err error
	globalConfig, err = loadConfig(*configPath)
	if err != nil {
		slog.Error("Failed to load configuration file", "path", *configPath, "error", err)
		os.Exit(1)
	}

	slog.Info("Starting SCS Redis Health Check Sidecar service",
		"config_path", *configPath,
		"redis_target", globalConfig.RedisAddress,
		"http_port", globalConfig.HTTPPort,
		"log_level", logLevel.String(),
	)

	http.HandleFunc("/master", masterHandler)
	addr := fmt.Sprintf(":%d", globalConfig.HTTPPort)
	if err := http.ListenAndServe(addr, nil); err != nil {
		slog.Error("Could not start HTTP server", "error", err)
		os.Exit(1)
	}
}
