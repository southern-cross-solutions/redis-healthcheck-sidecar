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
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// Config holds the structure for our external JSON file
type Config struct {
	RedisAddress  string `json:"redis_address"`
	RedisPassword string `json:"redis_password"`
	HTTPPort      int    `json:"http_port"`
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
		payload, err := rw.ReadString('\n')
		if err != nil {
			return "", err
		}
		trimmedPayload := strings.TrimSpace(payload)
		logger.Debug("Redis bulk string payload received", "bytes", len(trimmedPayload))
		return trimmedPayload, nil
	}

	logger.Debug("Redis inline response received", "response", line)
	return line, nil
}

func masterHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	reqLogger := slog.With("client_ip", r.RemoteAddr, "path", r.URL.Path)
	reqLogger.Debug("Received /master request")

	// 1. Connect using config-defined address
	conn, err := net.DialTimeout("tcp", globalConfig.RedisAddress, timeout)
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

	_ = conn.SetDeadline(time.Now().Add(timeout))
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
