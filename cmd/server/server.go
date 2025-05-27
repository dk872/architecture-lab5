package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dk872/architecture-lab5/httptools"
	"github.com/dk872/architecture-lab5/signal"
)

var port = flag.Int("port", 8080, "server port")

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"
const dbServiceURL = "http://db:8083/db"

func postCurrentDate(key string) error {
	value := time.Now().Format("2006-01-02")

	body := fmt.Sprintf(`{"value":"%s"}`, value)
	resp, err := http.DefaultClient.Post(
		fmt.Sprintf("%s/%s", dbServiceURL, key),
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("failed to POST to db: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("POST failed with status %d: %s", resp.StatusCode, data)
	}
	return nil
}

func waitForDBAndPostDate(key string, retries int, delay time.Duration) error {
	for i := 0; i < retries; i++ {
		err := postCurrentDate(key)
		if err == nil {
			return nil
		}
		log.Printf("Retry %d/%d: %v", i+1, retries, err)
		time.Sleep(delay)
	}
	return fmt.Errorf("failed to initialize DB value after %d retries", retries)
}

func main() {
	flag.Parse()

	if err := waitForDBAndPostDate("gitpushforce", 10, 1*time.Second); err != nil {
		log.Fatalf("Failed to initialize DB value: %v", err)
	}

	h := new(http.ServeMux)

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		if delaySecStr := os.Getenv(confResponseDelaySec); delaySecStr != "" {
			if delaySec, err := strconv.Atoi(delaySecStr); err == nil && delaySec > 0 && delaySec < 300 {
				time.Sleep(time.Duration(delaySec) * time.Second)
			}
		}

		report.Process(r)

		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(rw, "missing key parameter", http.StatusBadRequest)
			return
		}

		resp, err := http.DefaultClient.Get(fmt.Sprintf("%s/%s", dbServiceURL, key))
		if err != nil {
			http.Error(rw, "failed to query DB", http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			http.NotFound(rw, r)
			return
		} else if resp.StatusCode != http.StatusOK {
			data, _ := io.ReadAll(resp.Body)
			http.Error(rw, string(data), resp.StatusCode)
			return
		}

		var result struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			http.Error(rw, "invalid response from DB", http.StatusInternalServerError)
			return
		}

		rw.Header().Set("content-type", "application/json")
		json.NewEncoder(rw).Encode(result)
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
