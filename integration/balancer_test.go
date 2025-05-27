package integration

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	url := fmt.Sprintf("%s/api/v1/some-data?key=gitpushforce", baseAddress)

	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected status: %d, body: %s", resp.StatusCode, body)
	}

	serverID := resp.Header.Get("lb-from")
	if serverID == "" {
		t.Fatalf("missing lb-from header")
	}
	t.Logf("served by: %s", serverID)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}

	if len(body) == 0 {
		t.Errorf("response body is empty")
	} else {
		t.Logf("response body: %s", string(body))
	}
}

func BenchmarkBalancer(b *testing.B) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		b.Skip("Integration benchmark is not enabled")
	}

	url := fmt.Sprintf("%s/api/v1/some-data?key=gitpushforce", baseAddress)
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(url)
		if err != nil {
			b.Fatalf("benchmark request %d failed: %v", i, err)
		}
		resp.Body.Close()
	}
}
