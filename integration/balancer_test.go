package integration

import (
	"fmt"
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

    const tries = 10
    url := fmt.Sprintf("%s/api/v1/some-data", baseAddress)

    serversSeen := make(map[string]bool)

    for i := 0; i < tries; i++ {
        resp, err := client.Get(url)
        if err != nil {
            t.Fatalf("request %d failed: %v", i, err)
        }

        serverID := resp.Header.Get("lb-from")
        resp.Body.Close()

        if serverID == "" {
            t.Fatalf("request %d missing lb-from header", i)
        }

        t.Logf("request %d served by %s", i, serverID)
        serversSeen[serverID] = true
    }

    if len(serversSeen) < 2 {
        t.Errorf("expected requests to be distributed to at least 2 different servers, got %d", len(serversSeen))
    } else {
        t.Logf("requests distributed to %d different servers as expected", len(serversSeen))
    }
}

func BenchmarkBalancer(b *testing.B) {
    if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
        b.Skip("Integration benchmark is not enabled")
    }

    url := fmt.Sprintf("%s/api/v1/some-data", baseAddress)
    for i := 0; i < b.N; i++ {
        resp, err := client.Get(url)
        if err != nil {
            b.Fatalf("benchmark request %d failed: %v", i, err)
        }
        resp.Body.Close()
    }
}
