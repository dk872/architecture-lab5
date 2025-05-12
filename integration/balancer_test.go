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

    const tries = 5
    url := fmt.Sprintf("%s/api/v1/some-data", baseAddress)

    resp, err := client.Get(url)
    if err != nil {
        t.Fatalf("first request failed: %v", err)
    }

    t.Logf("response from [%s]", resp.Header.Get("lb-from"))
    resp.Body.Close()

    firstFrom := resp.Header.Get("lb-from")
    if firstFrom == "" {
        t.Fatalf("missing lb-from header on first request")
    }

    for i := 1; i < tries; i++ {
        resp, err := client.Get(url)
        if err != nil {
            t.Fatalf("request %d failed: %v", i, err)
        }

        t.Logf("response %d from [%s]", i, resp.Header.Get("lb-from"))
        resp.Body.Close()

        from := resp.Header.Get("lb-from")
        if from != firstFrom {
            t.Errorf("request %d: expected lb-from=%q, got %q", i, firstFrom, from)
        }
    }

    t.Logf("All %d requests went to %s as expected for variant 1", tries, firstFrom)
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
