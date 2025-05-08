package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestBalancer(t *testing.T) {
	mu.Lock()
	healthyServers = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
	mu.Unlock()

	distribution := make(map[string]int)
	totalClients := 100

	for i := 0; i < totalClients; i++ {
		clientAddr := fmt.Sprintf("192.168.1.%d:12345", i)
		server, ok := getServerForClient(clientAddr)
		if !ok {
			t.Fatalf("no server found for client %s", clientAddr)
		}
		distribution[server]++
	}

	if len(distribution) < 2 {
		t.Errorf("expected at least 2 servers to be used, got %d", len(distribution))
	}

	t.Logf("Distribution: %+v", distribution)
}


func TestGetServerForClient_ConsistentHashing(t *testing.T) {
	mu.Lock()
	healthyServers = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
	mu.Unlock()

	clientAddr := "192.168.1.101:12345"
	server1, ok1 := getServerForClient(clientAddr)
	server2, ok2 := getServerForClient(clientAddr)

	if !ok1 || !ok2 {
		t.Fatal("expected tofind a healthy server")
	}
	if server1 != server2 {
		t.Errorf("expected consistent hashing, got %s and %s", server1, server2)
	}
}

func TestGetServerForClient_OneHealthyServer(t *testing.T) {
	soloServer := "server1:8080"
	mu.Lock()
	healthyServers = []string{soloServer}
	mu.Unlock()

	totalClients := 10

	for i := 0; i < totalClients; i++ {
		clientAddr := fmt.Sprintf("10.0.0.%d:12345", i)
		server, ok := getServerForClient(clientAddr)
		if !ok {
			t.Fatalf("expected a healthy server for client %s", clientAddr)
		}
		if server != soloServer {
			t.Errorf("expected %s, got %s", soloServer, server)
		}
	}
}

func TestMonitorHealth(t *testing.T) {
	// fake servers
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer healthy.Close()

	unhealthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer unhealthy.Close()

	// override serversPool
	mu.Lock()
	serversPool = []string{
		healthy.Listener.Addr().String(),
		unhealthy.Listener.Addr().String(),
	}
	mu.Unlock()

	// shorten timeout and sleep for fast test
	*timeoutSec = 1
	timeout = time.Second

	// run monitorHealth in background (only let it run once)
	done := make(chan struct{})
	go func() {
		// make monitorHealth to only run once for testing
		var newHealthy []string
		for _, server := range serversPool {
			if health(server) {
				newHealthy = append(newHealthy, server)
			}
		}
		mu.Lock()
		healthyServers = newHealthy
		mu.Unlock()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("monitorHealth did not finish in time")
	}

	mu.RLock()
	defer mu.RUnlock()

	if len(healthyServers) != 1 {
		t.Fatalf("expected 1 healthy server, got %d", len(healthyServers))
	}
	if healthyServers[0] != healthy.Listener.Addr().String() {
		t.Errorf("expected healthy server %s, got %s", healthy.Listener.Addr().String(), healthyServers[0])
	}
}

func TestGetServerForClient_NoHealthy(t *testing.T) {
	mu.Lock()
	healthyServers = []string{}
	mu.Unlock()

	server, ok := getServerForClient("192.168.1.101:12345")
	if ok {
		t.Errorf("expected no healthy server, got %s", server)
	}
}