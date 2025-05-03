package main

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/dk872/architecture-lab4/httptools"
	"github.com/dk872/architecture-lab4/signal"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	timeout        = time.Duration(*timeoutSec) * time.Second
	mu             sync.RWMutex
	healthyServers []string
	serversPool    = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
)

// scheme повертає поточну схему запиту — http або https
func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

// health перевіряє здоров'я бекенд-сервера за адресою dst
func health(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)

	// Створюємо GET-запит до /health endpoint
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false // Сервер не відповідає або недоступний
	}
	if resp.StatusCode != http.StatusOK {
		return false // Сервер не пройшов перевірку здоров’я
	}

	return true
}

// monitorHealth постійно перевіряє стан серверів і оновлює healthyServers
func monitorHealth() {
	for {
		var newHealthy []string
		for _, server := range serversPool {
			if health(server) {
				newHealthy = append(newHealthy, server)
			}
		}
		mu.Lock()
		healthyServers = newHealthy
		mu.Unlock()
		time.Sleep(10 * time.Second)
	}
}

// getServerForClient вибирає один здоровий сервер для конкретного клієнта (хешуванням)
func getServerForClient(addr string) (string, bool) {
	mu.RLock()
	defer mu.RUnlock()
	if len(healthyServers) == 0 {
		return "", false
	}

	// Генеруємо хеш-значення від адреси клієнта
	hash := sha1.Sum([]byte(addr))
	num := binary.BigEndian.Uint32(hash[:4])
	idx := int(num) % len(healthyServers)

	return healthyServers[idx], true
}

// forward перенаправляє запит клієнта до вибраного бекенд-сервера
func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

// main — точка входу програми, запускає балансувальник і моніторинг
func main() {
	flag.Parse()

	// Запускаємо моніторинг у фоновому режимі
	go monitorHealth()

	// Створюємо HTTP-сервер (балансувальник навантаження)
	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		clientAddr := r.RemoteAddr

		// Визначаємо, до якого сервера перенаправити клієнта
		server, ok := getServerForClient(clientAddr)
		if !ok {
			http.Error(rw, "No healthy backend available", http.StatusServiceUnavailable)
			return
		}

		// Форвардимо запит на відповідний сервер
		_ = forward(server, rw, r)
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
