package main

import (
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/dk872/architecture-lab5/datastore"
	"github.com/dk872/architecture-lab5/httptools"
	"github.com/dk872/architecture-lab5/signal"
)

var (
	port        = flag.Int("port", 8083, "server port")
	dbDir       = flag.String("dir", "./data", "path to db directory")
	segmentSize = flag.Int64("segmentSize", 1024, "max segment size in bytes")
)

type jsonResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type jsonRequest struct {
	Value string `json:"value"`
}

func main() {
	flag.Parse()

	if err := os.MkdirAll(*dbDir, 0755); err != nil {
		log.Fatalf("Failed to create db directory: %v", err)
	}

	db, err := datastore.Open(*dbDir, *segmentSize)
	if err != nil {
		log.Fatalf("Failed to open datastore: %v", err)
	}
	defer db.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("/db/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/db/")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		switch r.Method {
		case http.MethodGet:
			value, err := db.Get(key)
			if err != nil {
				if errors.Is(err, datastore.ErrNotFound) {
					http.Error(w, "not found", http.StatusNotFound)
				} else {
					http.Error(w, "internal error", http.StatusInternalServerError)
				}
				return
			}

			resp := jsonResponse{Key: key, Value: value}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)

		case http.MethodPost:
			var req jsonRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Value == "" {
				http.Error(w, "invalid JSON body", http.StatusBadRequest)
				return
			}

			if err := db.Put(key, req.Value); err != nil {
				http.Error(w, "failed to store value", http.StatusInternalServerError)
				return
			}

			w.WriteHeader(http.StatusOK)

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	server := httptools.CreateServer(*port, mux)
	server.Start()
	signal.WaitForTerminationSignal()
}
