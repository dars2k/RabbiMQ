package http

import (
	"RabbiMQ/api"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

// LocalServer is an HTTP server that provides an API for a local RabbiMQ instance.
type LocalServer struct {
	mq         api.RabbiMQ
	httpAddr   string
	passphrase string
}

// NewLocalServer creates a new HTTP server for a local RabbiMQ instance.
func NewLocalServer(mq api.RabbiMQ, httpAddr string) *LocalServer {
	return NewLocalServerWithAuth(mq, httpAddr, "")
}

// NewLocalServerWithAuth creates a new HTTP server for a local RabbiMQ instance with auth.
func NewLocalServerWithAuth(mq api.RabbiMQ, httpAddr, passphrase string) *LocalServer {
	return &LocalServer{
		mq:         mq,
		httpAddr:   httpAddr,
		passphrase: passphrase,
	}
}

// Start starts the HTTP server.
func (s *LocalServer) Start() {
	http.HandleFunc("/publish", s.handlePublish)
	http.HandleFunc("/metrics", s.handleMetrics)
	http.HandleFunc("/channels", s.handleChannels)
	http.HandleFunc("/consume", s.handleConsume)
	log.Printf("HTTP server listening on %s", s.httpAddr)
	go func() {
		if err := http.ListenAndServe(s.httpAddr, nil); err != nil {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()
}

// StartTLS starts the HTTPS server using the provided certificate and key.
func (s *LocalServer) StartTLS(certFile, keyFile string) {
	http.HandleFunc("/publish", s.handlePublish)
	http.HandleFunc("/metrics", s.handleMetrics)
	http.HandleFunc("/channels", s.handleChannels)
	http.HandleFunc("/consume", s.handleConsume)
	log.Printf("HTTPS server listening on %s", s.httpAddr)
	go func() {
		if err := http.ListenAndServeTLS(s.httpAddr, certFile, keyFile, nil); err != nil {
			log.Fatalf("HTTPS server failed: %v", err)
		}
	}()
}

func (s *LocalServer) handlePublish(w http.ResponseWriter, r *http.Request) {
	if !s.authorize(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	task, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	channel := r.URL.Query().Get("channel")
	if err := s.mq.Publish(channel, task); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *LocalServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(s.mq.Metrics()); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (s *LocalServer) handleChannels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(s.mq.Channels()); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (s *LocalServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	if !s.authorize(w, r) {
		return
	}
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if err := s.mq.AcquireConsumer(); err != nil {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(err.Error()))
		return
	}
	defer s.mq.ReleaseConsumer()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	channel := r.URL.Query().Get("channel")
	tasks, err := s.mq.Consume(channel)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case task := <-tasks:
			if _, err := fmt.Fprintf(w, "data: %s\n\n", string(task)); err != nil {
				return
			}
			flusher.Flush()
		case <-ticker.C:
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *LocalServer) authorize(w http.ResponseWriter, r *http.Request) bool {
	if s.passphrase == "" {
		return true
	}
	if r.Header.Get("X-RabbiMQ-Key") != s.passphrase {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte("unauthorized"))
		return false
	}
	return true
}
