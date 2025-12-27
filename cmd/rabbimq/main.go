package main

import (
	"RabbiMQ/api"
	"RabbiMQ/config"
	rmqhttp "RabbiMQ/http"
	"RabbiMQ/sdk"
	"bufio"
	"bytes"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	nethttp "net/http"
	"os"
	"strings"
	"time"
)

func main() {
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		switch os.Args[1] {
		case "publish":
			runPublish(os.Args[2:])
			return
		case "consume":
			runConsume(os.Args[2:])
			return
		default:
			fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
			os.Exit(2)
		}
	}

	runServer(os.Args[1:])
}

func runServer(args []string) {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:8080", "HTTP listen address")
	tlsCert := fs.String("tls-cert", "", "TLS certificate file")
	tlsKey := fs.String("tls-key", "", "TLS private key file")
	configPath := fs.String("config", "", "Server config file")
	fs.Parse(args)

	log.Println("Starting RabbiMQ...")

	serverCfg := config.ServerConfig{}
	if *configPath != "" {
		cfg, err := config.LoadServerConfig(*configPath)
		if err != nil {
			log.Fatalf("Failed to load config: %v", err)
		}
		serverCfg = cfg
	}

	// Create a new RabbiMQ instance with a persistent queue.
	mq, err := api.NewWithFileConfig("rabbimq.log", api.Config{Limits: serverCfg.Limits})
	if err != nil {
		log.Fatalf("Failed to create RabbiMQ instance: %v", err)
	}

	// Create and start the HTTP server.
	httpServer := rmqhttp.NewLocalServerWithAuth(mq, *addr, serverCfg.Passphrase)
	if *tlsCert != "" || *tlsKey != "" {
		if *tlsCert == "" || *tlsKey == "" {
			log.Fatal("both -tls-cert and -tls-key must be provided")
		}
		httpServer.StartTLS(*tlsCert, *tlsKey)
	} else {
		httpServer.Start()
	}

	log.Println("RabbiMQ started.")

	// Block forever.
	select {}
}

func runPublish(args []string) {
	fs := flag.NewFlagSet("publish", flag.ExitOnError)
	addr := fs.String("addr", "http://127.0.0.1:8080", "RabbiMQ HTTP address")
	channel := fs.String("channel", "", "Channel name")
	configPath := fs.String("config", "", "Client config file")
	passphrase := fs.String("passphrase", "", "Passphrase for authentication")
	insecure := fs.Bool("insecure", false, "skip TLS certificate verification")
	timeout := fs.Duration("timeout", 5*time.Second, "HTTP timeout")
	fs.Parse(args)

	if *configPath != "" {
		cfg, err := sdk.LoadConfig(*configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "config error: %v\n", err)
			os.Exit(1)
		}
		if *addr == "http://127.0.0.1:8080" && cfg.Address != "" {
			*addr = cfg.Address
		}
		if *passphrase == "" {
			*passphrase = cfg.Passphrase
		}
	}

	payload := strings.Join(fs.Args(), " ")
	if strings.TrimSpace(payload) == "" {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to read stdin: %v\n", err)
			os.Exit(1)
		}
		payload = string(bytes.TrimSpace(data))
	}

	if payload == "" {
		fmt.Fprintln(os.Stderr, "no task provided")
		os.Exit(1)
	}

	client := newHTTPClient(*timeout, *insecure)
	publishURL := *addr + "/publish"
	if *channel != "" {
		publishURL += "?channel=" + *channel
	}
	req, err := nethttp.NewRequest(nethttp.MethodPost, publishURL, bytes.NewReader([]byte(payload)))
	if err != nil {
		fmt.Fprintf(os.Stderr, "publish failed: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if *passphrase != "" {
		req.Header.Set("X-RabbiMQ-Key", *passphrase)
	}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "publish failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != nethttp.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = resp.Status
		}
		fmt.Fprintf(os.Stderr, "publish failed: %s\n", msg)
		os.Exit(1)
	}
}

func runConsume(args []string) {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	addr := fs.String("addr", "http://127.0.0.1:8080", "RabbiMQ HTTP address")
	channel := fs.String("channel", "", "Channel name")
	configPath := fs.String("config", "", "Client config file")
	passphrase := fs.String("passphrase", "", "Passphrase for authentication")
	insecure := fs.Bool("insecure", false, "skip TLS certificate verification")
	timeout := fs.Duration("timeout", 0, "HTTP timeout (0 disables)")
	once := fs.Bool("once", false, "exit after the first message")
	fs.Parse(args)

	if *configPath != "" {
		cfg, err := sdk.LoadConfig(*configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "config error: %v\n", err)
			os.Exit(1)
		}
		if *addr == "http://127.0.0.1:8080" && cfg.Address != "" {
			*addr = cfg.Address
		}
		if *passphrase == "" {
			*passphrase = cfg.Passphrase
		}
	}

	client := newHTTPClient(*timeout, *insecure)
	consumeURL := *addr + "/consume"
	if *channel != "" {
		consumeURL += "?channel=" + *channel
	}
	req, err := nethttp.NewRequest(nethttp.MethodGet, consumeURL, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "consume failed: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Accept", "text/event-stream")
	if *passphrase != "" {
		req.Header.Set("X-RabbiMQ-Key", *passphrase)
	}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "consume failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != nethttp.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = resp.Status
		}
		fmt.Fprintf(os.Stderr, "consume failed: %s\n", msg)
		os.Exit(1)
	}

	if err := streamConsume(resp.Body, *once); err != nil {
		fmt.Fprintf(os.Stderr, "consume failed: %v\n", err)
		os.Exit(1)
	}
}

func newHTTPClient(timeout time.Duration, insecure bool) *nethttp.Client {
	transport := &nethttp.Transport{}
	if insecure {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	return &nethttp.Client{
		Timeout:   timeout,
		Transport: transport,
	}
}

func streamConsume(r io.Reader, once bool) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var dataLines []string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if len(dataLines) > 0 {
				fmt.Println(strings.Join(dataLines, "\n"))
				if once {
					return nil
				}
				dataLines = dataLines[:0]
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	return scanner.Err()
}
