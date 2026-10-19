package main

import (
	"RabbiMQ/api"
	"RabbiMQ/config"
	rmqhttp "RabbiMQ/http"
	"RabbiMQ/sdk"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const (
	defaultServerAddr = "127.0.0.1:8080"
	defaultClientAddr = "http://127.0.0.1:8080"
	defaultDataFile   = "rabbimq.log"
	shutdownTimeout   = 10 * time.Second
)

const usage = `Usage:
  rabbimq [server flags]                 run the broker
  rabbimq publish [flags] [message...]   publish a message (reads stdin if none given)
  rabbimq consume [flags]                stream messages to stdout
  rabbimq metrics [flags]                print broker metrics as JSON
  rabbimq channels [flags]               print channel queue depths as JSON

Run "rabbimq <command> -h" for the flags of a command.
`

// errUsage marks errors caused by invalid command-line usage.
var errUsage = errors.New("usage error")

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

// run executes the CLI and returns the process exit code.
func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var err error
	cmd := ""
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}

	switch cmd {
	case "", "serve", "server":
		err = runServer(ctx, args, stderr)
	case "publish":
		err = runPublish(ctx, args, stdin, stderr)
	case "consume":
		err = runConsume(ctx, args, stdout, stderr)
	case "metrics":
		err = runMetrics(ctx, args, stdout, stderr)
	case "channels":
		err = runChannels(ctx, args, stdout, stderr)
	case "help":
		fmt.Fprint(stdout, usage)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command: %s\n\n%s", cmd, usage)
		return 2
	}

	switch {
	case err == nil, errors.Is(err, flag.ErrHelp):
		return 0
	case errors.Is(err, errUsage):
		fmt.Fprintln(stderr, err)
		return 2
	default:
		name := cmd
		if name == "" {
			name = "server"
		}
		fmt.Fprintf(stderr, "%s failed: %v\n", name, err)
		return 1
	}
}

func newFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}

// parseFlags parses args and reports which flags were set explicitly.
func parseFlags(fs *flag.FlagSet, args []string) (map[string]bool, error) {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %v", errUsage, err)
	}
	set := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	return set, nil
}

func runServer(ctx context.Context, args []string, stderr io.Writer) error {
	fs := newFlagSet("server", stderr)
	addr := fs.String("addr", defaultServerAddr, "HTTP listen address")
	tlsCert := fs.String("tls-cert", "", "TLS certificate file")
	tlsKey := fs.String("tls-key", "", "TLS private key file")
	configPath := fs.String("config", "", "Server config file (YAML or JSON)")
	dataFile := fs.String("data", defaultDataFile, "Queue log file for the default channel; other channels are stored next to it")
	set, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("%w: unexpected arguments: %s", errUsage, strings.Join(fs.Args(), " "))
	}
	if (*tlsCert == "") != (*tlsKey == "") {
		return fmt.Errorf("%w: both -tls-cert and -tls-key must be provided", errUsage)
	}

	serverCfg := config.ServerConfig{}
	if *configPath != "" {
		cfg, err := config.LoadServerConfig(*configPath)
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}
		serverCfg = cfg
	}
	if !set["addr"] && serverCfg.Addr != "" {
		*addr = serverCfg.Addr
	}
	if !set["data"] && serverCfg.DataFile != "" {
		*dataFile = serverCfg.DataFile
	}

	log.Println("Starting RabbiMQ...")
	mq, err := api.NewWithFileConfig(*dataFile, api.Config{Limits: serverCfg.Limits})
	if err != nil {
		return fmt.Errorf("create broker: %w", err)
	}

	httpServer := rmqhttp.NewLocalServerWithOptions(mq, rmqhttp.Options{
		Addr:         *addr,
		Passphrase:   serverCfg.Passphrase,
		MaxBodyBytes: int64(serverCfg.Limits.Normalize().MaxMessageBytes),
	})

	serveErr := make(chan error, 1)
	go func() {
		if *tlsCert != "" {
			serveErr <- httpServer.ListenAndServeTLS(*tlsCert, *tlsKey)
		} else {
			serveErr <- httpServer.ListenAndServe()
		}
	}()
	log.Println("RabbiMQ started.")

	select {
	case err := <-serveErr:
		mq.Shutdown()
		return err
	case <-ctx.Done():
	}

	log.Println("Shutting down RabbiMQ...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	httpDone := make(chan error, 1)
	// Stop accepting new requests, then shut the broker down, which ends
	// open consume streams so the HTTP shutdown can complete.
	go func() { httpDone <- httpServer.Shutdown(shutdownCtx) }()
	mq.Shutdown()
	if err := <-httpDone; err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}
	if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	log.Println("RabbiMQ stopped.")
	return nil
}

// clientFlags are the connection flags shared by client commands.
type clientFlags struct {
	fs         *flag.FlagSet
	addr       *string
	channel    *string
	configPath *string
	passphrase *string
	caCert     *string
	insecure   *bool
	timeout    *time.Duration
}

func newClientFlags(name string, stderr io.Writer, defaultTimeout time.Duration, withChannel bool) *clientFlags {
	fs := newFlagSet(name, stderr)
	cf := &clientFlags{
		fs:         fs,
		addr:       fs.String("addr", defaultClientAddr, "RabbiMQ HTTP address"),
		configPath: fs.String("config", "", "Client config file (YAML or JSON)"),
		passphrase: fs.String("passphrase", "", "Passphrase for authentication"),
		caCert:     fs.String("ca-cert", "", "CA certificate file for verifying the server"),
		insecure:   fs.Bool("insecure", false, "skip TLS certificate verification"),
		timeout:    fs.Duration("timeout", defaultTimeout, "HTTP timeout"),
	}
	if withChannel {
		cf.channel = fs.String("channel", "", "Channel name")
	}
	return cf
}

// client parses args and builds an SDK client. Values from the config file
// are used unless the corresponding flag was given explicitly.
func (cf *clientFlags) client(args []string) (*sdk.Client, error) {
	set, err := parseFlags(cf.fs, args)
	if err != nil {
		return nil, err
	}

	cfg := sdk.Config{}
	if *cf.configPath != "" {
		cfg, err = sdk.LoadConfig(*cf.configPath)
		if err != nil {
			return nil, fmt.Errorf("config error: %w", err)
		}
	}
	if set["addr"] || cfg.Address == "" {
		cfg.Address = *cf.addr
	}
	if set["passphrase"] || cfg.Passphrase == "" {
		cfg.Passphrase = *cf.passphrase
	}
	if set["ca-cert"] {
		cfg.CACertFile = *cf.caCert
	}
	if set["insecure"] {
		cfg.InsecureSkipVerify = *cf.insecure
	}
	if set["timeout"] || cfg.Timeout == "" {
		cfg.Timeout = cf.timeout.String()
	}
	if cf.channel != nil && set["channel"] {
		cfg.Channel = *cf.channel
	}

	client, err := sdk.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errUsage, err)
	}
	return client, nil
}

func runPublish(ctx context.Context, args []string, stdin io.Reader, stderr io.Writer) error {
	cf := newClientFlags("publish", stderr, 5*time.Second, true)
	client, err := cf.client(args)
	if err != nil {
		return err
	}

	payload := []byte(strings.Join(cf.fs.Args(), " "))
	if len(bytes.TrimSpace(payload)) == 0 {
		data, err := io.ReadAll(stdin)
		if err != nil {
			return fmt.Errorf("failed to read stdin: %w", err)
		}
		payload = bytes.TrimSpace(data)
	}
	if len(payload) == 0 {
		return fmt.Errorf("%w: no task provided", errUsage)
	}

	return client.Publish(ctx, "", payload)
}

func runConsume(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	cf := newClientFlags("consume", stderr, 0, true)
	once := cf.fs.Bool("once", false, "exit after the first message")
	client, err := cf.client(args)
	if err != nil {
		return err
	}

	if *once {
		msg, ok, err := client.Consume(ctx, "")
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if ok {
			fmt.Fprintln(stdout, string(msg))
		}
		return nil
	}

	msgs, errs := client.Subscribe(ctx, "")
	for msg := range msgs {
		fmt.Fprintln(stdout, string(msg))
	}
	// The error channel is closed without a value when ctx was canceled.
	return <-errs
}

func runMetrics(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	client, err := newClientFlags("metrics", stderr, 5*time.Second, false).client(args)
	if err != nil {
		return err
	}
	metrics, err := client.Metrics(ctx)
	if err != nil {
		return err
	}
	return printJSON(stdout, metrics)
}

func runChannels(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	client, err := newClientFlags("channels", stderr, 5*time.Second, false).client(args)
	if err != nil {
		return err
	}
	channels, err := client.Channels(ctx)
	if err != nil {
		return err
	}
	return printJSON(stdout, channels)
}

func printJSON(w io.Writer, v interface{}) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
