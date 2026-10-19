# Dockerized Broker + Producer + Consumer

This example starts a broker and runs one producer and one consumer.

The compose file mounts `examples/docker/rabbimq.yaml` to configure the broker passphrase
and passes the same config to producer/consumer so the `X-RabbiMQ-Key` header is sent.

```bash
cd examples/docker
docker compose up --build
```

The producer publishes a task to the `orders` channel and the consumer receives one task (`-once`), then exits. The broker reports healthy through `/healthz`.
