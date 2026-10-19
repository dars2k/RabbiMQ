FROM golang:1.25-alpine AS build

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /rabbimq ./cmd/rabbimq

FROM alpine:3.20
RUN addgroup -S rabbimq && adduser -S -G rabbimq rabbimq \
    && mkdir -p /data && chown rabbimq:rabbimq /data
WORKDIR /data
COPY --from=build /rabbimq /usr/local/bin/rabbimq
USER rabbimq
EXPOSE 8080
VOLUME ["/data"]
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8080/healthz || exit 1
# Listen on all interfaces so the published port is reachable from the host.
CMD ["rabbimq", "-addr", "0.0.0.0:8080"]
