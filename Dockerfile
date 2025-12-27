FROM golang:1.25-alpine AS build

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /rabbimq ./cmd/rabbimq

FROM alpine:3.19
WORKDIR /data
COPY --from=build /rabbimq /usr/local/bin/rabbimq
EXPOSE 8080
CMD ["rabbimq"]
