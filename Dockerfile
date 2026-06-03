FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o s3search ./cmd/s3search

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/s3search /usr/local/bin/s3search
ENTRYPOINT ["s3search", "server"]
