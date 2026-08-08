FROM golang:1.25-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/ptt-alertor .

FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S ptt-alertor \
    && adduser -S -G ptt-alertor ptt-alertor

WORKDIR /app
COPY --from=builder /out/ptt-alertor /app/ptt-alertor
COPY --chown=ptt-alertor:ptt-alertor public/ /app/public/

USER ptt-alertor
EXPOSE 9090
ENTRYPOINT ["/app/ptt-alertor"]
