# MQTTaton — MQTT v5 Broker (Zero Dependencies)

A full MQTT v5 broker in a single Go file, built entirely on the Go standard library.

## What It Does

MQTTaton listens on a TCP port and brokers MQTT v5 messages between clients:

- **CONNECT / CONNACK** — client authentication handshake
- **PUBLISH** — QoS 0 (at most once), QoS 1 (at least once), QoS 2 (exactly once)
- **SUBSCRIBE / SUBACK** — topic filter subscription with wildcard matching
- **UNSUBSCRIBE / UNSUBACK** — subscription removal
- **PINGREQ / PINGRESP** — keepalive
- **DISCONNECT** — clean disconnect
- **Topic wildcards** — `+` (single level) and `#` (multi level) per MQTT §4.7
- **Concurrent clients** — goroutine-per-client with mutex-guarded state

## How to Run

```bash
make build        # builds to bin/mqttaton
make run          # runs on port 1883
make test         # runs all 20 tests

# or directly:
go run . -port 1883
```

## Connect a Client

Any MQTT v5 client will work. Using `mosquitto_pub/sub`:

```bash
# Subscribe
mosquitto_sub -h localhost -p 1883 -t "sensor/#" -V 5

# Publish
mosquitto_pub -h localhost -p 1883 -t "sensor/temp" -m "23.5" -V 5
```

## Limitations

- No TLS (plaintext TCP only)
- No authentication enforcement (all clients accepted)
- No retained message store
- No persistent session storage
- No shared subscriptions
- Properties are parsed but not fully enforced
- Will messages are parsed but not delivered on disconnect

## File Layout

```
main.go       — entire broker in one file (~600 lines)
main_test.go  — 20 tests covering protocol correctness
```
