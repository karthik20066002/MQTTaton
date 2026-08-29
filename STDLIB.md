# STDLIB.md — Standard Library Substitutions

| Normally imported                  | Go stdlib replacement                              | Why                                                    |
|------------------------------------|----------------------------------------------------|---------------------------------------------------------|
| `github.com/eclipse/paho.mqtt.golang` | `net`, `encoding/binary`, `bytes`              | Full MQTT v5 broker: TCP server, packet codec, routing  |
| `github.com/go-logr/logr` or `logrus` | `log`                                         | Structured logging via stdlib `log`                     |
| `github.com/gorilla/websocket`    | `net`                                              | Raw TCP socket handling, no WebSocket needed             |
| `google.golang.org/protobuf`      | `encoding/binary`                                  | Binary packet encoding for MQTT wire protocol           |
| `go.uber.org/atomic`              | `sync` (Mutex, RWMutex)                           | Concurrency control via stdlib mutexes                  |
| `github.com/satori/go.uuid`       | none needed                                        | Client IDs come from the CONNECT packet, not generated |

## Design Notes

- **Packet encoding**: MQTT v5 uses a variable-byte integer (1–4 bytes) for remaining length. Implemented as a 6-line encode/decode loop — no library needed.
- **Topic matching**: Split filter and topic on `/`, then walk level by level matching `+` and `#` wildcards. A 20-line function replaces any topic-matching package.
- **Concurrency**: One goroutine per client, channels and mutexes for shared state. No actor framework required.
- **No serialization library**: MQTT packets are read with `binary.Read` and written with `bytes.Buffer`. No protobuf, no JSON.
