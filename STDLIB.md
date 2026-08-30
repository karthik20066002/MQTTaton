# STDLIB.md - Zero-Dependency Craft

This document documents all stdlib-for-package substitutions made in MQTTaton. Since this is a zero-dependency hackathon project, we rely exclusively on Go's standard library and document what we would have otherwise imported.

## stdlib-for-package Substitutions

### 1. MQTT Protocol Implementation
**Instead of importing:** `github.com/eclipse/paho.mqtt.golang` or similar MQTT libraries
**We use:** Go standard library:
- `net` - TCP networking for broker and client connections
- `encoding/binary` - MQTT packet encoding/decoding
- `bytes` - Buffer manipulation for packet construction
- `io` - Input/output operations for network streams
- `sync` - Concurrency primitives (mutexes, wait groups)
- `time` - Time handling for keepalive and timeouts
- `strings` - String manipulation for topic matching
- `fmt` - Formatting for logging and error messages
- `log` - Structured logging

### 2. Message Routing and Topic Management
**Instead of importing:** `github.com/mochi-co/mqtt` or similar routing libraries
**We use:** Custom implementation using:
- `sync.RWMutex` - Thread-safe client and subscription management
- `strings` - Topic wildcard matching (`+` and `#`)
- Custom `topicMatch()` function implementing MQTT topic matching rules

### 3. Authentication System
**Instead of importing:** `github.com/golang-jwt/jwt` or authentication libraries
**We use:** Simple authentication using:
- Basic username/password validation
- Command-line flag parsing using `flag` package
- Input validation using standard library validation functions

### 4. Error Handling and Validation
**Instead of importing:** `github.com/pkg/errors` or error handling libraries
**We use:** Go standard error handling:
- `fmt.Errorf()` for error creation
- Custom error messages for MQTT protocol errors
- Input validation using standard library functions

### 5. Configuration Management
**Instead of importing:** `github.com/spf13/viper` or configuration libraries
**We use:** Simple configuration using:
- `flag` package for command-line argument parsing
- Environment variable handling through standard library

### 6. Concurrency and Goroutine Management
**Instead of importing:** `github.com/gorilla/websocket` or concurrency libraries
**We use:** Go's built-in concurrency:
- `goroutines` for client handling
- `channels` for broker shutdown signaling
- `sync.Mutex` for thread-safe operations
- `sync.WaitGroup` for goroutine coordination

### 7. Network Protocol Implementation
**Instead of importing:** `github.com/gorilla/mux` or HTTP libraries
**We use:** Go's standard networking:
- `net.Listen()` for TCP server
- `net.Accept()` for connection handling
- `net.Conn` interface for network operations

### 8. Data Structures and Collections
**Instead of importing:** `github.com/golang-collections/collections` or similar
**We use:** Go standard library collections:
- `map` for client and session management
- `slice` for subscription lists
- Custom data structures (`Client`, `Broker`, `InFlight`, `Subscription`)

### 9. Binary Protocol Handling
**Instead of importing:** `github.com/tinylib/msgp` or message pack libraries
**We use:** Go's binary encoding:
- `encoding/binary` for MQTT packet encoding
- Custom packet structure implementations
- Byte-level manipulation for protocol compliance

### 10. Logging and Debugging
**Instead of importing:** `github.com/sirupsen/logrus` or structured logging libraries
**We use:** Go standard logging:
- `log` package for structured logging
- Custom log formatting for MQTT protocol events
- Context-aware logging with client IDs

## Key Design Decisions

### Why No External Dependencies?
1. **Zero Dependency Requirement**: The hackathon explicitly requires zero third-party runtime dependencies
2. **Security**: No supply chain security risks from external packages
3. **Portability**: Single binary deployment with no external dependencies
4. **Performance**: Direct access to standard library without abstraction layers

### stdlib-Only Architecture Benefits
- **Deterministic Build**: `go build` produces a single executable
- **No Version Conflicts**: Standard library version is fixed with Go toolchain
- **Security**: No risk of malicious package injection
- **Simplicity**: Direct access to networking, encoding, and concurrency primitives

### Protocol Implementation Approach
Instead of using an existing MQTT library, we implemented the protocol directly using:
- TCP networking via `net` package
- Binary encoding via `encoding/binary`
- Concurrency via `sync` and goroutines
- Custom topic matching algorithm

This demonstrates deep understanding of the MQTT protocol while maintaining zero dependencies.

## Compliance with Zero-Dependency Rules

✅ **Empty go.mod**: No `require` block  
✅ **No third-party imports**: All code uses standard library only  
✅ **Single executable**: `go build` produces a standalone binary  
✅ **No vendoring**: No copied third-party source code  
✅ **Transparent implementation**: All functionality implemented from scratch using stdlib  

## Performance Considerations

While external libraries might offer optimizations, our stdlib-only approach ensures:
- **No hidden dependencies**: Every line of code is visible and understandable
- **Minimal attack surface**: Standard library is well-audited and secure
- **Deterministic behavior**: No unexpected behavior from external package updates
- **Easy debugging**: Direct access to protocol implementation

## Future Extensibility

The stdlib-only approach makes it easy to extend the broker with additional features while maintaining zero dependency compliance:
- TLS support via `crypto/tls`
- WebSocket support via custom implementation
- Advanced authentication via standard crypto packages
- Monitoring and metrics via standard library logging

This implementation demonstrates that sophisticated networking protocols can be implemented using only the standard library, making it both impressive and practical for the zero-dependency hackathon.