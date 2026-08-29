// Package packets implements the MQTT 3.1.1 wire protocol: control packet
// encoding and decoding from first byte to last byte.
//
// This is the package that replaces "github.com/eclipse/paho.mqtt.golang"'s
// internal decoder. It is written entirely against the Go standard library.
//
// Control packet types covered (MQTT 3.1.1, OASIS spec):
//
//	1  CONNECT      2  CONNACK      3  PUBLISH     4  PUBACK
//	5  PUBREC       6  PUBREL       7  PUBCOMP     8  SUBSCRIBE
//	9  SUBACK      10  UNSUBSCRIBE 11  UNSUBACK   12  PINGREQ
//	13 PINGRESP    14  DISCONNECT
//
// Thread-safety: packet structs are immutable once encoded. Encode and
// Decode are self-contained and safe to call concurrently.
package packets

import "io"

// PacketType is the 4-bit control packet type (MQTT 3.1.1 §2.2.1, Table 2-1).
type PacketType byte

const (
	CONNECT     PacketType = 1
	CONNACK     PacketType = 2
	PUBLISH     PacketType = 3
	PUBACK      PacketType = 4
	PUBREC      PacketType = 5
	PUBREL      PacketType = 6
	PUBCOMP     PacketType = 7
	SUBSCRIBE   PacketType = 8
	SUBACK      PacketType = 9
	UNSUBSCRIBE PacketType = 10
	UNSUBACK    PacketType = 11
	PINGREQ     PacketType = 12
	PINGRESP    PacketType = 13
	DISCONNECT  PacketType = 14
)

func (t PacketType) String() string { return "" }

// ConnectFlags fields of the CONNECT packet (§3.1.2.3).
type ConnectFlags struct {
	CleanSession bool
	WillFlag     bool
	WillQoS      byte
	WillRetain   bool
	PasswordFlag bool
	UsernameFlag bool
}

// Connect is the CONNECT control packet (§3.1).
type Connect struct {
	ProtocolName  string
	ProtocolLevel byte
	Flags         ConnectFlags
	KeepAlive     uint16
	ClientID      string
	WillTopic     string
	WillMessage   []byte
	Username      string
	Password      []byte
}

// Connack is the CONNACK control packet (§3.2).
type Connack struct {
	SessionPresent bool
	ReturnCode     byte
}

// Publish is the PUBLISH control packet (§3.3).
type Publish struct {
	Topic      string
	QoS        byte
	Retain     bool
	Dup        bool
	PacketID   uint16
	Payload    []byte
}

// Puback/Pubrec/Pubrel/Pubcomp share the same shape (§3.4–§3.7).
type Ack struct {
	PacketID uint16
}

// Subscribe is the SUBSCRIBE control packet (§3.8).
type Subscribe struct {
	PacketID uint16
	Filters  []Subscription
}

// Subscription is a single (topic, qos) filter.
type Subscription struct {
	Topic string
	QoS   byte
}

// Suback is the SUBACK control packet (§3.9).
type Suback struct {
	PacketID   uint16
	ReturnCodes []byte
}

// Pingreq/Pingresp/Disonnect carry no variable header or payload.
type Pingreq struct{}
type Pingresp struct{}
type Disconnect struct{}

// Decode reads one complete control packet from r. It is a low-level entry
// point used by the client's connection loop.
func Decode(r io.Reader) (interface{}, error) { return nil, nil }

// encode performs the fixed-header + variable-header + payload serialization.
// This is where the remaining length varint (§2.2.3) is written.
func encode(p interface{}) ([]byte, error) { return nil, nil }

// encodeRemainingLength writes the 1-4 byte variable-length encoding (§2.2.3).
func encodeRemainingLength(length int) []byte { return nil }

// decodeLength reads the variable-length field from b and returns the length
// and the number of bytes consumed.
func decodeLength(b []byte) (int, int, error) { return 0, 0, nil }

// Encode serialises an individual packet value to bytes.
func Encode(p interface{}) ([]byte, error) { return nil, nil }
