// Package session tracks the client's persisted connection state that must
// survive reconnects: subscription filters, in-flight QoS 1/2 message state,
// and the next packet id (§4.4 Message Delivery Retry / §4.7 Ordering).
//
// A session is not persisted to disk in this scaffold. Durability across a
// clean broker disconnect is tied to the broker's CleanSession flag; making
// this optionally durable on disk (e.g. a log file) is an extension point.
package session

// State is the in-memory session state for a single client.
type State struct {
	Subscriptions map[string]byte // topic -> granted QoS
	InFlight      map[uint16]*Inflight
	NextPacketID  uint16
	cleanSession  bool
}

// Inflight tracks one QoS 1/2 message awaiting acknowledgement.
type Inflight struct {
	PacketID uint16
	QoS      byte
	Topic    string
	Payload  []byte
	// QoS2State is the current step of the two-phase handshake: PUBREC/PUBCOMP.
	QoS2State byte
}

// New returns an empty session.
func New(clean bool) *State { return nil }

// NextID allocates and reserves the next unused packet identifier.
func (s *State) NextID() uint16 { return 0 }

// AddSubscription records a topic filter at the given QoS.
func (s *State) AddSubscription(topic string, qos byte) {}

// RemoveSubscription deletes a topic filter.
func (s *State) RemoveSubscription(topic string) {}

// TrackInFlight records a message awaiting ack and returns its packet id.
func (s *State) TrackInFlight(topic string, qos byte, payload []byte) uint16 {
	return 0
}

// Ack completes the delivery state for a packet id.
func (s *State) Ack(id uint16) {}

// HasSubscription reports whether topic matches any stored filter.
func (s *State) HasSubscription(topic string) bool { return false }
