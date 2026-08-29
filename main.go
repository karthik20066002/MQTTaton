// MQTTaton — MQTT v5 broker built on Go's standard library only.
//
// Zero third-party dependencies. Uses net, encoding/binary, bytes, io,
// sync, time, strings, fmt, log only.
//
// Run:  go run . -port 1883
// Test: go test -v
package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

// ──────────────────────────────────────────────────────────────────────
// remaining length — MQTT variable-byte integer (§2.2.3)
// ──────────────────────────────────────────────────────────────────────

func encodeRemainingLength(n int) []byte {
	if n < 0 {
		n = 0
	}
	var buf [4]byte
	i := 0
	for {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			b |= 0x80
		}
		buf[i] = b
		i++
		if n == 0 || i >= 4 {
			break
		}
	}
	return buf[:i]
}

func decodeRemainingLength(r io.Reader) (int, error) {
	var multiplier int = 1
	var value int
	for i := 0; i < 4; i++ {
		b, err := readByte(r)
		if err != nil {
			return 0, err
		}
		value += int(b&0x7f) * multiplier
		multiplier *= 128
		if b&0x80 == 0 {
			return value, nil
		}
	}
	return 0, fmt.Errorf("remaining length overflow")
}

func readByte(r io.Reader) (byte, error) {
	var b [1]byte
	_, err := io.ReadFull(r, b[:])
	return b[0], err
}

func readBytes(r io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}

func readUTF8(r io.Reader) (string, error) {
	var n uint16
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return "", err
	}
	if n > 65535 {
		return "", fmt.Errorf("utf8 string too long: %d", n)
	}
	b, err := readBytes(r, int(n))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func writeUTF8(w io.Writer, s string) error {
	b := []byte(s)
	if len(b) > 65535 {
		return fmt.Errorf("utf8 string too long")
	}
	if err := binary.Write(w, binary.BigEndian, uint16(len(b))); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func writeByte(w io.Writer, b byte) error {
	if bw, ok := w.(io.ByteWriter); ok {
		return bw.WriteByte(b)
	}
	_, err := w.Write([]byte{b})
	return err
}

func readU16BE(r io.Reader) (uint16, error) {
	var v uint16
	if err := binary.Read(r, binary.BigEndian, &v); err != nil {
		return 0, err
	}
	return v, nil
}

// ──────────────────────────────────────────────────────────────────────
// MQTT v5 packet types
// ──────────────────────────────────────────────────────────────────────

const (
	TypeConnect     = 0x10
	TypeConnack     = 0x20
	TypePublish     = 0x30
	TypePuback      = 0x40
	TypePubrec      = 0x50
	TypePubrel      = 0x60
	TypePubcomp     = 0x70
	TypeSubscribe   = 0x80
	TypeSuback      = 0x90
	TypeUnsubscribe = 0xA0
	TypeUnsuback    = 0xB0
	TypePingreq     = 0xC0
	TypePingresp    = 0xD0
	TypeDisconnect  = 0xE0
)

// MQTT v5 reason codes (§3)
const (
	ReasonSuccess                    = 0x00
	ReasonUnspecifiedError           = 0x01
	ReasonMalformedPacket            = 0x02
	ReasonProtocolError              = 0x03
	ReasonImplementationSpecific     = 0x04
	ReasonUnsupportedProtocolVersion = 0x05
	ReasonClientIdentifierNotValid   = 0x06
	ReasonBadUserNameOrPassword      = 0x07
	ReasonNotAuthorized              = 0x08
	ReasonServerUnavailable          = 0x09
	ReasonServerBusy                 = 0x0A
	ReasonBanned                     = 0x0B
	ReasonServerShuttingDown         = 0x0C
	ReasonBadAuthenticationMethod  = 0x0D
	ReasonLeaseExpired               = 0x0E
	ReasonQuotaExceeded              = 0x0F
	ReasonPacketIdentifierInUse      = 0x10
	ReasonPacketIdentifierNotFound   = 0x11
	ReasonReceiveMaximumExceeded     = 0x13
	ReasonTopicAliasInvalid          = 0x14
	ReasonPacketTooLarge             = 0x15
	ReasonMessageRateTooHigh         = 0x16
	ReasonAdministrativeAction       = 0x18
	ReasonInvalidDataRepresentation  = 0x19
	ReasonDisconnectWillNotExist     = 0x1A
	ReasonSharedSubscriptionsNotSup  = 0x1B
	ReasonSubscriptionIdsNotSup      = 0x1C
	ReasonWildcardSubsNotSup         = 0x1D
)

// ──────────────────────────────────────────────────────────────────────
// Topic matching (MQTT §4.7)
// ──────────────────────────────────────────────────────────────────────

func topicMatch(filter, topic string) bool {
	ff := strings.Split(filter, "/")
	tf := strings.Split(topic, "/")
	fi := 0
	ti := 0
	for fi < len(ff) && ti < len(tf) {
		f := ff[fi]
		if f == "#" {
			return fi == len(ff)-1
		}
		if f == "+" {
			fi++
			ti++
			continue
		}
		if f != tf[ti] {
			return false
		}
		fi++
		ti++
	}
	if fi < len(ff) && ff[fi] == "#" {
		return true
	}
	return fi == len(ff) && ti == len(tf)
}

// ──────────────────────────────────────────────────────────────────────
// Types
// ──────────────────────────────────────────────────────────────────────

type Subscription struct {
	Filter string
	QoS    byte
}

type InFlight struct {
	PID     uint16
	QoS     byte
	Topic   string
	Payload []byte
}

type Client struct {
	mu        sync.Mutex
	Conn      net.Conn
	ID        string
	subs      []Subscription
	sessions  map[uint16]*InFlight
	nextPID   uint16
	keepalive time.Duration
	lastSeen  time.Time
	clean     bool
	running   bool
}

type Broker struct {
	mu      sync.RWMutex
	clients map[*Client]bool
	addr    string
	done    chan struct{}
	listener net.Listener
}

func NewBroker(addr string) *Broker {
	return &Broker{
		clients: make(map[*Client]bool),
		addr:    addr,
		done:    make(chan struct{}),
	}
}

func (b *Broker) addClient(c *Client) {
	b.mu.Lock()
	b.clients[c] = true
	b.mu.Unlock()
}

func (b *Broker) removeClient(c *Client) {
	b.mu.Lock()
	delete(b.clients, c)
	b.mu.Unlock()
}

// ──────────────────────────────────────────────────────────────────────
// Message routing
// ──────────────────────────────────────────────────────────────────────

func (b *Broker) broadcast(sender *Client, topic string, payload []byte, qos byte) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for c := range b.clients {
		if c == sender {
			continue
		}
		c.mu.Lock()
		matched := false
		for _, s := range c.subs {
			if topicMatch(s.Filter, topic) {
				matched = true
				break
			}
		}
		c.mu.Unlock()
		if matched {
			c.send(makePublishPkt(topic, qos, false, payload))
		}
	}
}

// ──────────────────────────────────────────────────────────────────────
// Client helpers
// ──────────────────────────────────────────────────────────────────────

func (c *Client) allocPID() uint16 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextPID++
	if c.nextPID == 0 {
		c.nextPID = 1
	}
	return c.nextPID
}

func (c *Client) send(data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, _ = c.Conn.Write(data)
}

func (c *Client) addSub(filter string, qos byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subs = append(c.subs, Subscription{Filter: filter, QoS: qos})
}

func (c *Client) removeSub(filter string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var ns []Subscription
	for _, s := range c.subs {
		if s.Filter != filter {
			ns = append(ns, s)
		}
	}
	c.subs = ns
}

// ──────────────────────────────────────────────────────────────────────
// Packet encoding
// ──────────────────────────────────────────────────────────────────────

func writeFixedHeader(w io.Writer, packetType byte, flags byte, remaining int) error {
	if err := writeByte(w, packetType|flags); err != nil {
		return err
	}
	rl := encodeRemainingLength(remaining)
	_, err := w.Write(rl)
	return err
}

func makePublishPkt(topic string, qos byte, retain bool, payload []byte) []byte {
	var buf bytes.Buffer
	flags := byte(0)
	if qos > 0 {
		flags |= (qos & 0x03) << 1
	}
	if retain {
		flags |= 0x01
	}
	writeUTF8(&buf, topic)
	binary.Write(&buf, binary.BigEndian, uint16(0))
	buf.Write(payload)
	var out bytes.Buffer
	writeFixedHeader(&out, TypePublish, flags, buf.Len())
	out.Write(buf.Bytes())
	return out.Bytes()
}

func makeConnAck(sessionPresent bool, reasonCode byte) []byte {
	var vh bytes.Buffer
	if sessionPresent {
		writeByte(&vh, 0x01)
	} else {
		writeByte(&vh, 0x00)
	}
	writeByte(&vh, reasonCode)
	var buf bytes.Buffer
	writeFixedHeader(&buf, TypeConnack, 0, vh.Len())
	buf.Write(vh.Bytes())
	return buf.Bytes()
}

func makeSubAck(packetID uint16, reasonCodes []byte) []byte {
	var buf bytes.Buffer
	writeFixedHeader(&buf, TypeSuback, 0, 2+len(reasonCodes))
	binary.Write(&buf, binary.BigEndian, packetID)
	buf.Write(reasonCodes)
	return buf.Bytes()
}

func makeUnsubAck(packetID uint16, reasonCodes []byte) []byte {
	var buf bytes.Buffer
	writeFixedHeader(&buf, TypeUnsuback, 0, 2+len(reasonCodes))
	binary.Write(&buf, binary.BigEndian, packetID)
	buf.Write(reasonCodes)
	return buf.Bytes()
}

func makePubackPkt(pid uint16) []byte {
	var buf bytes.Buffer
	writeFixedHeader(&buf, TypePuback, 0, 2)
	binary.Write(&buf, binary.BigEndian, pid)
	return buf.Bytes()
}

func makePubrecPkt(pid uint16) []byte {
	var buf bytes.Buffer
	writeFixedHeader(&buf, TypePubrec, 0, 2)
	binary.Write(&buf, binary.BigEndian, pid)
	return buf.Bytes()
}

func makePubrelPkt(pid uint16) []byte {
	var buf bytes.Buffer
	writeFixedHeader(&buf, TypePubrel, 0, 2)
	binary.Write(&buf, binary.BigEndian, pid)
	return buf.Bytes()
}

func makePubcompPkt(pid uint16) []byte {
	var buf bytes.Buffer
	writeFixedHeader(&buf, TypePubcomp, 0, 2)
	binary.Write(&buf, binary.BigEndian, pid)
	return buf.Bytes()
}

func makePingrespPkt() []byte {
	return []byte{TypePingresp, 0x00}
}

// ──────────────────────────────────────────────────────────────────────
// Packet decoding — read the fixed header including flags
// ──────────────────────────────────────────────────────────────────────

func decodePacket(r io.Reader) (packetType byte, flags byte, data []byte, err error) {
	first, err := readByte(r)
	if err != nil {
		return 0, 0, nil, err
	}
	packetType = first & 0xF0
	flags = first & 0x0F
	rl, err := decodeRemainingLength(r)
	if err != nil {
		return packetType, flags, nil, err
	}
	data = make([]byte, rl)
	if rl > 0 {
		if _, err := io.ReadFull(r, data); err != nil {
			return packetType, flags, nil, err
		}
	}
	return packetType, flags, data, nil
}

// ──────────────────────────────────────────────────────────────────────
// Client read loop
// ──────────────────────────────────────────────────────────────────────

func (c *Client) readLoop(b *Broker) {
	defer func() {
		c.running = false
		b.removeClient(c)
		c.Conn.Close()
	}()
	for c.running {
		c.Conn.SetReadDeadline(time.Now().Add(c.keepalive * 3 / 2))
		packetType, flags, data, err := decodePacket(c.Conn)
		if err != nil {
			return
		}
		if !c.running {
			return
		}
		c.lastSeen = time.Now()
		c.dispatch(packetType, flags, data, b)
	}
}

// ──────────────────────────────────────────────────────────────────────
// Packet dispatch
// ──────────────────────────────────────────────────────────────────────

func (c *Client) dispatch(packetType byte, flags byte, data []byte, b *Broker) {
	switch packetType {
	case TypeConnect:
		c.handleConnect(data, b)
	case TypePublish:
		c.handlePublish(flags, data, b)
	case TypeSubscribe:
		c.handleSubscribe(data, b)
	case TypeUnsubscribe:
		c.handleUnsubscribe(data, b)
	case TypePingreq:
		c.handlePingreq(b)
	case TypeDisconnect:
		c.handleDisconnect(b)
	case TypePuback:
		c.handlePuback(data)
	case TypePubrec:
		c.handlePubrec(data)
	case TypePubrel:
		c.handlePubrel(data)
	case TypePubcomp:
		c.handlePubcomp(data)
	default:
		log.Printf("[%s] unknown packet 0x%02x", c.ID, packetType)
	}
}

// ──────────────────────────────────────────────────────────────────────
// CONNECT
// ──────────────────────────────────────────────────────────────────────

func (c *Client) handleConnect(data []byte, b *Broker) {
	r := bytes.NewReader(data)
	protoName, err := readUTF8(r)
	if err != nil {
		return
	}
	protoLevel, err := readByte(r)
	if err != nil {
		return
	}
	connFlags, err := readByte(r)
	if err != nil {
		return
	}
	cleanSession := connFlags&0x02 != 0
	willFlag := connFlags&0x04 != 0
	willQoS := (connFlags >> 3) & 0x03
	willRetain := connFlags&0x20 != 0
	passwordFlag := connFlags&0x40 != 0
	usernameFlag := connFlags&0x80 != 0
	keepAlive, err := readU16BE(r)
	if err != nil {
		return
	}
	c.keepalive = time.Duration(keepAlive) * time.Second
	c.clean = cleanSession
	_ = willFlag
	_ = willQoS
	_ = willRetain
	_ = passwordFlag
	_ = usernameFlag
	// Properties
	propsLen, err := decodeRemainingLength(r)
	if err != nil {
		return
	}
	for propsLen > 0 {
		propID, err := readByte(r)
		if err != nil {
			break
		}
		propsLen--
		switch propID {
		case 0x11: // session expiry interval (4 bytes)
			if propsLen >= 4 {
				var v uint32
				binary.Read(r, binary.BigEndian, &v)
				_ = v
				propsLen -= 4
			}
		case 0x21: // receive maximum (2 bytes)
			if propsLen >= 2 {
				var v uint16
				binary.Read(r, binary.BigEndian, &v)
				_ = v
				propsLen -= 2
			}
		case 0x22: // topic alias maximum (2 bytes)
			if propsLen >= 2 {
				var v uint16
				binary.Read(r, binary.BigEndian, &v)
				_ = v
				propsLen -= 2
			}
		default:
			propsLen = 0
		}
	}
	clientID, err := readUTF8(r)
	if err != nil {
		return
	}
	c.ID = clientID
	// Will
	if connFlags&0x04 != 0 {
		wt, _ := readUTF8(r)
		wm, _ := readUTF8(r)
		_ = wt
		_ = wm
	}
	if usernameFlag {
		u, _ := readUTF8(r)
		_ = u
	}
	if passwordFlag {
		pl, _ := readU16BE(r)
		if pl > 0 {
			readBytes(r, int(pl))
		}
	}
	log.Printf("[%s] connected clean=%d proto=%s level=%d keepalive=%v", c.ID, boolToInt(cleanSession), protoName, protoLevel, c.keepalive)
	b.addClient(c)
	c.send(makeConnAck(true, ReasonSuccess))
	go c.readLoop(b)
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// ──────────────────────────────────────────────────────────────────────
// PUBLISH
// ──────────────────────────────────────────────────────────────────────

func (c *Client) handlePublish(flags byte, data []byte, b *Broker) {
	qos := (flags >> 1) & 0x03
	retain := flags&0x01 != 0
	dup := flags&0x08 != 0
	_ = dup
	r := bytes.NewReader(data)
	topic, err := readUTF8(r)
	if err != nil {
		return
	}
	var packetID uint16
	if qos > 0 {
		binary.Read(r, binary.BigEndian, &packetID)
	}
	payload := make([]byte, r.Len())
	if r.Len() > 0 {
		io.ReadFull(r, payload)
	}
	log.Printf("[%s] PUBLISH topic=%s qos=%d retain=%v len=%d", c.ID, topic, qos, retain, len(payload))
	if qos == 0 {
		b.broadcast(c, topic, payload, 0)
	} else if qos == 1 {
		c.sessions[packetID] = &InFlight{PID: packetID, QoS: 1, Topic: topic, Payload: payload}
		c.send(makePubackPkt(packetID))
		b.broadcast(c, topic, payload, 1)
	} else if qos == 2 {
		c.sessions[packetID] = &InFlight{PID: packetID, QoS: 2, Topic: topic, Payload: payload}
		c.send(makePubrecPkt(packetID))
	}
}

func (c *Client) handlePuback(data []byte) {
	if len(data) < 2 {
		return
	}
	var pid uint16
	binary.Read(bytes.NewReader(data), binary.BigEndian, &pid)
	c.mu.Lock()
	delete(c.sessions, pid)
	c.mu.Unlock()
}

func (c *Client) handlePubrec(data []byte) {
	if len(data) < 2 {
		return
	}
	var pid uint16
	binary.Read(bytes.NewReader(data), binary.BigEndian, &pid)
	c.send(makePubrelPkt(pid))
}

func (c *Client) handlePubrel(data []byte) {
	if len(data) < 2 {
		return
	}
	var pid uint16
	binary.Read(bytes.NewReader(data), binary.BigEndian, &pid)
	c.mu.Lock()
	if inflight, ok := c.sessions[pid]; ok && inflight.QoS == 2 {
		delete(c.sessions, pid)
	}
	c.mu.Unlock()
	c.send(makePubcompPkt(pid))
}

func (c *Client) handlePubcomp(data []byte) {
	if len(data) < 2 {
		return
	}
	var pid uint16
	binary.Read(bytes.NewReader(data), binary.BigEndian, &pid)
	c.mu.Lock()
	delete(c.sessions, pid)
	c.mu.Unlock()
}

// ──────────────────────────────────────────────────────────────────────
// SUBSCRIBE
// ──────────────────────────────────────────────────────────────────────

func (c *Client) handleSubscribe(data []byte, b *Broker) {
	r := bytes.NewReader(data)
	packetID, err := readU16BE(r)
	if err != nil {
		return
	}
	var reasonCodes []byte
	for r.Len() > 0 {
		filter, err := readUTF8(r)
		if err != nil {
			break
		}
		qos, err := readByte(r)
		if err != nil {
			break
		}
		c.addSub(filter, qos&0x03)
		log.Printf("[%s] SUBSCRIBE filter=%s qos=%d", c.ID, filter, qos&0x03)
		reasonCodes = append(reasonCodes, ReasonSuccess)
	}
	if len(reasonCodes) == 0 {
		reasonCodes = []byte{ReasonUnspecifiedError}
	}
	c.send(makeSubAck(packetID, reasonCodes))
}

// ──────────────────────────────────────────────────────────────────────
// UNSUBSCRIBE
// ──────────────────────────────────────────────────────────────────────

func (c *Client) handleUnsubscribe(data []byte, b *Broker) {
	r := bytes.NewReader(data)
	packetID, err := readU16BE(r)
	if err != nil {
		return
	}
	var reasonCodes []byte
	for r.Len() > 0 {
		filter, err := readUTF8(r)
		if err != nil {
			break
		}
		_ = filter
		c.removeSub(filter)
		log.Printf("[%s] UNSUBSCRIBE filter=%s", c.ID, filter)
		reasonCodes = append(reasonCodes, ReasonSuccess)
	}
	if len(reasonCodes) == 0 {
		reasonCodes = []byte{ReasonUnspecifiedError}
	}
	c.send(makeUnsubAck(packetID, reasonCodes))
}

// ──────────────────────────────────────────────────────────────────────
// PINGREQ / PINGRESP
// ──────────────────────────────────────────────────────────────────────

func (c *Client) handlePingreq(b *Broker) {
	c.send(makePingrespPkt())
}

// ──────────────────────────────────────────────────────────────────────
// DISCONNECT
// ──────────────────────────────────────────────────────────────────────

func (c *Client) handleDisconnect(b *Broker) {
	c.running = false
	b.removeClient(c)
	c.Conn.Close()
}

// ──────────────────────────────────────────────────────────────────────
// Broker server
// ──────────────────────────────────────────────────────────────────────

func (b *Broker) listen() {
	ln, err := net.Listen("tcp", b.addr)
	if err != nil {
		log.Fatalf("broker listen: %v", err)
	}
	b.listener = ln
	log.Printf("MQTTaton v5 broker listening on %s", b.addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-b.done:
				return
			default:
				log.Printf("accept error: %v", err)
				continue
			}
		}
		go b.handleConnection(conn)
	}
}

func (b *Broker) handleConnection(conn net.Conn) {
	c := &Client{
		Conn:      conn,
		subs:      make([]Subscription, 0),
		sessions:  make(map[uint16]*InFlight),
		keepalive: 60 * time.Second,
		running:   true,
		lastSeen:  time.Now(),
	}
	go c.readLoop(b)
}

func (b *Broker) Stop() {
	close(b.done)
	if b.listener != nil {
		b.listener.Close()
	}
	b.mu.Lock()
	for c := range b.clients {
		c.Conn.Close()
	}
	b.mu.Unlock()
}

// ──────────────────────────────────────────────────────────────────────
// Main
// ──────────────────────────────────────────────────────────────────────

var (
	portFlag = flag.String("port", "1883", "TCP port to listen on")
)

func main() {
	flag.Parse()
	b := NewBroker(":" + *portFlag)
	b.listen()
}
